import os
import logging
import signal
import hashlib
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_FLUSH_EXCHANGE = f"{SUM_PREFIX}_flush"
SUM_FLUSH_QUEUE = f"{SUM_FLUSH_EXCHANGE}_{ID}"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


def _get_aggregation_index(fruit, amount):
    return int(hashlib.md5(fruit.encode()).hexdigest(), 16) % amount


class SumFilter:
    def __init__(self):
        self.state_lock = threading.Lock()

        self.amount_by_fruit_by_client = {}
        self.local_count = {}
        self.expected_total = {}
        self.peer_counts = {}
        self.flushed = set()

        self._data_connection = None
        self._data_consumer = None
        self._flush_connection = None
        self._flush_consumer = None

    def _flush_to_aggregation(self, client_id, agg_publisher):
        amount_by_fruit = self.amount_by_fruit_by_client.get(client_id, {})
        for final_fruit_item in amount_by_fruit.values():
            aggregation_index = _get_aggregation_index(
                final_fruit_item.fruit, AGGREGATION_AMOUNT
            )
            agg_publisher.send(
                message_protocol.internal.serialize(
                    message_protocol.internal.build_sum_data(
                        client_id, final_fruit_item.fruit, final_fruit_item.amount
                    )
                ),
                routing_key=f"{AGGREGATION_PREFIX}_{aggregation_index}"
            )

        agg_publisher.send(
            message_protocol.internal.serialize(
                message_protocol.internal.build_sum_eof(client_id)
            )
        )

        self.amount_by_fruit_by_client.pop(client_id, None)
        self.local_count.pop(client_id, None)
        self.expected_total.pop(client_id, None)
        self.peer_counts.pop(client_id, None)
        self.flushed.add(client_id)
        logging.info(f"Flushed client {client_id}")

    def _check_convergence(self, client_id, agg_publisher):
        if client_id in self.flushed:
            return
        total = self.expected_total.get(client_id)
        if total is None:
            return
        reported = sum(self.peer_counts.get(client_id, {}).values())
        if reported == total:
            self._flush_to_aggregation(client_id, agg_publisher)


    def _report_own_count(self, client_id, count, flush_publisher):
        if client_id not in self.peer_counts:
            self.peer_counts[client_id] = {}
        self.peer_counts[client_id][ID] = count
        flush_publisher.send(
            message_protocol.internal.serialize(
                message_protocol.internal.build_sum_partial_count(client_id, ID, count)
            )
        )

    def _process_data(self, client_id, fruit, amount, flush_publisher):
        with self.state_lock:
            if client_id in self.flushed:
                return

            by_fruit = self.amount_by_fruit_by_client.setdefault(client_id, {})
            by_fruit[fruit] = by_fruit.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, amount)
            self.local_count[client_id] = self.local_count.get(client_id, 0) + 1

            if client_id in self.expected_total:
                self._report_own_count(client_id, self.local_count[client_id], flush_publisher)

    def _on_data_message(self, message, ack, _nack, flush_publisher):
        fields = message_protocol.internal.deserialize(message)
        parsed = message_protocol.internal.parse_sum_message(fields)

        if parsed[0] == "data":
            _, client_id, fruit, amount = parsed
            self._process_data(client_id, fruit, int(amount), flush_publisher)

        elif parsed[0] == "client_eof":
            if len(parsed) != 3:
                raise ValueError(f"Expected gateway EOF with total on input_queue: {parsed}")
            _, client_id, total = parsed
            logging.info(f"Received gateway EOF for {client_id} (total={total}), broadcasting")
            flush_publisher.send(
                message_protocol.internal.serialize(
                    message_protocol.internal.build_sum_flush_broadcast(client_id, total)
                )
            )

        ack()


    def _process_flush_broadcast(self, client_id, total, flush_publisher, agg_publisher):
        with self.state_lock:
            if client_id in self.flushed:
                return

            self.expected_total[client_id] = total

            self._report_own_count(client_id, self.local_count.get(client_id, 0), flush_publisher)

            self._check_convergence(client_id, agg_publisher)

    def _process_partial_count(self, client_id, sum_id, count, agg_publisher):
        with self.state_lock:
            if client_id in self.flushed:
                return

            if client_id not in self.peer_counts:
                self.peer_counts[client_id] = {}
            self.peer_counts[client_id][sum_id] = count

            self._check_convergence(client_id, agg_publisher)

    def _on_flush_message(self, message, ack, _nack, flush_publisher, agg_publisher):
        fields = message_protocol.internal.deserialize(message)
        parsed = message_protocol.internal.parse_sum_flush(fields)

        if parsed[0] == "flush_broadcast":
            _, client_id, total = parsed
            self._process_flush_broadcast(client_id, total, flush_publisher, agg_publisher)

        elif parsed[0] == "partial_count":
            _, client_id, sum_id, count = parsed
            self._process_partial_count(client_id, sum_id, count, agg_publisher)

        ack()

    def _run_data_thread(self):
        input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        flush_publisher = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_FLUSH_EXCHANGE, [], exchange_type='fanout',
            connection=input_queue.connection, channel=input_queue.channel,
        )

        self._data_connection = input_queue.connection
        self._data_consumer = input_queue

        try:
            input_queue.start_consuming(
                lambda m, a, n: self._on_data_message(m, a, n, flush_publisher)
            )
        finally:
            try:
                input_queue.close()
            except Exception:
                pass

    def _run_flush_thread(self):
        flush_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_FLUSH_EXCHANGE, [], exchange_type='fanout',
        )
        agg_publisher = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX,
            [f"{AGGREGATION_PREFIX}_{i}" for i in range(AGGREGATION_AMOUNT)],
            exchange_type='direct',
            connection=flush_exchange.connection, channel=flush_exchange.channel,
        )

        self._flush_connection = flush_exchange.connection
        self._flush_consumer = flush_exchange

        try:
            flush_exchange.start_consuming(
                lambda m, a, n: self._on_flush_message(m, a, n, flush_exchange, agg_publisher),
                queue_name=SUM_FLUSH_QUEUE,
            )
        finally:
            try:
                flush_exchange.close()
            except Exception:
                pass

    def start(self):
        data_thread = threading.Thread(target=self._run_data_thread, name="sum-data")
        flush_thread = threading.Thread(target=self._run_flush_thread, name="sum-flush")
        data_thread.start()
        flush_thread.start()
        data_thread.join()
        flush_thread.join()

    def stop(self):
        if self._data_connection is not None and self._data_consumer is not None:
            try:
                self._data_connection.add_callback_threadsafe(
                    lambda: self._data_consumer.stop_consuming()
                )
            except Exception as exc:
                logging.error(f"error scheduling data stop: {exc}")

        if self._flush_connection is not None and self._flush_consumer is not None:
            try:
                self._flush_connection.add_callback_threadsafe(
                    lambda: self._flush_consumer.stop_consuming()
                )
            except Exception as exc:
                logging.error(f"error scheduling flush stop: {exc}")

    def close(self):
        pass


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    signal.signal(signal.SIGTERM, lambda _s, _f: sum_filter.stop())
    try:
        sum_filter.start()
    finally:
        sum_filter.close()
    return 0


if __name__ == "__main__":
    main()
