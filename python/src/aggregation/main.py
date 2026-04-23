import os
import logging
import signal
import bisect

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top_by_client = {}
        self.eof_count_by_client = {}

    def _get_client_fruit_top(self, client_id):
        if client_id not in self.fruit_top_by_client:
            self.fruit_top_by_client[client_id] = []
        return self.fruit_top_by_client[client_id]

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        client_fruit_top = self._get_client_fruit_top(client_id)
        for i in range(len(client_fruit_top)):
            if client_fruit_top[i].fruit == fruit:
                updated_fruit_item = client_fruit_top.pop(i) + fruit_item.FruitItem(
                    fruit, amount
                )
                bisect.insort(client_fruit_top, updated_fruit_item)
                return
        bisect.insort(client_fruit_top, fruit_item.FruitItem(fruit, amount))

    

    def _process_eof(self, client_id):
        self.eof_count_by_client[client_id] = (
            self.eof_count_by_client.get(client_id, 0) + 1
        )
        logging.info(
            f"Received EOF {self.eof_count_by_client[client_id]}/{SUM_AMOUNT} "
            f"for client {client_id}"
        )
        if self.eof_count_by_client[client_id] < SUM_AMOUNT:
            return
        self.eof_count_by_client.pop(client_id, None)
        client_fruit_top = self.fruit_top_by_client.get(client_id, [])
        fruit_chunk = list(client_fruit_top[-TOP_SIZE:])
        fruit_chunk.reverse()
        fruit_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        self.output_queue.send(
            message_protocol.internal.serialize(
                message_protocol.internal.build_aggregation_partial(client_id, fruit_top)
            )
        )
        self.fruit_top_by_client.pop(client_id, None)

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        parsed_message = message_protocol.internal.parse_sum_to_aggregation(fields)
        if parsed_message[0] == "data":
            _, client_id, fruit, amount = parsed_message
            self._process_data(client_id, fruit, amount)
        elif parsed_message[0] == "sum_eof":
            _, client_id = parsed_message
            self._process_eof(client_id)
        ack()

    def stop(self):
        self.input_exchange.stop_consuming()

    def close(self):
        try: self.input_exchange.close()
        except middleware.MessageMiddlewareCloseError: pass
        try: self.output_queue.close()
        except middleware.MessageMiddlewareCloseError: pass

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    signal.signal(signal.SIGTERM, lambda _s, _f: aggregation_filter.stop())
    try:
        aggregation_filter.start()
    finally:
        aggregation_filter.close()
    return 0


if __name__ == "__main__":
    main()
