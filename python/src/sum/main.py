import os
import logging
import threading
import hashlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_FLUSH_EXCHANGE = f"{SUM_PREFIX}_flush"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


def _get_aggregation_index(fruit, amount):
    return int(hashlib.md5(fruit.encode()).hexdigest(), 16) % amount

class SumFilter:
    def __init__(self):
        self._lock = threading.Lock()

        self.queue_consumer = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        self.fanout_publisher = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_FLUSH_EXCHANGE, [], exchange_type="fanout"
        )
        self.fanout_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_FLUSH_EXCHANGE, [], exchange_type="fanout"
        )                                                             
        self.data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}" for i in range(AGGREGATION_AMOUNT)]
        )
        self.amount_by_fruit_by_client = {}

    def _get_client_amount_by_fruit(self, client_id):
        if client_id not in self.amount_by_fruit_by_client:
            self.amount_by_fruit_by_client[client_id] = {}
        return self.amount_by_fruit_by_client[client_id]

    def _flush_client(self, client_id):
        logging.info(f"Flushing state for client {client_id}")
        amount_by_fruit = self.amount_by_fruit_by_client.get(client_id, {})
        for final_fruit_item in amount_by_fruit.values():
            aggregation_index = _get_aggregation_index(final_fruit_item.fruit, AGGREGATION_AMOUNT)
            self.data_output_exchange.send_to(f"{AGGREGATION_PREFIX}_{aggregation_index}",
                message_protocol.internal.serialize(
                    message_protocol.internal.build_sum_data(
                        client_id, final_fruit_item.fruit, final_fruit_item.amount
                    )
                )
            )
        self.data_output_exchange.send(
            message_protocol.internal.serialize(
                message_protocol.internal.build_sum_eof(client_id)
            )
        )       
        self.amount_by_fruit_by_client.pop(client_id, None)

    def process_data_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        parsed = message_protocol.internal.parse_sum_message(fields)
        if parsed[0] == "data":
            _, client_id, fruit, amount = parsed
            with self._lock:
                amount_by_fruit = self._get_client_amount_by_fruit(client_id)
                amount_by_fruit[fruit] = amount_by_fruit.get(
                    fruit, fruit_item.FruitItem(fruit, 0)
                ) + fruit_item.FruitItem(fruit, int(amount))
        elif parsed[0] == "client_eof":
            _, client_id = parsed
            logging.info(f"Received client EOF for {client_id}, broadcasting flush")
            self.fanout_publisher.send(
                message_protocol.internal.serialize(
                    message_protocol.internal.build_sum_eof(client_id)
                )
            )
        ack()

    def process_flush(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        parsed = message_protocol.internal.parse_sum_message(fields)
        _, client_id = parsed
        with self._lock:
            self._flush_client(client_id)
        ack()

    def start(self):
        flush_thread = threading.Thread(
            target=self.fanout_consumer.start_consuming,
            args=(self.process_flush,),
            kwargs={"queue_name": f"{SUM_FLUSH_EXCHANGE}_{ID}"},
            daemon=True,
        )
        flush_thread.start()
        self.queue_consumer.start_consuming(self.process_data_message)
        flush_thread.join()


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
