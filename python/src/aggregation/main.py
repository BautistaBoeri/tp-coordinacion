import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

INPUT_QUEUE = f"{AGGREGATION_PREFIX}_{ID}_queue"
ROUTING_KEY = f"{AGGREGATION_PREFIX}_{ID}"


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [ROUTING_KEY]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.amount_by_fruit_by_client = {}
        self.eof_count_by_client = {}

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        by_fruit = self.amount_by_fruit_by_client.setdefault(client_id, {})
        by_fruit[fruit] = by_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, amount)

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
        by_fruit = self.amount_by_fruit_by_client.pop(client_id, {})
        ordered = sorted(by_fruit.values(), reverse=True)[:TOP_SIZE]
        fruit_top = [(item.fruit, item.amount) for item in ordered]
        self.output_queue.send(
            message_protocol.internal.serialize(
                message_protocol.internal.build_aggregation_partial(client_id, fruit_top)
            )
        )

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
        self.input_exchange.start_consuming(
            self.process_messsage, queue_name=INPUT_QUEUE
        )


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
