import os
import logging

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.received_partials_by_client = {}
        self.amount_by_fruit_by_client = {}

    def _merge_partial_top(self, client_id, partial_top):
        client_amount_by_fruit = self.amount_by_fruit_by_client.setdefault(client_id, {})
        for fruit, amount in partial_top:
            client_amount_by_fruit[fruit] = client_amount_by_fruit.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, amount)

    def _build_final_top(self, client_id):
        client_amount_by_fruit = self.amount_by_fruit_by_client.get(client_id, {})
        ordered_fruit_items = sorted(client_amount_by_fruit.values(), reverse=True)
        ordered_fruit_items = ordered_fruit_items[:TOP_SIZE]
        return list(
            map(
                lambda fruit_instance: (fruit_instance.fruit, fruit_instance.amount),
                ordered_fruit_items,
            )
        )

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        fields = message_protocol.internal.deserialize(message)
        client_id, partial_top = message_protocol.internal.parse_aggregation_partial(
            fields
        )

        self._merge_partial_top(client_id, partial_top)

        received_partials = self.received_partials_by_client.get(client_id, 0) + 1
        self.received_partials_by_client[client_id] = received_partials

        if received_partials == AGGREGATION_AMOUNT:
            final_top = self._build_final_top(client_id)
            self.output_queue.send(
                message_protocol.internal.serialize(
                    message_protocol.internal.build_aggregation_partial(
                        client_id, final_top
                    )
                )
            )
            self.received_partials_by_client.pop(client_id, None)
            self.amount_by_fruit_by_client.pop(client_id, None)

        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
