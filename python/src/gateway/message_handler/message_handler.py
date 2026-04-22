import uuid

from common import message_protocol


class MessageHandler:

    def __init__(self):
        self.client_id = str(uuid.uuid4())
        self.count = 0

    def serialize_data_message(self, message):
        [fruit, amount] = message
        self.count += 1
        return message_protocol.internal.serialize(
            message_protocol.internal.build_sum_data(self.client_id, fruit, amount)
        )

    def serialize_eof_message(self, _message):
        return message_protocol.internal.serialize(
            message_protocol.internal.build_client_eof(self.client_id, self.count)
        )

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        result_client_id, result_fruit_top = (
            message_protocol.internal.parse_aggregation_partial(fields)
        )

        if result_client_id != self.client_id:
            return []

        return result_fruit_top
