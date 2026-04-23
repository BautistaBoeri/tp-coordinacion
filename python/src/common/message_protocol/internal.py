import json


DATA_CODE = 0
GATEWAY_EOF_CODE = 1
SUM_EOF_CODE = 2
PARTIAL_CODE = 3
SUM_FLUSH_CODE = 4
SUM_PARTIAL_COUNT_CODE = 5


def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))


def build_sum_data(client_id, fruit, amount):
    return [DATA_CODE, client_id, fruit, amount]


def build_sum_eof(client_id):
    return [SUM_EOF_CODE, client_id]


def build_client_eof(client_id, total):
    return [GATEWAY_EOF_CODE, client_id, total]


def build_sum_flush_broadcast(client_id, total):
    return [SUM_FLUSH_CODE, client_id, total]


def build_sum_partial_count(client_id, sum_id, count):
    return [SUM_PARTIAL_COUNT_CODE, client_id, sum_id, count]


def build_aggregation_partial(client_id, fruit_top):
    return [PARTIAL_CODE, client_id, fruit_top]


def parse_gateway_to_sum(fields):
    if not fields:
        raise ValueError(f"Empty gateway-to-sum message: {fields}")
    tag = fields[0]
    if tag == DATA_CODE and len(fields) == 4:
        _, client_id, fruit, amount = fields
        return ("data", client_id, fruit, amount)
    if tag == GATEWAY_EOF_CODE and len(fields) == 3:
        _, client_id, total = fields
        return ("gateway_eof", client_id, total)
    raise ValueError(f"Invalid gateway-to-sum message format: {fields}")


def parse_sum_to_aggregation(fields):
    if not fields:
        raise ValueError(f"Empty sum-to-aggregation message: {fields}")
    tag = fields[0]
    if tag == DATA_CODE and len(fields) == 4:
        _, client_id, fruit, amount = fields
        return ("data", client_id, fruit, amount)
    if tag == SUM_EOF_CODE and len(fields) == 2:
        _, client_id = fields
        return ("sum_eof", client_id)
    raise ValueError(f"Invalid sum-to-aggregation message format: {fields}")


def parse_sum_flush(fields):
    if not fields:
        raise ValueError(f"Empty flush message: {fields}")
    tag = fields[0]
    if tag == SUM_FLUSH_CODE and len(fields) == 3:
        _, client_id, total = fields
        return ("flush_broadcast", client_id, total)
    if tag == SUM_PARTIAL_COUNT_CODE and len(fields) == 4:
        _, client_id, sum_id, count = fields
        return ("partial_count", client_id, sum_id, count)
    raise ValueError(f"Invalid flush message format: {fields}")


def parse_aggregation_partial(fields):
    if len(fields) != 3 or fields[0] != PARTIAL_CODE:
        raise ValueError(f"Invalid aggregation partial format: {fields}")
    _, client_id, fruit_top = fields
    return (client_id, fruit_top)
