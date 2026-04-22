import json


DATA_CODE = 0
EOF_CODE = 1
PARTIAL_CODE = 2
SUM_FLUSH_CODE = 3
SUM_PARTIAL_COUNT_CODE = 4



def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))


def build_sum_data(client_id, fruit, amount):
    return [DATA_CODE, client_id, fruit, amount]


def build_sum_eof(client_id):
    return [EOF_CODE, client_id]


def build_client_eof(client_id, total):
    return [EOF_CODE, client_id, total]


def build_sum_flush_broadcast(client_id, total):
    return [SUM_FLUSH_CODE, client_id, total]


def build_sum_partial_count(client_id, sum_id, count):
    return [SUM_PARTIAL_COUNT_CODE, client_id, sum_id, count]


def build_aggregation_partial(client_id, fruit_top):
    return [PARTIAL_CODE, client_id, fruit_top]


def parse_sum_message(fields):
    if not fields:
        raise ValueError(f"Empty sum message: {fields}")
    tag = fields[0]
    if tag == DATA_CODE and len(fields) == 4:
        _, client_id, fruit, amount = fields
        return ("data", client_id, fruit, amount)
    if tag == EOF_CODE and len(fields) == 3:
        _, client_id, total = fields
        return ("client_eof", client_id, total)
    if tag == EOF_CODE and len(fields) == 2:
        _, client_id = fields
        return ("client_eof", client_id)
    raise ValueError(f"Invalid sum message format: {fields}")


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
    if len(fields) != 3 or fields[0] != PARTIAL_CODE or not isinstance(fields[1], str) or not isinstance(fields[2], list):
        raise ValueError(f"Invalid aggregation partial format: {fields}")
    _, client_id, fruit_top = fields
    return (client_id, fruit_top)
