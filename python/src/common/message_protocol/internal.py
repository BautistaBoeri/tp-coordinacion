import json


def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))


def build_sum_data(client_id, fruit, amount):
    return [client_id, fruit, amount]


def build_sum_eof(client_id):
    return [client_id]


def build_aggregation_partial(client_id, fruit_top):
    return [client_id, fruit_top]


def parse_sum_message(fields):
    if len(fields) == 3:
        return ("data", fields[0], fields[1], fields[2])
    if len(fields) == 1:
        return ("eof", fields[0])
    raise ValueError(f"Invalid sum message format: {fields}")


def parse_aggregation_partial(fields):
    if len(fields) != 2 or not isinstance(fields[0], str) or not isinstance(fields[1], list):
        raise ValueError(f"Invalid aggregation partial format: {fields}")
    return (fields[0], fields[1])
