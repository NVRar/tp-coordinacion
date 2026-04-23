import json


def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))


def serialize_data_message(client_id, fruit, amount):
    return serialize(["DATA", client_id, fruit, amount])

def serialize_eof(client_id):
    return serialize(["EOF", client_id])

def serialize_top_message(client_id, fruit_top):
    return serialize(["TOP", client_id, fruit_top])

def serialize_result_message(client_id, result):
    return serialize(["RESULT", client_id, result])

def is_data(fields):
    return fields[0] == "DATA"

def is_eof(fields):
    return fields[0] == "EOF"

def is_result(fields):
    return fields[0] == "RESULT"