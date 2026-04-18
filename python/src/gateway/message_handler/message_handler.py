import uuid
from common import message_protocol


class MessageHandler:

    def __init__(self):
        self.client_id = str(uuid.uuid4())

    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize_data_message(self.client_id, fruit, amount)

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize_eof(self.client_id)

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if fields[0] == "RESULT" and fields[1] == self.client_id:
            return fields[2]
        return None
