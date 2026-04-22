import os
import logging
import threading
import hashlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = f"{SUM_PREFIX}_control"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self, fruit_store, message_lock):
        self.fruit_store = fruit_store
        self.message_lock = message_lock
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_CONTROL_EXCHANGE}_{i}" for i in range(SUM_AMOUNT)]
        )

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")
        self.fruit_store.add(client_id, fruit, amount)

    def _process_eof(self, client_id):
        logging.info(f"Broadcasting EOF to control exchange for client {client_id}")
        self.control_exchange.send(message_protocol.internal.serialize_eof(client_id))
        

    def process_data_messsage(self, message, ack, nack):
        with self.message_lock:
            fields = message_protocol.internal.deserialize(message)
            if fields[0] == "DATA":
                self._process_data(*fields[1:])
            elif fields[0] == "EOF":
                self._process_eof(*fields[1:])
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_data_messsage)


class ControlConsumer:
    def __init__(self, fruit_store, message_lock):
        self.fruit_store = fruit_store
        self.message_lock = message_lock
        

    def _process_eof(self, client_id):
        with self.message_lock:
            items = self.fruit_store.get(client_id)
        
        for fi in items:
            target = int(hashlib.md5(fi.fruit.encode()).hexdigest(), 16) % AGGREGATION_AMOUNT
            self.data_output_exchanges[target].send(message_protocol.internal.serialize_data_message(client_id, fi.fruit, fi.amount))

        self.fruit_store.delete(client_id)

        for exchange in self.data_output_exchanges:
            exchange.send(message_protocol.internal.serialize_eof(client_id))

    def process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if fields[0] == "EOF":
            self._process_eof(fields[1])
        ack()

    def start(self):
        self.control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_CONTROL_EXCHANGE}_{ID}"]
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.control_exchange.start_consuming(self.process_control_message)


class FruitStore:
    def __init__(self):
        self.data = {}
        self.lock = threading.Lock()
    
    def add(self, client_id, fruit, amount):
        with self.lock:
            key = (client_id, fruit)
            self.data[key] = self.data.get(key, fruit_item.FruitItem(fruit, 0)) + fruit_item.FruitItem(fruit, int(amount))

    def get(self, client_id):
        with self.lock:
            return [fi for (c_id, _), fi in self.data.items() if c_id == client_id]

    def delete(self, client_id):
        with self.lock:
            keys_to_delete = [key for key in self.data if key[0] == client_id]
            for key in keys_to_delete:
                del self.data[key]


def main():
    logging.basicConfig(level=logging.INFO)
    fruit_store = FruitStore()
    message_lock = threading.Lock()
    sum_filter = SumFilter(fruit_store, message_lock)

    control_consumer = ControlConsumer(fruit_store, message_lock)
    control_thread = threading.Thread(target=control_consumer.start)
    control_thread.start()

    sum_filter.start()

    control_thread.join()
    return 0


if __name__ == "__main__":
    main()
