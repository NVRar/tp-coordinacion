import os
import logging
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self, fruit_store):
        self.fruit_store = fruit_store
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")
        self.fruit_store.add(client_id, fruit, amount)

    def _process_eof(self, client_id):
        logging.info(f"Broadcasting data messages")
        
        items = self.fruit_store.get(client_id)
        for final_fruit_item in items:
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(
                    message_protocol.internal.serialize(
                        [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                    )
                )
                
        self.fruit_store.delete(client_id)

        logging.info(f"Broadcasting EOF message")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))


    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_data_messsage)

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
    sum_filter = SumFilter(fruit_store)
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
