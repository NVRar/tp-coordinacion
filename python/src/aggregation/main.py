import os
import logging
import bisect
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


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_data = {}
        self.eof_counts = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")

        if client_id not in self.fruit_data:
            self.fruit_data[client_id] = []

        fruit_list = self.fruit_data[client_id]

        for i in range(len(fruit_list)):
            if fruit_list[i].fruit == fruit:
                updated_item = fruit_list[i] + fruit_item.FruitItem(fruit, amount)
                fruit_list.pop(i)
                bisect.insort(fruit_list, updated_item)
                return
        bisect.insort(fruit_list, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id):
        logging.info("Received EOF")
        
        self.eof_counts[client_id] = self.eof_counts.get(client_id, 0) + 1
        
        if self.eof_counts[client_id] < SUM_AMOUNT:
            return

        if client_id in self.fruit_data:
            fruit_list = self.fruit_data[client_id]
            fruit_chunk = list(fruit_list[-TOP_SIZE:])
            fruit_chunk.reverse()
            fruit_top = list(
                map(
                    lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                    fruit_chunk,
                )
            )
            self.output_queue.send(message_protocol.internal.serialize_top_message(client_id, fruit_top))
            del self.fruit_data[client_id]
        else:
            self.output_queue.send(message_protocol.internal.serialize_top_message(client_id, []))
            
        del self.eof_counts[client_id]

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if fields[0] == "DATA":
            self._process_data(*fields[1:])
        elif fields[0] == "EOF":
            self._process_eof(*fields[1:])
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)

    def stop(self):
        self.input_exchange.stop_consuming()

    def close(self):
        self.input_exchange.close()
        self.output_queue.close()

def main():
    logging.basicConfig(level=logging.INFO)

    with AggregationFilter() as aggregation_filter:
        signal.signal(signal.SIGTERM, lambda sig, frame: aggregation_filter.stop())
        aggregation_filter.start()

    return 0


if __name__ == "__main__":
    main()
