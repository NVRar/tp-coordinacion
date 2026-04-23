import os
import logging
import signal

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
        self.partial_tops = {}
        self.top_counts = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        fields = message_protocol.internal.deserialize(message)
        client_id = fields[1]
        partial_top = [fruit_item.FruitItem(f, a) for f, a in fields[2]]

        self.partial_tops[client_id] = self.partial_tops.get(client_id, []) + partial_top
        self.top_counts[client_id] = self.top_counts.get(client_id, 0) + 1

        if self.top_counts[client_id] < AGGREGATION_AMOUNT:
            ack()
            return

        all_items = sorted(self.partial_tops[client_id])
        top_items = all_items[-TOP_SIZE:]
        top_items.reverse()
        
        final_top = [(item.fruit, item.amount) for item in top_items]

        result = message_protocol.internal.serialize_result_message(client_id, final_top)
        try:
            self.output_queue.send(result)
            del self.partial_tops[client_id]
            del self.top_counts[client_id]
            ack()
        except Exception as e:
            logging.error(f"Error sending message: {e}")
            nack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)

    def close(self):
        self.input_queue.close()
        self.output_queue.close()

    def stop(self):
        self.input_queue.stop_consuming()

def main():
    logging.basicConfig(level=logging.INFO)

    with JoinFilter() as join_filter:
        signal.signal(signal.SIGTERM, lambda sig, frame: join_filter.stop())
        join_filter.start()

    return 0


if __name__ == "__main__":
    main()
