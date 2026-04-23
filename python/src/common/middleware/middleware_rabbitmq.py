import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.queue_name = queue_name
        self.channel.queue_declare(queue=queue_name, durable=True)

    def close(self):
        self.connection.close()

    def send(self, message):
        self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message, properties=pika.BasicProperties(delivery_mode=2))

    def start_consuming(self, callback):
        self.channel.basic_qos(prefetch_count=1)
        def callback_wrapper(channel, method, properties, body):
            ack = lambda: channel.basic_ack(delivery_tag=method.delivery_tag)
            nack = lambda: channel.basic_nack(delivery_tag=method.delivery_tag)
            callback(body, ack, nack)

        self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback_wrapper)
        self.channel.start_consuming()

    def stop_consuming(self):
        self.connection.add_callback_threadsafe(self.channel.stop_consuming)
  

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

    def close(self):
        self.connection.close()
    
    def send(self, message):
        for routing_key in self.routing_keys:
            self.channel.basic_publish(exchange=self.exchange_name, routing_key=routing_key, body=message)

    def start_consuming(self, callback):
        self.channel.basic_qos(prefetch_count=1)
        result = self.channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        for routing_key in self.routing_keys:
            self.channel.queue_bind(exchange=self.exchange_name, queue=queue_name, routing_key=routing_key)
        def callback_wrapper(channel, method, properties, body):
            ack = lambda: channel.basic_ack(delivery_tag=method.delivery_tag)
            nack = lambda: channel.basic_nack(delivery_tag=method.delivery_tag)
            callback(body, ack, nack)
        self.channel.basic_consume(queue=queue_name, on_message_callback=callback_wrapper)
        self.channel.start_consuming()

    def stop_consuming(self):
        self.connection.add_callback_threadsafe(self.channel.stop_consuming)