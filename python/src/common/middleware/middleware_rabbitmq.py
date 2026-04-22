import pika
import random
import string
from .middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)

DISCONNECTED_EXCEPTIONS = (
    pika.exceptions.AMQPConnectionError,
)


def _build_callback(on_message_callback):
    def _callback(ch, method, _properties, body):
        def ack():
            ch.basic_ack(delivery_tag=method.delivery_tag)

        def nack():
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        on_message_callback(body, ack, nack)

    return _callback

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name, connection=None, channel=None):
        if connection is None:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        else:
            self.connection = connection

        if channel is None:
            self.channel = self.connection.channel()
        else:
            self.channel = channel

        self.channel.queue_declare(queue=queue_name, durable=True, arguments={'x-queue-type': 'quorum'})
        self.channel.confirm_delivery()
        self.queue_name = queue_name

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent,
                )
            )
        except DISCONNECTED_EXCEPTIONS as exc:
            raise MessageMiddlewareDisconnectedError() from exc
        except Exception as exc:
            raise MessageMiddlewareMessageError() from exc

    def start_consuming(self, on_message_callback):
        try:
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=_build_callback(on_message_callback)
            )
            self.channel.start_consuming()
        except DISCONNECTED_EXCEPTIONS as exc:
            raise MessageMiddlewareDisconnectedError() from exc
        except Exception as exc:
            raise MessageMiddlewareMessageError() from exc

    def stop_consuming(self):
        if self.channel.is_open:
            try:
                self.channel.stop_consuming()
            except DISCONNECTED_EXCEPTIONS as exc:
                raise MessageMiddlewareDisconnectedError() from exc

    def close(self):
        close_error = None
        for resource in (self.channel, self.connection):
            try:
                if resource.is_open:
                    resource.close()
            except Exception as exc:
                if close_error is None:
                    close_error = exc

        if close_error is not None:
            raise MessageMiddlewareCloseError() from close_error


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys, exchange_type='direct', connection=None, channel=None):
        if connection is None:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        else:
            self.connection = connection

        if channel is None:
            self.channel = self.connection.channel()
        else:
            self.channel = channel

        self.channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type, durable=True)
        self.channel.confirm_delivery()
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys

    def send(self, message, routing_key=None):
        try:
            if routing_key is not None:
                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=routing_key,
                    body=message,
                    properties=pika.BasicProperties(
                        delivery_mode=pika.DeliveryMode.Persistent,
                    )
                )
            else:
                keys = self.routing_keys if self.routing_keys else [""]
                for routing_key in keys:
                    self.channel.basic_publish(
                        exchange=self.exchange_name,
                        routing_key=routing_key,
                        body=message,
                        properties=pika.BasicProperties(
                            delivery_mode=pika.DeliveryMode.Persistent,
                        )
                    )
        except DISCONNECTED_EXCEPTIONS as exc:
            raise MessageMiddlewareDisconnectedError() from exc
        except Exception as exc:
            raise MessageMiddlewareMessageError() from exc
          

    def start_consuming(self, on_message_callback, queue_name=None):
        try:
            if queue_name:
                self.channel.queue_declare(queue=queue_name, durable=True, arguments={'x-queue-type': 'quorum'})
            else:
                result = self.channel.queue_declare(queue='', exclusive=True)
                queue_name = result.method.queue

            if self.routing_keys:
                for routing_key in self.routing_keys:
                    self.channel.queue_bind(exchange=self.exchange_name, queue=queue_name, routing_key=routing_key)
            else:
                self.channel.queue_bind(exchange=self.exchange_name, queue=queue_name)

            self.channel.basic_consume(
                queue=queue_name,
                on_message_callback=_build_callback(on_message_callback)
            )
            self.channel.start_consuming()
        except DISCONNECTED_EXCEPTIONS as exc:
            raise MessageMiddlewareDisconnectedError() from exc
        except Exception as exc:
            raise MessageMiddlewareMessageError() from exc

    def stop_consuming(self):
        try:
            if self.channel.is_open:
                self.channel.stop_consuming()
        except DISCONNECTED_EXCEPTIONS as exc:
            raise MessageMiddlewareDisconnectedError() from exc
        
            
    def close(self):
        close_error = None
        for resource in (self.channel, self.connection):
            try:
                if resource.is_open:
                    resource.close()
            except Exception as exc:
                if close_error is None:
                    close_error = exc

        if close_error is not None:
            raise MessageMiddlewareCloseError() from close_error
