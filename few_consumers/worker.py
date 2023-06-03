#!/usr/bin/env python
import pika
import time

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))

channel = connection.channel()
channel.queue_declare(queue='hello')


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    time.sleep(body.count(b'.'))
    print(" [x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)


'''
This uses the basic.qos protocol method to tell RabbitMQ not to give more than one message
to a worker at a time. Or, in other words, don't dispatch a new message to a worker until
it has processed and acknowledged the previous one. Instead, it will dispatch it to the next 
worker that is not still busy.'''
channel.basic_qos(prefetch_count=1)

channel.basic_consume('hello', callback)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
