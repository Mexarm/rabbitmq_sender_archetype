#!/usr/bin/env python
import pika

SEND_QUEUE = 'send'

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

queue = channel.queue_declare(queue=SEND_QUEUE, durable=True)

for i in range(20):
    msg = f'my msg {i}'
    channel.basic_publish(exchange='',
                          routing_key=SEND_QUEUE,
                          body=msg)
    print(f" [x] Sent '{msg}'")

connection.close()
