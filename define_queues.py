#!/usr/bin/env python
import pika

RECEIVED = 0
RETRY_DELAY_MS = 5000
_DELIVERY_MODE_PERSISTENT = 2
EXCHANGE = ''
SEND_QUEUE = 'send'
RETRY_QUEUE = 'retry'

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.basic_qos(prefetch_count=10, global_qos=False)
channel.queue_declare(queue=SEND_QUEUE, durable=True)
channel.queue_declare(
    queue=RETRY_QUEUE,
    durable=True,
    arguments={
        'x-message-ttl': RETRY_DELAY_MS,
        'x-dead-letter-exchange': EXCHANGE,
        'x-dead-letter-routing-key': SEND_QUEUE
    }
)


print('queues created, closing channel...')
channel.close()
