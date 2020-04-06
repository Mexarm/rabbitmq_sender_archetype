#!/usr/bin/env python
import pika
import time
import random

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


def fakesend():
    status_codes = [200, 200, 200, 200, 500]
    return random.choice(status_codes)


def callback(ch, method, properties, body):
    global RECEIVED
    RECEIVED += 1

    print(" [x] (%i) Received %r" % (RECEIVED, body))
    time.sleep(random.randint(1, 4))
    result = fakesend()

    channel.basic_ack(delivery_tag=method.delivery_tag)

    if result != 200:
        print(" [x] (%i) Error Sending, retrying ..." % RECEIVED)
        RECEIVED -= 1
        channel.basic_publish(
            exchange=EXCHANGE,
            routing_key=RETRY_QUEUE,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=_DELIVERY_MODE_PERSISTENT
            )
        )

    else:
        print(" [x] (%i) Done" % RECEIVED)
        with open(body.decode('utf-8'), 'w') as handle:
            handle.write(body.decode('utf-8'))


channel.basic_consume(
    queue=SEND_QUEUE,
    on_message_callback=callback,
)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
