"""
Scheduler queues
"""

import marshal, cPickle as pickle

from queuelib import queue

from scrapy.utils import conf

import pika
import code

class RabbitQueue(object):
    """Queue that uses RabbitMQ with Pika for the backend"""

    def __init__(self, queuename="default"):
        self.queuename = queuename
        self.seen_queues = []
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(conf.get_config().get('RabbitConfig', 'host')))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queuename)
        self.seen_queues.append(self.queuename)

    def push(self, string):
        # print "Pushed %s" % string
        self.channel.basic_publish(exchange='',
                                   routing_key=self.queuename,
                                   body=string)

    def push_to_queue(self, string, queue):
        if queue not in self.seen_queues:
            self.channel.queue_declare(queue=queue)
            self.seen_queues.append(queue)
        self.channel.basic_publish(exchange='',
                                   routing_key=queue,
                                   body=string)

    def pop(self, queuename=None):
        qn = queuename or self.queuename
        method_frame, properties, body = self.channel.basic_get(queue=qn)
        if method_frame:
            self.channel.basic_ack(method_frame.delivery_tag)
        return body

    def __len__(self):
        return self.channel.queue_declare(self.queuename).method.message_count

    def close(self):
        self.channel.basic_cancel()
        self.channel.close()
        self.connection.close()

def _serializable_queue(queue_class, serialize, deserialize):

    class SerializableQueue(queue_class):

        def push(self, obj):
            # code.interact('push', local=locals())
            s = serialize(obj)
            parent = super(SerializableQueue, self)
            if obj.callback and hasattr(parent, 'push_to_queue') and callable(parent.push_to_queue):
                parent.push_to_queue(s, obj.callback.im_self.__class__.__module__)
            else:
                super(SerializableQueue, self).push(s)

        def pop(self):
            s = super(SerializableQueue, self).pop()
            if s:
                return deserialize(s)

    return SerializableQueue

def _pickle_serialize(obj):
    try:
        return pickle.dumps(obj, protocol=2)
    except pickle.PicklingError as e:
        raise ValueError(str(e))

PickleFifoDiskQueue = _serializable_queue(queue.FifoDiskQueue, \
    _pickle_serialize, pickle.loads)
PickleLifoDiskQueue = _serializable_queue(queue.LifoDiskQueue, \
    _pickle_serialize, pickle.loads)
MarshalFifoDiskQueue = _serializable_queue(queue.FifoDiskQueue, \
    marshal.dumps, marshal.loads)
MarshalLifoDiskQueue = _serializable_queue(queue.LifoDiskQueue, \
    marshal.dumps, marshal.loads)
FifoMemoryQueue = queue.FifoMemoryQueue
LifoMemoryQueue = queue.LifoMemoryQueue

SerializedRabbitQueue = _serializable_queue(RabbitQueue, _pickle_serialize, pickle.loads)
