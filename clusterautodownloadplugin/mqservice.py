from kombu.mixins import ConsumerMixin
from kombu import Connection, Exchange, Queue
import logging
import uuid
import threading


class MqService(ConsumerMixin):
    """Mq"""

    def __init__(self, mq_host, mq_port, mq_user, mq_password, deluge_api):
        self.deluge_api = deluge_api
        self.offline_exchange = Exchange(
            'offline-exchange', 'direct', durable=True)
        self.torrent_queue = Queue('torrent-' + uuid.uuid4().urn[9:],
                                   exchange=self.offline_exchange,
                                   routing_key='torrent-task.add', auto_delete=True)
        self.connection = Connection(
            hostname=mq_host, port=mq_port, password=mq_password, userid=mq_user)
        self.torrent_queue(self.connection).declare()

    def get_consumers(self, Consumer, channel):
        """D"""
        return [Consumer(channel, self.torrent_queue,
                         callbacks=[self.on_message])]

    def on_message(self, body, message):
        """d"""
        logging.info(type(body))
        message.ack()

    def start_async(self):
        """Start this async"""
        th_run = threading.Thread(target=self.run)
        th_run.start()

    def stop(self):
        """stop"""
        self.should_stop = True
