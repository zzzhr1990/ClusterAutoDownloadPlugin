from kombu import Consumer as KConsumer
from kombu.mixins import ConsumerMixin
from kombu import Connection, Exchange, Queue
import logging
import uuid
import json
import threading
import requests
import base64


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
        return [KConsumer(channel, self.torrent_queue,
                          callbacks=[self.on_message])]

    def on_message(self, body, message):
        """d"""
        """{\"bucket\":\"other-storage\",\"storeFileId\":\"Ft7r9oFcC5lFFXIPLQKmPIjE7EuD\",
        \"type\":\"torrent\",\"userId\":\"1\",\"key\":\"user-upload/Ft7r9oFcC5lFFXIPLQKmPIjE7EuD\",
        \"url\":\"http://other.qiecdn.com/user-upload/Ft7r9oFcC5lFFXIPLQKmPIjE7EuD\",
        \"hash\":\"Ft7r9oFcC5lFFXIPLQKmPIjE7EuD\"}"""
        self._on_torrent_added(message.body)
        message.ack()

    def start_async(self):
        """Start this async"""
        th_run = threading.Thread(target=self.run)
        th_run.start()

    def stop(self):
        """stop"""
        self.should_stop = True

    def _on_torrent_added(self, info):
        # Get File.
        req = requests.get(info["url"],
                           headers={"X-Task-Token": "1024tasktoken"}, timeout=5)
        if req.status_code == 200:
            torrent_id = self.deluge_api.add_torrent_file(info["hash"],
                                                          base64.encodestring(req.content), {})
            logging.info("Adding torrent %s", torrent_id)
        else:
            logging.info(req.status_code)
