from kombu import Consumer as KConsumer
from kombu.mixins import ConsumerMixin
from kombu import Connection, Exchange, Queue
from deluge._libtorrent import lt
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
        xbody = message.body
        try:
            self._on_torrent_added(json.loads(body))
        except Exception as e:
            logging.info(e)

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
        logging.info(type(info))
        req = requests.get(info["url"], timeout=5)
        if req.status_code == 200:
            logging.info("Down!!")
            # torrent_id = self.deluge_api.add_torrent_file(info["hash"],
            #                                              base64.encodestring(req.content), {})
            # logging.info(torrent_id)
            filedump = req.content
            try:
                torrent_info = lt.torrent_info(lt.bdecode(filedump))
                torrent_id = str(torrent_info.info_hash())
                logging.info("Torrent id %s", torrent_id)
            except RuntimeError as ex:
                logging.info(ex)
        else:
            logging.info("%s download %d", info["url"], req.status_code)
            return False
