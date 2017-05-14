from kombu import Consumer as KConsumer
from kombu.mixins import ConsumerProducerMixin
from kombu import Connection, Exchange, Queue
from deluge._libtorrent import lt
from util import Util
from io import BytesIO
import logging
import uuid
import json
import threading
import requests
import base64
import magic


class MqService(ConsumerProducerMixin):
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
            self._on_torrent_added(json.loads(body), message)
        except Exception as e:
            logging.info(e)

    def start_async(self):
        """Start this async"""
        th_run = threading.Thread(target=self.run)
        th_run.start()

    def stop(self):
        """stop"""
        self.should_stop = True

    def _on_torrent_added(self, info, message):
        # Get File.
        url = info["url"]
        file_hash = info["hash"]
        data = self._try_and_get_content(url, file_hash)
        if not data:
            message.requeue()
            return
        else:
            message.ack()
        # Check MIME
        mime = Util.mime_buffer(data)
        torrent_hash = None
        if mime == 'application/x-bittorrent':
            try:
                torrent_info = lt.torrent_info(lt.bdecode(data))
                torrent_hash = unicode(torrent_info.info_hash())
            except RuntimeError as ex:
                logging.warning(
                    'Unable to add torrent, decoding filedump failed: %s', ex)
                self._delive_torrent_parse_fail(-2, file_hash)
                return
            self._add_new_torrent_file(info, data, torrent_hash)
        else:
            logging.warn("%s is not a torrent file. %s", url, mime)
            self._delive_torrent_parse_fail(-1, file_hash)

    def _add_new_torrent_file(self, info, torrent_data, torrent_hash):
        pass

    def _delive_torrent_parse_fail(self, status, file_or_url_hash):
        # Report
        pass

    def _try_and_get_content(self, url, file_hash, try_time=10):
        req = requests.get(url, timeout=5)
        if try_time < 0:
            logging.error("Download file %s error", url)
        try:
            if req.status_code < 400:
                filedump = req.content
                etag = Util.wcs_etag_bytes(filedump)
                if etag != file_hash:
                    logging.warn("File %s , hash %s mismatch. atr %s",
                                 str(url), str(file_hash), str(etag))
                    return self._try_and_get_content(url, file_hash, try_time - 1)
                return filedump
            else:
                logging.warn("File %s , errorcode %d.",
                             str(url), req.status_code)
                return self._try_and_get_content(url, file_hash, try_time - 1)
        except RuntimeError as ex:
            logging.error(ex)
            return self._try_and_get_content(url, file_hash, try_time - 1)
