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

    def __init__(self, server_id, mq_host, mq_port, mq_user, mq_password, deluge_api):
        self.deluge_api = deluge_api
        self.offline_exchange = Exchange(
            'offline-exchange', 'direct', durable=True)
        self.torrent_queue = Queue('torrent-' + server_id,
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
        # logging.info("ON@")
        #xbody = message.body
        try:
            self._on_torrent_added(json.loads(body), message)
        except Exception as e:
            logging.info("!!!!FITAL_EXC %s", body)
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
                self._delive_torrent_parse_fail(-2,
                                                file_hash, 'TORRENT_PROCESS_FAILED', info)
                return
            self._add_new_torrent_file(info, data, torrent_hash)
        else:
            logging.warn("%s is not a torrent file. %s", url, mime)
            self._delive_torrent_parse_fail(-1,
                                            file_hash, 'MIME_MISMATCH', info)

    def _add_new_torrent_file(self, info, torrent_data, torrent_hash):
        try:
            torrent_id = self.deluge_api.add_torrent_file(
                info["hash"], base64.encodestring(torrent_data), {})
            if not torrent_id:
                logging.info("%s existed.", torrent_hash)
                torrent_id = torrent_hash
            else:
                logging.warn(
                    "Torrent id mismatch!!!! rechange....%s", json.dumps(info))
            logging.info("Added torrent success %s", torrent_id)
            # Get Torrent File Info
            file_data = self.deluge_api.get_torrent_status(
                torrent_id, [])
            self._delive_torrent_parse_success(info["hash"], file_data, info)
        except RuntimeError as ex:
            logging.warning(
                'Unable to add torrent, failed: %s', ex)
            self._delive_torrent_parse_fail(-5,
                                            info['hash'], 'DELUGE_ERROR', info)

    def _delive_torrent_parse_fail(self, status, url_hash, message, info):
        # Report
        logging.info("publish")
        self.producer.publish(
            {'success': False, 'status': status,
             'hash': url_hash, 'message': message, 'info': info},
            exchange='offline-exchange',
            routing_key="torrent-task.pre_parse",
            retry=True,
        )
        logging.info("publish_end")

    def _delive_torrent_parse_success(self, url_hash, data, info):
        # Report
        logging.info("suc_publish")
        self.producer.publish(
            {'success': True, 'status': 100, 'message': 'OK',
             'hash': url_hash, 'data': data, 'info': info},
            exchange='offline-exchange',
            routing_key="torrent-task.pre_parse",
            retry=True,
        )
        logging.info("suc_publish_end")
        logging.info(json.dumps(info))

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
