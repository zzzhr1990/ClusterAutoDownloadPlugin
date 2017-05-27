from kombu import Consumer as KConsumer
from kombu.mixins import ConsumerProducerMixin
from kombu import Connection, Exchange, Queue
from deluge._libtorrent import lt
from tcommon import get_magnet_info
from magnetparser import MagnetParser
from torrentuploader import TorrentUploader
from util import Util
from io import BytesIO
import logging
import uuid
import json
import threading
import requests
import base64
import magic
import traceback
import threading


class MqService(ConsumerProducerMixin):
    """Mq"""

    def __init__(self, server_id, mq_host, mq_port, mq_user, mq_password, deluge_api, user_controller):
        self.deluge_api = deluge_api
        self.offline_exchange = Exchange(
            'offline-exchange', 'direct', durable=True)
        self.user_controller = user_controller
        self.torrent_queue = Queue('torrent-' + server_id,
                                   exchange=self.offline_exchange,
                                   routing_key='torrent-task.add', auto_delete=True)
        self.connection = Connection(
            hostname=mq_host, port=mq_port, password=mq_password, userid=mq_user)
        self.torrent_queue(self.connection).declare()
        self.server_id = server_id
        # may be error?
        # TorrentFileCompletedEvent?torrent_id
        # TorrentFileCompletedEvent?single file..torrent_id, index

    def _on_torrent_rename(self, torrent_id, index, name):
        logging.info("changing %s to %s (%d)", torrent_id, name, index)

    def get_consumers(self, Consumer, channel):
        """D"""
        return [KConsumer(channel, self.torrent_queue,
                          callbacks=[self.on_message])]

    def on_message(self, body, message):
        """d"""
        # logging.info("ON@")
        #xbody = message.body
        try:
            x_body = json.loads(body)
            if x_body['type'] == 'magnet':
                self._on_magnet_added(x_body, message)
            else:
                self._on_torrent_added(x_body, message)
        except Exception as e:
            logging.info("!!%s!!FITAL_EXC %s", type(e), body)
            logging.error(traceback.format_exc())

    def start_async(self):
        """Start this async"""
        th_run = threading.Thread(target=self.run)
        th_run.start()

    def stop(self):
        """stop"""
        self.should_stop = True

    def _on_magnet_added(self, info, message):
        info['sid'] = self.server_id
        url = info["url"]
        file_hash = info["hash"]
        # Thus the `filehash` maybe error
        message.ack()
        # anyway, check it first.
        magnet_info = get_magnet_info(url)
        if magnet_info:
            # start
            torrent_hash = magnet_info['info_hash']
            if torrent_hash != info['infoHash']:
                logging.warning("%s info hash mismatch! remote : %s, %s",
                                url, magnet_info['info_hash'], torrent_hash)
            self._add_new_magnet_url(info, file_hash, url, torrent_hash)
        else:
            logging.error('Unable to add magnet, invalid magnet info: %s', url)
            self._delive_torrent_parse_fail(-5,
                                            file_hash, 'TORRENT_PROCESS_FAILED', info)
        # re_magnets = re.compile('(magnet:\?xt\S+)&tr=')?

        # do we have magnet info in torrent list?
        #
    def _on_torrent_added(self, info, message):
        # Get File.
        info['sid'] = self.server_id
        url = info["url"]
        file_hash = info["hash"]
        data = self._try_and_get_content(url, file_hash)
        if not data:
            message.requeue()
            return
        else:
            message.ack()
        #
        self._add_torrent_next_step(url, info, file_hash, data)

    def _add_torrent_next_step(self, url, info, file_hash, torrent_dump):
        # Check MIME
        mime = Util.mime_buffer(torrent_dump)
        torrent_hash = None
        if mime == 'application/x-bittorrent':
            try:
                torrent_info = lt.torrent_info(lt.bdecode(torrent_dump))
                # create file map
                torrent_hash = unicode(torrent_info.info_hash())
                files_count = torrent_info.num_files()
                file_map = {}
                orign_map = {}
                for i in range(0, files_count):
                    file_inf = torrent_info.file_at(i)
                    file_name = file_inf.path
                    orign_map[i] = file_name
                    file_map[i] = torrent_hash + "/" + Util.md5(file_name)
                self._add_new_torrent_file(
                    info, torrent_dump, torrent_hash, file_map, orign_map)
            except RuntimeError as ex:
                logging.warning(
                    'Unable to add torrent, decoding filedump failed: %s', ex)
                self._delive_torrent_parse_fail(-2,
                                                file_hash, 'TORRENT_PROCESS_FAILED', info)
                return
        else:
            logging.warn("%s is not a torrent file. %s", url, mime)
            self._delive_torrent_parse_fail(-1,
                                            file_hash, 'MIME_MISMATCH', info)

    def _add_new_magnet_url(self, info, file_hash, magnet_url, torrent_hash):
        # Checking if has torrent -0-
        logging.info("paring magnet %s", magnet_url)
        parser = MagnetParser()
        torrent_dump = parser.start_parse(torrent_hash)
        if torrent_dump:
            self._process_magnet_to_torrent_files(
                info, magnet_url, file_hash, torrent_dump)
            logging.info("%s convered to torrent.", magnet_url)
            try:
                self.deluge_api.add_torrent_magnet(magnet_url, [])
            except RuntimeError as ex:
                logging.error(ex)
            return
        try:
            torrent_id = self.deluge_api.add_torrent_magnet(magnet_url, [])
            if not torrent_id:
                logging.info("%s existed.", torrent_hash)
                torrent_id = torrent_hash
            else:
                if torrent_hash != torrent_id:
                    logging.warn(
                        "Magnet id mismatch!!!! rechange....%s", json.dumps(info))
            logging.info(
                "Added magnet [%s] success %s", magnet_url, torrent_id)
            self._delive_torrent_parse_success(info["hash"], [], info)
            self.deluge_api.resume_torrent([torrent_id])
        except RuntimeError as ex:
            logging.warning(
                'Unable to add magnet %s, failed: %s', magnet_url, ex)
            self._delive_torrent_parse_fail(-7,
                                            info['hash'], 'DELUGE_ERROR', info)

    def _process_magnet_to_torrent_files(self, info, magnet_url, file_hash, torrent_dump):
        self._add_torrent_next_step(magnet_url, info, file_hash, torrent_dump)
        upload_tread = threading.Thread(
            target=self._upload_magnet_to_torrent_files, args=(file_hash, torrent_dump,))
        upload_tread.start()
        # new thread, give a new data...

    def _upload_magnet_to_torrent_files(self, file_hash, torrent_dump):
        uploader = TorrentUploader(self.user_controller)
        res = uploader.post_file(-1, file_hash,
                                 "application/x-bittorrent", torrent_dump)
        if not res:
            logging.warning('upload torrent failed...')

    def _add_new_torrent_file(self, info, torrent_data, torrent_hash, file_map, orign_map):
        try:
            torrent_id = self.deluge_api.add_torrent_file(
                info["hash"], base64.encodestring(torrent_data),
                {'add_paused': True, 'mapped_files': file_map})
            if not torrent_id:
                logging.info("%s existed.", torrent_hash)
                torrent_id = torrent_hash
            else:
                if torrent_hash != torrent_id:
                    logging.warn(
                        "Torrent id mismatch!!!! rechange....%s", json.dumps(info))
            logging.info("Added torrent success %s", torrent_id)
            # Get Torrent File Info
            file_data = self.deluge_api.get_torrent_status(
                torrent_id,
                [])
            for file_info in file_data['files']:
                file_info['path'] = orign_map[file_info['index']]
            self._delive_torrent_parse_success(info["hash"], file_data, info)
            logging.info("add torrent data %s", json.dumps(file_data))
            self.deluge_api.resume_torrent([torrent_id])
            file_data = self.deluge_api.get_torrent_status(
                torrent_id,
                [])
            for file_info in file_data['files']:
                file_info['path'] = orign_map[file_info['index']]
            logging.info("add torrent data finished %s", json.dumps(file_data))

            # mapped_files
            # Change FileName...
            """
            to_change = []
            for file_info in file_data['files']:
                to_change.append(
                    (file_info['index'], torrent_id + '/'
                     + Util.md5(file_info['path'].encode('utf-8').strip())))
            # Record original file to download.
            self.name_cache[torrent_id] = {}
            self.name_cache[torrent_id]['data'] = file_data['file_priorities']
            self.name_cache[torrent_id]['count'] = 0
            tmp = []
            for d in file_data['file_priorities']:
                tmp.append(0)
            self.deluge_api.set_torrent_file_priorities(torrent_id, tmp)
            logging.info(json.dumps(tmp))
            logging.info(json.dumps(file_data['file_priorities']))
            self.deluge_api.resume_torrent([torrent_id])
            self.deluge_api.rename_files(torrent_id, to_change)
            logging.info("File Renamed. %s", torrent_id)
            """
        except RuntimeError as ex:
            logging.warning(
                'Unable to add torrent, failed: %s', ex)
            self._delive_torrent_parse_fail(-5,
                                            info['hash'], 'DELUGE_ERROR', info)

    def _delive_torrent_parse_fail(self, status, url_hash, message, info):
        # Report
        self.producer.publish(
            {'success': False, 'status': status,
             'hash': url_hash, 'message': message, 'info': info},
            exchange='offline-exchange',
            routing_key="torrent-task.pre_parse",
            retry=True,
        )

    def delive_torrent_progress(self, info):
        # Report
        self.producer.publish(
            info,
            exchange='offline-exchange',
            routing_key="offline.upload.check",
            retry=True,
        )

    def _delive_torrent_parse_success(self, url_hash, data, info):
        # Report
        self.producer.publish(
            {'success': True, 'status': 100, 'message': 'OK',
             'hash': url_hash, 'data': data, 'info': info},
            exchange='offline-exchange',
            routing_key="torrent-task.pre_parse",
            retry=True,
        )

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
