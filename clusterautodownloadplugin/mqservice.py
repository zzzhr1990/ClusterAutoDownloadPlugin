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
import traceback


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
        self.server_id = server_id
        deluge_api.eventmanager.register_event_handler(
            "TorrentFileCompletedEvent", self._on_torrent_file_completed)
        deluge_api.eventmanager.register_event_handler(
            "TorrentFinishedEvent", self._on_torrent_completed)
        # listen to TorrentFileRenamedEvent
        # may be error?
        # TorrentFileCompletedEvent?torrent_id
        # TorrentFileCompletedEvent?single file..torrent_id, index
        self.name_cache = {}

    def _on_torrent_completed(self, torrent_id):
        logging.info("%s downloaded.", torrent_id)

    def _on_torrent_file_completed(self, torrent_id, index):
        # get file info...
        try:
            torrent_info = self.deluge_api.get_torrent_status(
                torrent_id,
                ['files', 'save_path', 'move_completed', 'move_completed_path'])
            file_data = torrent_info['files'][index]
#            size = file_data['size']
            dest_path = torrent_info["save_path"]
            if torrent_info["move_completed"]:
                dest_path = torrent_info["move_completed_path"]
            file_path = u'/'.join([dest_path, file_data["path"]])
            logging.info("%s of %d completed in %d.",
                         torrent_id, index, file_path)
        except RuntimeError as ex:
            logging.error(ex)

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
            self._on_torrent_added(x_body, message)
        except Exception as e:
            logging.info("!!!!FITAL_EXC %s", body)
            logging.error(e)

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
        message.ack()
        # anyway, check it first.

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
        # Check MIME
        mime = Util.mime_buffer(data)
        torrent_hash = None
        if mime == 'application/x-bittorrent':
            try:
                torrent_info = lt.torrent_info(lt.bdecode(data))
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
                    info, data, torrent_hash, file_map, orign_map)
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
            self.deluge_api.resume_torrent([torrent_id])
            # Check Every File Progress...
            try:
                new_file_data = self.deluge_api.torrentmanager[torrent_id].handler.file_progress(
                )
                logging.info(new_file_data)
            except Exception as sx:
                logging.error(sx)

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
