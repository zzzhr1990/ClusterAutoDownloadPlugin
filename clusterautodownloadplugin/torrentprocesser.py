import json
import time
import requests
import os
import base64
import traceback
import datetime
import threading
from multiprocessing import Process
from multiprocessing.queues import Empty
from filemanager import BucketManager
from workconfig import WorkConfig
from wcs.services.uploadprogressrecorder import UploadProgressRecorder
from wcs.commons.util import etag
from deluge.log import LOG as log
from workconfig import get_auth
from taskprocess import TaskProcess
from wcssliceupload import WcsSliceUpload



class TorrentProcesser(Process):
    """Process Torrent"""
    def __init__(self, process_id, in_queue, command_queue):
        self.process_id = process_id
        self.in_queue = in_queue
        self.command_queue = command_queue
        self.looping_thread = threading.Thread(target=self._loop)
        self.looping_thread.daemon = False
        self.terminated = False
        self.task = TaskProcess(WorkConfig.SERVER_URL)
        super(TorrentProcesser, self).__init__()

    def _loop(self):
        while True:
            if not self.command_queue.empty():
                log.info("Torrent process %d terminated.", self.process_id)
                self.terminated = True
                self.terminate()
                return
            time.sleep(1)

    def _sleep_and_wait(self, stime):
        if not self.terminated:
            if stime < 1:
                stime = 1
            for i in range(0, stime):
                if not self.terminated:
                    time.sleep(1)

    def _fetch_and_process(self):
        try:
            data = self.in_queue.get(True, 2)
            #log.info("%d processing torrents %s", self.process_id, data["hash"])
            if data != None:
                self.process_single_torrent(data)

        except Empty:
            time.sleep(2)

    def run(self):
        """Main process"""
        self.looping_thread.start()
        try:
            while not self.terminated:
                self._fetch_and_process()
        except Exception as e:
            log.error("Exception occored in torrent process. %s -- \r\n%s",\
            e, traceback.format_exc())
        finally:
            self.terminated()

    def process_single_torrent(self, torrent_info):
        """Add one torrent to process"""
        is_finished = torrent_info["is_finished"]
        torrent_hash = torrent_info["hash"]
        dest_path = torrent_info["save_path"]
        if torrent_info["move_completed"]:
            dest_path = torrent_info["move_completed_path"]
        progress = torrent_info["progress"]

        all_success_download = True
        #Assign download succ
        for index, file_detail in enumerate(torrent_info["files"]):
 #           if self.terminate:
 #               if self.current_upload != None:
 #                   self.current_upload.stop()
 #                   return
                    #TODO CHECK THIS
            file_progress = torrent_info["file_progress"][index]
            file_download = torrent_info["file_priorities"][index]
            if file_download:
                if file_progress == 1:
                    file_path = (u'/'.join([dest_path, file_detail["path"]])).encode('utf8')
                    if os.path.exists(file_path):
                        a_size = os.path.getsize(file_path)
                        if a_size == file_detail["size"]:
                            single_success_download = self._upload_to_ws(file_path)
                            if not single_success_download:
                                all_success_download = False
                        else:
                            all_success_download = False
                            log.warn("file %s size not equal %ld (need %ld)..."\
                            , file_path, a_size, file_detail["size"])
                    else:
                        all_success_download = False
                        log.warn("file %s download complete, but cannot be found...", file_path)
                else:
                    all_success_download = False

    def _post_file(self, file_path, file_key):
        auth = get_auth()
        putpolicy = {'scope':'other-storage:' + file_key\
            , 'deadline':str(int(time.time()) * 1000 + 86400000), \
            'overwrite':1, 'returnBody':\
            'url=$(url)&fsize=$(fsize)&bucket=$(bucket)&key=$(key)&hash=$(hash)&fsize=$(fsize)&mimeType=$(mimeType)'}
        token = auth.uploadtoken(putpolicy)
        param = {'position':'local', 'message':'upload'}
        upload_progress_recorder = UploadProgressRecorder()
        modify_time = time.time()
        sliceupload = WcsSliceUpload(token, file_path, file_key, param\
            , upload_progress_recorder, modify_time, WorkConfig.PUT_URL)
        #self.current_upload = sliceupload
        if self.terminate:
            return 0, None
        code, hashvalue = sliceupload.slice_upload()
        log.info("upload code %d, %s", code, json.dumps(hashvalue))

    def _upload_to_ws(self, file_path):
        #begin = time.time()
        file_key = etag(file_path)
        #log.info("Process %ld in %f s", file_size, time.time() - begin)
        bucket = "other-storage"
        file_hash = etag(file_path)
        file_key = "raw/" + file_hash
        filemanager = BucketManager(get_auth(), WorkConfig.MGR_URL)
        code, text = filemanager.stat(bucket, file_key)
        if code == 200:
            #file exists
            if isinstance(text, dict):
                result = text
            else:
                result = json.loads(text)
            remote_hash = result['hash']
            #remote_size = long(result['fsize'])
            if remote_hash != file_hash:
                log.warn("file: %s hash mismatch: local: %s, remote: %s"\
                , file_key, file_hash, remote_hash)
                #repost
                self._post_file(file_path, file_key)
 #           else:
 #               log.info("%s exists on server, ignore...", file_path)
            #TODO: check and report..
        else:
            if code == 404:
                self._post_file(file_path, file_key)
            else:
                log.warn("file get message %d, %s, we have to repost file.", code, text)
                self._post_file(file_path, file_key)
        return file_path


