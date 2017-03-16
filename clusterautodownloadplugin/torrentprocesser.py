import json
import time
import requests
import os
import base64
import traceback
import datetime
from multiprocessing import Process
from filemanager import BucketManager
from workconfig import WorkConfig
from wcs.services.uploadprogressrecorder import UploadProgressRecorder
from wcs.commons.util import etag
from deluge.log import LOG as log
from workconfig import get_auth
from wcssliceupload import WcsSliceUpload



class TorrentProcesser(Process):
    
    """Process Torrent"""
        
    def __init__(self, torrent_info, group=None, target=None, name=None, args=(), kwargs={}):
        self.disable = False
        self.current_upload = None
        self.torrent_info = torrent_info
        super(TorrentProcesser, self).__init__()

    def stop(self):
        """Stop"""
        if self.disable:
            return
        self.disable = True
        if self.current_upload != None:
            self.current_upload.stop()
    def run(self):
        try:
            self.process_single_torrent()
        except Exception as e:
            log.error("Exception occored in torrent processs.")
        finally:
            self.disable = True
    def finished(self):
        """determin"""
        return self.disable

    def process_single_torrent(self):
        """Add one torrent to process"""
        torrent_info = self.torrent_info
        is_finished = torrent_info["is_finished"]
        torrent_hash = torrent_info["hash"]
        dest_path = torrent_info["save_path"]
        if torrent_info["move_completed"]:
            dest_path = torrent_info["move_completed_path"]
        progress = torrent_info["progress"]

        all_success_download = True
        #Assign download succ
        for index, file_detail in enumerate(torrent_info["files"]):
            if self.disable:
                if self.current_upload != None:
                    self.current_upload.stop()
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
            , 'deadline':str(int(time.time()) * 1000 + 86400000), 'overwrite':1}
        token = auth.uploadtoken(putpolicy)
        param = {'position':'local', 'message':'upload'}
        upload_progress_recorder = UploadProgressRecorder()
        modify_time = time.time()
        sliceupload = WcsSliceUpload(token, file_path, file_key, param\
            , upload_progress_recorder, modify_time, WorkConfig.PUT_URL)
        code, hashvalue = sliceupload.slice_upload()
        self.current_upload = sliceupload
        log.info("upload %d, %s", code, json.dumps(hashvalue))

    def _upload_to_ws(self, file_path):

        log.info("starting proc to ws")
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
            else:
                log.info("%s exists on server, ignore...", file_path)
            #TODO: check and report..
        else:
            if code == 404:
                self._post_file(file_path, file_key)
            else:
                log.warn("file get message %d, %s, we have to repost file.", code, text)
                self._post_file(file_path, file_key)
        return file_path
