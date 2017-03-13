import json
import time
import requests
import os
import base64
import traceback
import datetime
from multiprocessing import Pool as ThreadPool
from filemanager import BucketManager
from workconfig import WorkConfig
from wcs.services.uploadprogressrecorder import UploadProgressRecorder
from wcs.commons.util import etag
from deluge.log import LOG as log
from workconfig import get_auth
from wcssliceupload import WcsSliceUpload



class TorrentProcesser(object):
    def __init__(self, max_process):
        #self.torrent_list = []
        self.max_process = max_process
        self.pool = ThreadPool(max_process)
        self.processing_file = {}
        self.disable = False
 #   def add_torrent

 #   def start_process(self):
 #       process = self.max_process
 #       l_list = len(self.torrent_list)
 #       if l_list < process:
 #           process = l_list

        #pool = ThreadPool(process)
        #log.info("New process %d",process)
        #pool.map(self.process_single_torrent, self.torrent_list)
        #pool.close()
        #pool.join()

    def try_terminate(self):
        log.info("closing.......processor")
        if self.disable:
            return
        try:
            #self.pool.close()
            self.pool.terminate()
            self.disable = True
        except Exception as identifier:
            log.warn("stop download plugin error")
            pass
    def process_single_torrent(self, torrent_info):
 #           torrent_key = key
        is_finished = torrent_info["is_finished"]
        torrent_hash = torrent_info["hash"]
        dest_path = torrent_info["save_path"]
        if torrent_info["move_completed"]:
            dest_path = torrent_info["move_completed_path"]
        progress = torrent_info["progress"]

        for index, file_detail in enumerate(torrent_info["files"]):
            file_progress = torrent_info["file_progress"][index]
            file_download = torrent_info["file_priorities"][index]
            if file_download:
                if file_progress == 1:
                    file_path = (u'/'.join([dest_path, file_detail["path"]])).encode('utf8')
                    if os.path.exists(file_path):
                        a_size = os.path.getsize(file_path)
                        if a_size == file_detail["size"]:
                            self.prepare_upload(file_path)
                        else:
                            log.warn("file %s size not equal %ld (need %ld)...", file_path, a_size, file_detail["size"])
                    else:
                        log.warn("file %s download complete, but cannot be found...", file_path)

    def post_file(self, file_path, file_key):
        auth = get_auth()
        putpolicy = {'scope':'other-storage:' + file_key,'deadline':str(int(time.time()) * 1000 + 86400000),'overwrite':1}
        token = auth.uploadtoken(putpolicy)
        param = {'position':'local', 'message':'upload'}
        upload_progress_recorder = UploadProgressRecorder()
        modify_time = time.time()
        sliceupload = WcsSliceUpload(token, file_path, file_key, param, upload_progress_recorder, modify_time, WorkConfig.PUT_URL)
        code, hashvalue = sliceupload.slice_upload()
        log.info("upload %d, %s",code,json.dumps(hashvalue))

    def prepare_upload(self,file_path):
        if file_path in self.processing_file:
            return
        if(len(self.processing_file) >= self.max_process):
            return
        log.info("add in pool %d - %d", len(self.processing_file), self.max_process)
        self.processing_file[file_path] = {}
        log.info("proc file %s", file_path)
        pp = self.pool.apply_async(self.upload_to_ws, (file_path),self.update_processing)
        log.info(pp.get())
        if(len(self.processing_file) == self.max_process):
            log.info("closing pool %d", len(self.processing_file))
            self.pool.close()
            self.pool.join()
    
    def update_processing(self,file_path):
        log.info("CALLBACK %s", file_path)
        
    def upload_to_ws(self,file_path):
        
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
            if isinstance(text,dict):
                result = text
            else:
                result = json.loads(text)
            remote_hash = result['hash']
            #remote_size = long(result['fsize'])
            if remote_hash != file_hash:
                log.warn("file: %s hash mismatch: local: %s, remote: %s", file_key, file_hash, remote_hash)
                #repost
                self.post_file(file_path, file_key)
            else:
                log.info("%s exists on server, ignore...", file_path)
            #TODO: check and report..
        else:
            if code == 404:
                self.post_file(file_path, file_key)
            else:
                log.warn("file get message %d, %s, we have to repost file." , code, text)
                self.post_file(file_path, file_key)
        return file_path