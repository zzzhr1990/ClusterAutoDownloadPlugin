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
import os.path
import hashlib




class TorrentProcesser(Process):
    """Process Torrent"""
    def __init__(self, process_id, in_queue, out_queue, command_queue):
        self.process_id = process_id
        self.in_queue = in_queue
        self.out_queue = out_queue
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
            if data != None:
                start = time.time()
                self.process_single_torrent(data)

        except Empty:
            time.sleep(2)

    def run(self):
        """Main process"""
        self.looping_thread.start()
        while not self.terminated:
            try:
                self._fetch_and_process()
            except Exception as e:
                log.error("Exception occored in torrent process. %s -- \r\n%s",\
                e, traceback.format_exc())
        #finally:
        #    self.terminated()

    def process_single_torrent(self, torrent_info):
        """Add one torrent to process"""
        is_finished = torrent_info["is_finished"]
        torrent_hash = torrent_info["hash"]
        dest_path = torrent_info["save_path"]
        if torrent_info["move_completed"]:
            dest_path = torrent_info["move_completed_path"]
        #progress = torrent_info["progress"]
        log.info(json.dumps(torrent_info))
        all_success_download = True
        succeed_files = []
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
                            file_prop = {"torrent_hash":torrent_hash, "path":file_detail["path"]}
                            single_success_download = self._upload_to_ws(file_path, file_prop)
                            if not single_success_download:
                                all_success_download = False
                            else:
                                succeed_files.append(file_detail)
                        else:
                            all_success_download = False
                            log.warn("file %s size not equal %ld (need %ld)..."\
                            , file_path, a_size, file_detail["size"])
                    else:
                        all_success_download = False
                        log.warn("PID[%d] file %s download complete, but cannot be found...", self.process_id, file_path)
                else:
                    all_success_download = False
        if all_success_download and is_finished:
            tid = self._md5(torrent_hash)
            self.task.change_torrent_status(tid, {"status":10})
            self.out_queue.put\
            ({"hash" : torrent_hash, "finished" : True, "files" : succeed_files}, False)
        else:
            self.out_queue.put\
            ({"hash" : torrent_hash, "finished" : False, "files" : succeed_files}, False)


    def _post_file(self, file_path, file_key, file_prop):
        auth = get_auth()
        putpolicy = {'scope':'other-storage:' + file_key\
            , 'deadline':str(int(time.time()) * 1000 + 86400000), \
            'overwrite':1, 'returnBody':\
            'url=$(url)&fsize=$(fsize)&bucket=$(bucket)&key=$(key)&hash=$(hash)&fsize=$(fsize)&mimeType=$(mimeType)&avinfo=$(avinfo)'}
        token = auth.uploadtoken(putpolicy)
        param = {'position':'local', 'message':'upload'}
        upload_progress_recorder = UploadProgressRecorder()
        modify_time = time.time()
        sliceupload = WcsSliceUpload(self.process_id, token, file_path, file_key, param\
            , upload_progress_recorder, modify_time, WorkConfig.PUT_URL)
        #self.current_upload = sliceupload
        if self.terminated:
            return 0, None
        code, hashvalue = sliceupload.slice_upload()
        if code == 200:
            #Preview
            info = hashvalue["avinfo"]
            log.info("AVIF %s", info)
            create_video_preview = False
            height = 0
            width = 0
            duration = 0.0
            if info:
                log.info("BASE64 %s", info)
                info_dict = json.loads(base64.urlsafe_b64decode(info))
                if "streams" in info_dict:
                    log.info("STREAM FOUND")
                    for stream in info_dict["streams"]:
                        create_video_preview = True
                        if "codec_name" in stream:
                            if stream["codec_name"] == "gif":
                                create_video_preview = False
                                break
                        if "width" in stream:
                            width = stream["width"]
                        if "height" in stream:
                            height = stream["height"]
                        if "duration" in stream:
                            if duration < stream["duration"]:
                                duration = stream["duration"]

            fid = self._md5(hashvalue["hash"])
            if create_video_preview and duration > 10:
                log.info("PreCreate Convert...")
                log.info("Video need create preview %d x %d", width, height)
                dest_key = "sp/m3u8/" + time.strftime("%Y%m%d",time.localtime())
                ops_prefix = "avthumb/m3u8/segtime/5/vcodec/libx264/acodec/libfaac|saveas/"


            file_name = os.path.basename(file_path)
            file_data = {"size":hashvalue["fsize"], "name":file_name, "key":hashvalue["key"]}
            post_data = {"tid":self._md5(file_prop["torrent_hash"]),\
             "name":file_name, "fid":fid, "file":file_data,\
            "path":file_prop["path"].split('/')}
            self.task.upload_file_info(post_data)
        log.info("upload code %d, %s", code, json.dumps(hashvalue))

    def _md5(self, str_s):
        m_uu = hashlib.md5()
        m_uu.update(str_s)
        return m_uu.hexdigest()
    def _upload_to_ws(self, file_path, file_prop):
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
                self._post_file(file_path, file_key, file_prop)
 #           else:
 #               log.info("%s exists on server, ignore...", file_path)
            #TODO: check and report..
        else:
            if code == 404:
                self._post_file(file_path, file_key, file_prop)
            else:
                log.warn("file get message %d, %s, we have to repost file.", code, text)
                self._post_file(file_path, file_key, file_prop)
        return file_path


