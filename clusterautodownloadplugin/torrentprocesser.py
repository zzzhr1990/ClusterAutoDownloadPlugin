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
from videoconvert import VideoConvert
import os.path
import hashlib
import magic




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
                #log.info("PID[%d] Starting processing torrent %s", self.process_id, data["hash"])
                start = time.time()
                self.process_single_torrent(data)
                #log.info("PID[%d] Starting processing torrent %s in %d ms", self.process_id, data["hash"], \
                #(time.time() - start) * 1000)

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
        #log.info(json.dumps(torrent_info))
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
                #fid = self._md5(file_detail["path"] + "@" + torrent_hash)
                if file_progress == 1:
                    file_path = (u'/'.join([dest_path, file_detail["path"]])).encode('utf8')
                    if os.path.exists(file_path):
                        a_size = os.path.getsize(file_path)
                        if a_size == file_detail["size"]:
                            tid = self._md5(torrent_hash)
                            file_prop = {"tid":tid, "torrent_hash":torrent_hash, \
                            "path":file_detail["path"], "size": file_detail["size"]}
                            upload_result = self._upload_to_ws(file_path, file_prop)
                            if not upload_result["uploaded"]:
                                all_success_download = False
                            else:
                                if upload_result["status"] == 0:
                                    log.info("Checking if need convert...%s", file_path)
                                    if "ext" in upload_result:
                                        if "avinfo" in upload_result["ext"]:
                                            avinfo = upload_result["ext"]["avinfo"]
                                            self._parse_and_convert(avinfo, upload_result, file_prop)
                                        else:
                                            log.info("AVINFO_MISSING")
                                            log.info("%s", json.dumps(upload_result))
                                    else:
                                        log.info("EXT MISSING")
                                        log.info("%s", json.dumps(upload_result))
                                succeed_files.append(file_detail)
                        else:
                            all_success_download = False
                            log.warn("file %s size not equal %ld (need %ld)..."\
                            , file_path, a_size, file_detail["size"])
                    else:
                        all_success_download = False
                        log.warn("PID[%d] file %s download complete, but cannot be found..."\
                        , self.process_id, file_path)
                else:
                    all_success_download = False
        if all_success_download and is_finished:
            tid = self._md5(torrent_hash)
            self.task.change_torrent_status(tid, {"status":10})
            self.out_queue.put\
            ({"hash" : torrent_hash, "finished" : True, "files" : succeed_files}, False)
            log.info("%s(tid=%s) download finished.", torrent_hash, tid)
        else:
            self.out_queue.put\
            ({"hash" : torrent_hash, "finished" : False, "files" : succeed_files}, False)


    def _post_file(self, h_result , file_path, file_key):
        h_result["uploaded"] = False
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
            return h_result
        code, hashvalue = sliceupload.slice_upload()
        if code == 200:
            h_result["uploaded"] = True
            if "avinfo" in hashvalue:
                h_result["ext"] = \
                {"avinfo":json.loads(base64.urlsafe_b64decode(hashvalue["avinfo"]))}
            else:
                log.info("Missing avinfo...")
            h_result["key"] = hashvalue["key"]
            h_result["size"] = hashvalue["fsize"]
            h_result["etag"] = hashvalue["hash"]
            h_result["mime"] = hashvalue["mimeType"]
            return h_result
        else:
            return h_result
            #Preview
            """
            info = hashvalue["avinfo"]
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
                            if stream["codec_name"] == "text":
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
            if create_video_preview and duration > 10 and height > 0 and width > 0:
                log.info("PreCreate Convert...")
                log.info("Video need create preview %d x %d", width, height)
                video_conv = VideoConvert(fid,"other-storage", \
                file_key, width, height, "qietv-video-play", duration)
                video_conv.do_convert_action()
            file_name = os.path.basename(file_path)
            file_data = {"size":hashvalue["fsize"], "name":file_name, "key":hashvalue["key"]}
            post_data = {"tid":self._md5(file_prop["torrent_hash"]),\
             "name":file_name, "fid":fid, "file":file_data,\
              "path":file_prop["path"].split('/')}
            self.task.upload_file_info(post_data)
            """
        #log.info("upload code %d, %s", code, json.dumps(hashvalue))

    def _md5(self, str_s):
        m_uu = hashlib.md5()
        m_uu.update(str_s)
        return m_uu.hexdigest()

    def _upload_to_ws(self, file_path, file_prop):
        #begin = time.time()
        file_mime = "application/octet-stream"
        try:
            file_mime = magic.from_file(file_path)
        except Exception as s:
            file_mime = "application/octet-stream"
        #file_key = etag(file_path)
        #log.info("Process %ld in %f s", file_size, time.time() - begin)
        
        bucket = "other-storage"
        file_hash = etag(file_path)
        #Check if fid exists...
        fid = self._md5(file_hash)
        file_prop["fid"] = fid
        file_key = "raw/" + file_hash
        h_result = {"fid":fid, "file_path":file_path, "key":file_key, "status":0}
        remote_info = self.task.get_file_info(fid)
        if len(remote_info) > 0:
            file_uploaded = True
            h_result["uploaded"] = True
            if remote_info["ext"]:
                h_result["ext"] = json.loads(remote_info["ext"])
            h_result["size"] = remote_info["size"]
            h_result["etag"] = remote_info["etag"]
            h_result["mime"] = remote_info["mime"]
            h_result["status"] = remote_info["status"]
            h_result["step"] = "ALREADY_EXISTS_ON_LX_SERVER"
            return h_result
            #
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
                h_result["step"] = "ALREADY_EXISTS_BUT_HASH_MISMATCH_REPOST"
                return self._create_file_info(\
                self._post_file(h_result, file_path, file_key), file_prop)
            else:
                log.info("%s exists on server, ignore...", file_path)
                #Fetch avinfo...
                avinfo = self.task.get_wcs_avinfo(file_key)
                save_stream = {}
                if "streams" in avinfo:
                    save_stream = avinfo
                h_result["ext"] = {"avinfo" : save_stream}
                h_result["uploaded"] = True
                h_result["size"] = result["fsize"]
                h_result["etag"] = result["hash"]
                h_result["mime"] = result["mimeType"]
                h_result["step"] = "ALREADY_EXISTS_WCS_SERVER_IGNORE_UPDATE"
                return self._create_file_info(h_result, file_prop)
            #TODO: check and report..
        else:
            if code == 404:
                h_result["step"] = "NEW_UPLOAD"
                return self._create_file_info(\
                self._post_file(h_result, file_path, file_key), file_prop)
            else:
                log.warn("file get message %d, %s, we have to repost file.", code, text)
                h_result["step"] = "OTHER_CODE_NEW_UPLOAD"
                return self._create_file_info(\
                self._post_file(h_result, file_path, file_key), file_prop)

    def _create_file_info(self, h_result, file_prop):
        file_name = os.path.basename(file_prop["path"])
        file_data = {"size":h_result["size"], "name":file_name, "key":h_result["key"]}
        post_data = {"tid":file_prop["tid"],\
            "name":file_name, "fid":h_result["fid"], \
            "ext":json.dumps(h_result["ext"]), "file":file_data,\
            "path":file_prop["path"].split('/')}
        self.task.create_file_info(post_data)
        log.info("Post to LX Server to create file...[%s], [%s]", json.dumps(h_result), json.dumps(post_data))
        return h_result

    def _update_convert_status(self, h_result, file_prop, status):
        #file_name = os.path.basename(file_prop["path"])
        #file_data = {"size":h_result["size"], "name":file_name, "key":h_result["key"]}
        post_data = {"status":status}
        self.task.update_file_info(h_result["fid"], post_data)

    def _parse_and_convert(self, avinfo, h_result, file_prop):
        create_video_preview = False
        height = 0
        width = 0
        duration = 0.0
        if "streams" in avinfo:
            for stream in avinfo["streams"]:
                create_video_preview = True
                if "codec_name" in stream:
                    if stream["codec_name"] == "gif":
                        create_video_preview = False
                        break
                    if stream["codec_name"] == "text":
                        create_video_preview = False
                        break
                    if "width" in stream:
                        width = stream["width"]
                    if "height" in stream:
                        height = stream["height"]
                    if "duration" in stream:
                        if duration < stream["duration"]:
                            duration = stream["duration"]

        if width < 1:
            create_video_preview = False
        if height < 1:
            create_video_preview = False
        if create_video_preview:
            log.info("Video need create preview %d x %d", width, height)
            v_conv = VideoConvert(h_result["fid"], "other-storage", \
            h_result["key"], width, height, "qietv-video-play", duration)
            exec_result = v_conv.do_convert_action()
            log.info("PID[%d] exec convert action %s %s", self.process_id, h_result["fid"], json.dumps(exec_result))
            self._update_convert_status(h_result, file_prop, 2)
        else:
            self._update_convert_status(h_result, file_prop, 1)






