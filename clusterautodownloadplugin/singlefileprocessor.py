
from multiprocessing import Process
from multiprocessing.queues import Empty
import traceback
import logging
import time
import os
import json
import base64
import magic
from util import Util
from masterapi import MasterApi
from globalconfig import PGlobalConfig
from wcsbucketmanager import WcsBucketManager
from wcssliceuploader import WcsSliceUploader
from videoconvert import VideoConvert
class SingleFileProcesser(Process):
    """Process Single File"""
    def __init__(self, process_id, in_queue, out_queue):
        self.process_id = process_id
        self.in_queue = in_queue
        self.out_queue = out_queue
        #self.command_queue = command_queue
        super(SingleFileProcesser, self).__init__()
        self.confinue = True
        logging.info("Child process %d created", self.process_id)
        self.master = MasterApi(PGlobalConfig.master_api_server_prefix)
        #self.file_manager = WcsBucketManager()

    def run(self):
        """Main process"""
        while self.confinue:
            self._fetch_and_process()
    def stop_process(self):
        """STOP_PROCESS"""
        self.confinue = False
    def _fetch_and_process(self):
        try:
            data = self.in_queue.get(True, 2)
            if data != None:
                self._process_single_file(data)
            else:
                logging.warning("Get None file to process...")

        except Empty:
            time.sleep(2)

    def _process_single_file(self, data):
        try:
            #self._process_finished()
            result = self._do_work(data)
            self._process_finished(data, result)
        except Exception as exc:
            logging.error("Exception occored in torrent process. %s -- \r\n%s",\
            exc, traceback.format_exc())
            self._process_finished(data, False)

    def _process_finished(self, data, success):
        data["upload_success"] = success
        self.out_queue.put(data)

    def _do_work(self, dat):
        #torrent_id = dat["torrent_id"]
        file_path = dat["file_path"]
        file_size = dat["file_size"]
        if not os.path.exists(file_path):
            logging.warning("file %s not exist, return.", file_path)
            return False
        a_file_size = os.path.getsize(file_path)
        if os.path.getsize(file_path) != file_size:
            logging.warning("file %s size not match (%ld/%ld), return."\
            , file_path, file_size, a_file_size)
            return False
        #Calc file_hash
        file_hash = Util.wcs_etag(file_path)
        file_id = Util.md5(file_hash)
        dat["file_hash"] = file_hash
        dat["file_id"] = file_id
        dat["status"] = 0
        dat["step"] = u"PREPARE_CHECK"
        dat["uploaded"] = False
        #Get Mime
        file_mime = u"application/octet-stream"
        try:
            file_mime = magic.from_file(file_path, mime=True)
        except Exception:
            file_mime = u"application/octet-stream"
        #logging.info("Calc file fid %s etag:%s,[%s] %s", file_id, file_hash, file_mime, file_path)
        dat["file_mime"] = file_mime
        dat = self._check_and_upload(dat)
        upload_success = dat["uploaded"]
        if not upload_success:
            logging.info("Create file info failed!!! step:%s", dat["step"])
            return False
        else:
            #determing avinfo...
            current_status = dat["status"]
            if current_status < 1:
                return self._check_and_try_convert(dat)
            else:
                logging.info("file %s convert status %d", file_id, current_status)
            return True
    def _check_and_try_convert(self, dat):
        #
        if not "ext" in dat:
            logging.warning("No ext in file %s", json.dumps(dat))
            return False
        if not "avinfo" in dat["ext"]:
            logging.warning("No avinfo in file %s", json.dumps(dat))
            return False
        avinfo = dat["ext"]["avinfo"]
        create_video_preview = False
        file_id = dat["file_id"]
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
            logging.info("File %s video %s need create preview %d x %d",\
             file_id, dat["file_name"], width, height)
            file_key = dat["file_key"]
            v_conv = VideoConvert(file_id,\
             PGlobalConfig.wcs_source_file_bucket, file_key, width, height, \
            PGlobalConfig.wcs_video_dest_bucket, duration)
            exec_result = v_conv.do_convert_action()
            logging.info("PID[%d] exec convert action %s %s",\
             self.process_id, file_id, json.dumps(exec_result))
            return self._update_convert_status(file_id, 2)
        else:
            return self._update_convert_status(file_id, 1)
    def _update_convert_status(self,file_id , status):
        post_data = {"status":status}
        is_succ = self.master.update_file_info(file_id, post_data)
        if not is_succ:
            logging.warning("file %s update_convert_status error.", file_id)
        return is_succ
    def _check_and_upload(self, dat):
        bucket = PGlobalConfig.wcs_source_file_bucket
        file_hash = dat["file_hash"]
        file_key = u'raw/' + file_hash
        dat["file_key"] = file_key
        file_id = dat["file_id"]
        remote_info = self.master.get_file_info(file_id)
        if len(remote_info) > 0:
            #fileUploaded
            dat["uploaded"] = True
            dat["status"] = remote_info["status"]
            dat["step"] = "ALREADY_EXISTS_ON_LX_SERVER"
            if remote_info["etag"] != file_hash:
                logging.warning("File %s, etag mismatch. local %s, rmote %s - %s"\
                , file_id, file_hash, remote_info["etag"], json.dumps(remote_info))
            if remote_info["ext"]:
                dat["ext"] = json.loads(remote_info["ext"])
            return dat
        #logging.info("Checking file %s on WCS", file_id)
        file_manager = WcsBucketManager(Util.default_wcs_auth(), PGlobalConfig.wcs_mgr_url)
        code, result = file_manager.stat(bucket, file_key)
        avinfo = {}
        if code == 200:
            # Alread upload, get avinfo
            info = Util.get_wcs_avinfo(PGlobalConfig.wcs_avinfo_prefix, file_key)
            if info:
                avinfo = info
            #logging.info("file %s exists, get avinfo %s", file_id, json.dumps(info))
        else:
            if code == 404:
                dat["step"] = "NEW_UPLOAD"
            else:
                dat["step"] = "RE_UPLOAD"
            succ = self._do_wcs_upload(dat, bucket, file_key)
            if not succ:
                dat["uploaded"] = False
                return dat
            else:
                avinfo = succ
        #process avinfo into ext, and post to server.
        if not "ext" in dat:
            dat["ext"] = {}
        dat["ext"]["avinfo"] = avinfo
        create_file_success = self._create_file_info(dat)
        dat["uploaded"] = create_file_success
        return dat
    
    def _create_file_info(self, dat):
        file_path = dat["file_path"]
        file_name = os.path.basename(file_path)
        torrent_id = dat["torrent_id"]
        file_size = dat["file_size"]
        fid = dat["file_id"]
        torrent_path = dat["torrent_path"]
        torrent_path_array = torrent_path.split('/')
        file_key = dat["file_key"]
        file_hash = dat["file_hash"]
        updated_time = time.time() * 1000
        file_ext = json.dumps(dat["ext"])
        file_mime = dat["file_mime"]

        file_data = {"size":file_size, "fid":fid, \
         "name":file_name, "key":file_key, "hash":file_hash\
         , "etag":file_hash, "updatedtime":updated_time, "ext":file_ext, "path":torrent_path_array}
        dat["step"] = "POST_TO_MASTER_SERVER"
        post_data = {"tid":torrent_id, "size":file_size, "mime":file_mime, \
            "name":file_name, "fid":fid, "etag":file_hash, "key":file_key,\
            "ext":file_ext, "file":file_data, "updatedtime":updated_time, \
            "path":torrent_path_array}
        return self.master.create_file_info(post_data)

    def _do_wcs_upload(self, dat, bucket, file_key):
        auth = Util.default_wcs_auth()
        putpolicy = {'scope':'other-storage:' + file_key\
            , 'deadline':str(int(time.time()) * 1000 + 86400000), \
            'overwrite':1, 'returnBody':\
            'url=$(url)&fsize=$(fsize)&bucket=$(bucket)&key=$(key)&hash=$(hash)&fsize=$(fsize)&mimeType=$(mimeType)&avinfo=$(avinfo)'}
        token = auth.uploadtoken(putpolicy)
        file_path = dat["file_path"]
        put_url = PGlobalConfig.wcs_put_url
        #block_size = 1024 * 1024 * 4
        #put_size = 512 * 1024
        file_size = dat["file_size"]
        orign_etag = dat["file_hash"]
        file_id = dat["file_id"]
        upload = WcsSliceUploader(token, file_path, put_url)
        start_time = time.time()
        code, body = upload.start_upload()
        time_cost = time.time() - start_time
        speed = file_size / time_cost
        logging.info("file %s:%s filesize uploaded %s/sec", file_id\
         , Util.sizeof_fmt(file_size), Util.sizeof_fmt(speed))
        #logging.info("code %d, body %s etag:%s", code, json.dumps(body), orign_etag)
        if code != 200:
            logging.warning("upload file: %s fail", json.dumps(dat))
            dat["step"] = "POST_TO_WCS_FAIL"
            return False
        etag = body["hash"]
        if etag != orign_etag:
            logging.warning("File %s etag not match.", file_id)
        dat["step"] = "POST_TO_WCS_SUCCESS"
        return json.loads(base64.urlsafe_b64decode(str(body["avinfo"])))















