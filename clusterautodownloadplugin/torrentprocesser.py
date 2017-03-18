import json
import time
import requests
import os
import base64
import traceback
import datetime
import threading
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
    def __init__(self, process_id, in_queue, out_queue, command_queue):
        self.process_id = process_id
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.command_queue = command_queue
        self.looping_thread = threading.Thread(target=self._loop)
        self.looping_thread.daemon = True
        self.terminated = False
        super(TorrentProcesser, self).__init__()

    def _loop(self):
        while True:
            if not self.in_queue.empty():
                log.info("Torrent process %d terminated.", self.process_id)
                self.terminated = True
                self.terminate()
                return
            self._sleep_and_wait(2)

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
            log.info("%d processing torrents", 20)
        except Exception as e:
            log.error("EE_______GET_________. %s -- \r\n%s",\
            e, traceback.format_exc())

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


