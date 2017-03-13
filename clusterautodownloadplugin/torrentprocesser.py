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
        log.info("TorrentProcesser inited")

    def process_single_torrent(self, torrent_info):
        pass

    def try_terminate(self):
        log.info("closing.......processor")