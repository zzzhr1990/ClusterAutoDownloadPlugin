import logging
import multiprocessing
import threading
import six
import os
import time
import json
import traceback
import deluge.component as component
from singlefileprocessor import SingleFileProcesser
from multiprocessing.queues import Empty
from torrentevents import TorrentBatchFileUploadCompletedEvent
from util import Util

LT_TORRENT_STATE_MAP = {
    'queued_for_checking': 'Checking',
    'checking_files': 'Checking',
    'downloading_metadata': 'Downloading',
    'downloading': 'Downloading',
    'finished': 'Seeding',
    'seeding': 'Seeding',
    'allocating': 'Allocating',
    'checking_resume_data': 'Checking',
    'error': 'Error'
}


class TorrentProcessor(object):
    """Processing Torrents"""

    def __init__(self, max_child_process, server_name, core, sid):
        self.max_child_process = max_child_process
        self.server_name = server_name
        self.file_processors = []
        self.working_dict = {}
        self.in_queue = multiprocessing.Queue()
        self.out_queue = multiprocessing.Queue()
        self.disable = False
        self.core = core
        self.sid = sid
        #self.task_looping_thread = threading.Thread(target=self._time_tick)
        #self.task_looping_thread.daemon = True
        #self.out_queue = multiprocessing.Queue()
        for i in range(0, max_child_process):
            processor = SingleFileProcesser(i, self.in_queue, self.out_queue)
            processor.daemon = True
            processor.start()
            self.file_processors.append(processor)
        logging.info(
            "TorrentProcesser [%d] ,[%s] inited.", max_child_process, server_name)

    # def start(self):
    #    """Start processor"""
    #    self.task_looping_thread.start()
    def disable_process(self):
        """closing"""
        self.disable = True
        self.in_queue.close()
        self.out_queue.close()

    def _sleep_and_wait(self, stime):
        if not self.disable:
            if stime < 1:
                stime = 1
                for ignore in range(0, stime):
                    if not self.disable:
                        time.sleep(1)
    # def _time_tick(self):
    #    while not self.disable:
    #        self._sleep_and_wait(5)
    #        try:
    #            self.update_torrent_info(self.core.get_torrents_status({}, {}))
    #        except Exception as e:
    #            logging.error(e)
    #            logging.error("Exception occored in _time_tick.\r\n%s", traceback.format_exc())

    def update_torrent_info(self):
        """Used for update torrent info."""
        # logging.info("Checking torrent info....")
        # torrents_info = core.get_torrents_status({}, {})
        # Checking if finished.
        """
        downloaded_dict = {}
        """
        downloaded = []
        while not self.out_queue.empty():
            try:
                dat = self.out_queue.get(False)
                if dat != None:
                    """
                    torrent_id = dat["torrent_id"]
                    if torrent_id in self.working_dict:
                        work_dict = self.working_dict[torrent_id]
                        file_path = dat["file_path"]
                        #file_index = dat["file_index"]
                        if file_path in work_dict:
                            work_dict.pop(file_path)
                            if not torrent_id in downloaded_dict:
                                downloaded_dict[torrent_id] = []
                            downloaded_dict[torrent_id].append(dat)
                        else:
                            logging.warning(
                                "File Process Finished but connot found in working... %s of %d",
                                dat["torrent_id"], dat["file_index"])
                    else:
                        logging.warning("File Process Finished but connot found... %s of %d",
                                        dat["torrent_id"], dat["file_index"])
                    """
                    # When file downloaded finished...
                    torrent_id = dat["torrent_id"]
                    # Index of torrent id.
                    file_index = dat["file_index"]
                    if dat["success"]:
                        downloaded.append(dat)
                    else:
                        logging.info("%s, %d failed at step %s.",
                                     torrent_id, file_index, dat["step"])
                        dat["try_time"] = dat["try_time"] + 1
                        if dat["try_time"] > 5:
                            logging.error("%s:%d failed upload.",
                                          torrent_id, file_index)
                        else:
                            self.add_torrent_file(dat)
                    # dispatch events to mqservice to announce
            except Empty:
                pass
            # Check and report file to file server
            # Check and report status to main server. (USE MQ)
            if downloaded:
                component.get('EventManager').emit(
                    TorrentBatchFileUploadCompletedEvent(downloaded))
            torrents_info = self.core.get_torrents_status({}, {})
            # we'd
            #report_dict = {'downloaded': downloaded}
            # here we report current status to server.
        """
        if downloaded_dict:
            # Refresh files
            for torrent_id in downloaded_dict:
                md5_tid = Util.md5(torrent_id)
                if torrent_id in torrents_info:
                    file_prop = torrents_info[torrent_id]["file_priorities"]
                    progress = torrents_info[torrent_id]["progress"]
                    remote_file_prop = self.core.get_torrent_status(md5_tid)
                    if remote_file_prop:
                        if remote_file_prop["updatedtime"] > 0:
                            if "info" in remote_file_prop:
                                if remote_file_prop["info"]:
                                    file_prop = json.loads(
                                        remote_file_prop["info"])
                                else:
                                    logging.warning(
                                        "Remote status null %s", md5_tid)
                            else:
                                logging.warning(
                                    "Remote status missing %s", md5_tid)
                        else:
                            logging.info(
                                "Remote status create new for %s", md5_tid)
                    else:
                        logging.warning(
                            "Remote status cannot found for %s", md5_tid)
                    for downloaded in downloaded_dict[torrent_id]:
                        d_succ = downloaded["upload_success"]
                        if d_succ:
                            logging.info(
                                "%s [%s]Download/convert finish.", downloaded["file_id"], downloaded["file_path"])
                            file_index = downloaded["file_index"]
                            file_prop[file_index] = 0
                    count = 0

                    for i_count in file_prop:
                        count = count + i_count
                    if count < 1:
                        logging.info(
                            "Torrent %s downloaded finished...", torrent_id)
                        # TO/DO:REMOVE Torrent
                        self.working_dict.pop(torrent_id)
                        core.remove_torrent(torrent_id, True)
                        self.core.change_torrent_status(
                            md5_tid, {"status": 10})
                    else:
                        # TO/DO:CHANGE TORRENT_STATUS
                        core.set_torrent_file_priorities(torrent_id, file_prop)
                        self.core.refresh_torrent_progress(
                            md5_tid, progress, json.dumps(file_prop))
                else:
                    logging.warning(
                        "%s cannot be found in torrent list", torrent_id)
            # TO/DO:REFRESH TORRENT
        # ensure remove all success files.
        for torrent_id in torrents_info:
            work_list = self._process_single_torrent(
                torrent_id, torrents_info[torrent_id])
            if work_list:
                if not torrent_id in self.working_dict:
                    self.working_dict[torrent_id] = {}
                current_working = self.working_dict[torrent_id]
                for file_wait in work_list:
                    file_path = file_wait["file_path"]
                    if not file_path in current_working:
                        current_working[file_path] = file_wait
                        self.in_queue.put(file_wait, False)
#                        logging.info("%d in %s adding to processor.....", file_wait["file_index"], file_wait["torrent_id"])
#                   else:
#                        logging.info("%d in %s already exists", file_wait["file_index"], file_wait["torrent_id"])
        """

    def add_torrent_file(self, file_prop):
        """Add new file"""
        """
        {'torrent_id': torrent_id,
                         'file_path': file_path, 'file_size': file_data['size'],
                         'file_index': index, 'success': False}
        """
        try:
            self.in_queue.put(file_prop, False)
        except Exception as ex:
            logging.error("Adding upload task fail, %s", ex)

    def _process_single_torrent(self, torrent_id, torrent_info):
        #logging.info("Processing %s", torrent_id)
        wait_to_add = []
        # Findout if it need to upload.
        #is_finished = torrent_info["is_finished"]
        md5_tid = Util.md5(torrent_id)
        dest_path = torrent_info["save_path"]
        if torrent_info["move_completed"]:
            dest_path = torrent_info["move_completed_path"]
        remote_file_prop = self.core.get_torrent_status(md5_tid)
        file_prop = []
        if remote_file_prop:
            if remote_file_prop["updatedtime"] > 0:
                if "info" in remote_file_prop:
                    if remote_file_prop["info"]:
                        file_prop = json.loads(remote_file_prop["info"])
                    else:
                        logging.warning("Remote status null %s", md5_tid)
                else:
                    logging.warning("Remote status missing %s", md5_tid)
        else:
            logging.warning("Remote status cannot found for %s, %s, %s",
                            md5_tid, torrent_id, torrent_info["hash"])
        for index, file_detail in enumerate(torrent_info["files"]):
            file_progress = torrent_info["file_progress"][index]
            file_download = torrent_info["file_priorities"][index]
            if file_prop:
                if file_prop[index] == 0:
                    continue
            if file_download:
                if file_progress == 1:
                    file_path = u'/'.join([dest_path, file_detail["path"]])
                    #logging.info("Need to check and upload file:%s, %s", file_path, torrent_id)
                    # Assuming processing file OK
                    task = {"file_index": index, "torrent_id": torrent_id,
                            "file_path": file_path, "torrent_path": file_detail["path"], "file_size": file_detail["size"]}
                    wait_to_add.append(task)
        return wait_to_add
