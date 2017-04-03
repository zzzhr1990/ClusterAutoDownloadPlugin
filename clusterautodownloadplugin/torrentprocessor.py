import logging
import multiprocessing
import threading
import six, os, time
import traceback
from singlefileprocessor import SingleFileProcesser
from multiprocessing.queues import Empty
from util import Util

class TorrentProcessor(object):
    """Processing Torrents"""
    def __init__(self, max_child_process, server_name, core, master):
        self.max_child_process = max_child_process
        self.server_name = server_name
        self.file_processors = []
        self.working_dict = {}
        self.in_queue = multiprocessing.Queue()
        self.out_queue = multiprocessing.Queue()
        self.core = core
        self.disable = False
        self.master = master
        #self.task_looping_thread = threading.Thread(target=self._time_tick)
        #self.task_looping_thread.daemon = True
        #self.out_queue = multiprocessing.Queue()
        for i in range(0, max_child_process):
            processor = SingleFileProcesser(i, self.in_queue, self.out_queue)
            processor.daemon = True
            processor.start()
            self.file_processors.append(processor)
        logging.info("TorrentProcesser [%d] ,[%s] inited."\
        , max_child_process, server_name)

    #def start(self):
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
                for i in range(0, stime):
                    if not self.disable:
                        time.sleep(1)
    #def _time_tick(self):
    #    while not self.disable:
    #        self._sleep_and_wait(5)
    #        try:
    #            self.update_torrent_info(self.core.get_torrents_status({}, {}))
    #        except Exception as e:
    #            logging.error(e)
    #            logging.error("Exception occored in _time_tick.\r\n%s", traceback.format_exc())
    def update_torrent_info(self, torrents_info):
        """Used for update torrent info."""
        #Checking if finished.
        downloaded_dict = {}
        while not self.out_queue.empty():
            try:
                dat = self.out_queue.get(False)
                if dat != None:
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
                            logging.warning(\
                            "File Process Finished but connot found in working... %s of %d",\
                            dat["torrent_id"], dat["file_index"])
                    else:
                        logging.warning("File Process Finished but connot found... %s of %d",\
                         dat["torrent_id"], dat["file_index"])
            except Empty:
                pass

        if downloaded_dict:
            # Refresh files
            for torrent_id in downloaded_dict:
                if torrent_id in torrents_info:
                    file_prop = torrents_info[torrent_id]["file_priorities"]
                    logging.info("old_file_prop: %s", file_prop)
                    for downloaded in downloaded_dict[torrent_id]:
                        d_succ = downloaded["upload_success"]
                        if d_succ:
                            logging.info("%s [%s]Download/convert finish.", downloaded["file_id"]\
                            , downloaded["file_path"])
                            file_index = downloaded["file_index"]
                            file_prop[file_index] = 0
                    count = 0
                    for i_count in file_prop:
                        count = count + i_count
                    if count < 1:
                        logging.info("Torrent %s downloaded finished...", torrent_id)
                        #TODO:REMOVE Torrent
                        self.working_dict.pop(torrent_id)
                        self.core.remove_torrent(torrent_id, True)
                        tid = Util.md5(torrent_id)
                        self.master.change_torrent_status(tid\
                            , {"status" : 10, "infohash" : torrent_id})
                    else:
                        #TODO:CHANGE TORRENT_STATUS
                        logging.info("new_file_prop: %s", file_prop)
                        self.core.set_torrent_file_priorities(torrent_id, file_prop)
                else:
                    logging.warning("%s cannot be found in torrent list", torrent_id)
            #TODO:REFRESH TORRENT
        # ensure remove all success files.
        for torrent_id in torrents_info:
            work_list = self._process_single_torrent(torrent_id, torrents_info[torrent_id])
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


    def _process_single_torrent(self, torrent_id, torrent_info):
        #logging.info("Processing %s", torrent_id)
        wait_to_add = []
        #Findout if it need to upload.
        #is_finished = torrent_info["is_finished"]
        dest_path = torrent_info["save_path"]
        if torrent_info["move_completed"]:
            dest_path = torrent_info["move_completed_path"]
        for index, file_detail in enumerate(torrent_info["files"]):
            file_progress = torrent_info["file_progress"][index]
            file_download = torrent_info["file_priorities"][index]
            if file_download:
                if file_progress == 1:
                    file_path = u'/'.join([dest_path, file_detail["path"]])
                    #logging.info("Need to check and upload file:%s, %s", file_path, torrent_id)
                    #Assuming processing file OK
                    task = {"file_index":index, "torrent_id":torrent_id,\
                     "file_path":file_path, "torrent_path":file_detail["path"]\
                     , "file_size":file_detail["size"]}
                    wait_to_add.append(task)
        return wait_to_add

