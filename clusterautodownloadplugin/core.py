#
# core.py
#
# Copyright (C) 2009 zzzhr <zzzhr@hotmail.com>
#
# Basic plugin template created by:
# Copyright (C) 2008 Martijn Voncken <mvoncken@gmail.com>
# Copyright (C) 2007-2009 Andrew Resch <andrewresch@gmail.com>
# Copyright (C) 2009 Damien Churchill <damoxc@gmail.com>
#
# Deluge is free software.
#
# You may redistribute it and/or modify it under the terms of the
# GNU General Public License, as published by the Free Software
# Foundation; either version 3 of the License, or (at your option)
# any later version.
#
# deluge is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with deluge.    If not, write to:
# 	The Free Software Foundation, Inc.,
# 	51 Franklin Street, Fifth Floor
# 	Boston, MA  02110-1301, USA.
#
#    In addition, as a special exception, the copyright holders give
#    permission to link the code of portions of this program with the OpenSSL
#    library.
#    You must obey the GNU General Public License in all respects for all of
#    the code used other than OpenSSL. If you modify file(s) with this
#    exception, you may extend this exception to your version of the file(s),
#    but you are not obligated to do so. If you do not wish to do so, delete
#    this exception statement from your version. If you delete this exception
#    statement from all source files in the program, then also delete it here.
#

import json
import time
import requests
import os
import base64
import traceback
import datetime
import threading
import multiprocessing
from deluge.error import InvalidTorrentError
from deluge.log import LOG as log
from deluge.plugins.pluginbase import CorePluginBase
import deluge.component as component
import deluge.configmanager
from deluge.core.rpcserver import export
from workconfig import WorkConfig
from taskprocess import TaskProcess
from torrentprocesser import TorrentProcesser
from multiprocessing.queues import Empty





DEFAULT_PREFS = {
    "test":"NiNiNi"
}

class Core(CorePluginBase):
    '''Init Function'''
    def __init__(self, plugin_name):
        self.plugin_name = plugin_name
        self.processing = False
        self.config = deluge.configmanager\
        .ConfigManager("clusterautodownloadplugin.conf", DEFAULT_PREFS)
        super(Core, self).__init__(plugin_name)
        self.looping_thread = threading.Thread(target=self._loop)
        self.looping_thread.daemon = True
        self.task_looping_thread = threading.Thread(target=self._task_loop)
        self.task_looping_thread.daemon = True
        WorkConfig.disable = True
        self.busy = False
        self.fetching_task = False
        self.processor = TaskProcess(WorkConfig.SERVER_URL)
        self.processing_pool = []
        self.command_queues = []
        self.working_dict = {}
        self.waiting_queue = multiprocessing.Queue()
        self.response_queue = multiprocessing.Queue()
        self.record_lock = threading.Lock()
        log.info("Cluster downloader init, poolsize %d", WorkConfig.MAX_PROCESS)

    def enable(self):
        """Call when plugin enabled."""
        WorkConfig.disable = False
        for i in range(0, WorkConfig.MAX_PROCESS):
            command_queue = multiprocessing.Queue()
            proccess = TorrentProcesser(i, self.waiting_queue, self.response_queue, command_queue)
            self.command_queues.append(command_queue)
            self.processing_pool.append(proccess)
            proccess.daemon = True
            proccess.start()

        self.looping_thread.start()
        self.task_looping_thread.start()
        log.info("- Plugin %s enabled.", self.plugin_name)

    def disable(self):
        """Call when plugin disabled."""
        self.record_lock.acquire()
        WorkConfig.disable = True
        self.waiting_queue.close()
        self.response_queue.close()
        log.warn("Trying to shutdown download plugin")
        #
        for queue in self.command_queues:
            log.info("Sending...")
            queue.put(True, block=False)
            log.info("Send")
        self.record_lock.release()
        time.sleep(5)
        for queue in self.command_queues:
            queue.close()
        log.warn("Trying to shutdown download plugin...success")


    def _task_loop(self):
        while True:
            self.record_lock.acquire()
            if WorkConfig.disable:
                self.record_lock.release()
                log.info("Ternimate task loop...")
                return
            if self.fetching_task:
                log.warn("Slow fetching task.")
                self.record_lock.release()
                return
            self.fetching_task = True
            self.record_lock.release()
            try:
                self.processor.check_tasks()
            except Exception as e:
                log.error("Exception occored in task loop. %s -- \r\n%s", e, traceback.format_exc())
            finally:
                self.fetching_task = False
            self._sleep_and_wait(5)

    def _loop(self):
        while True:
            self.record_lock.acquire()
            if WorkConfig.disable:
                self.record_lock.release()
                log.info("Ternimate check loop...")
                return
            if self.busy:
                log.warn("Slow query found.")
                self.record_lock.release()
                return
            self.busy = True
            try:
                self._checking_tasks()
            except Exception as e:
                log.error("Exception occored in status loop. %s -- \r\n%s", e, traceback.format_exc())
            finally:
                self.busy = False
                self.record_lock.release()
            self._sleep_and_wait(2)

    def _change_file_prop(self, core, data):
        torrent_id = data["hash"]
        stat = core.get_torrent_status(torrent_id,{"file_priorities"})
        if stat == None or len(stat) < 1:
            log.warn("Cannot find file_priorities for torrent %s", torrent_id)
        else:
            for change_file in enumerate(data["files"]):
                #stat[change_file["index"]] = 0
                log.info(change_file["index"])
            #core.set_torrent_file_priorities(torrent_id, stat)

    def _checking_tasks(self):
        core = component.get("Core")
        while not self.response_queue.empty():
            try:
                dat = self.response_queue.get(False)
                if dat != None:
                    if dat["finished"] is True:
                        core.remove_torrent(dat["hash"], True)
                        log.info("Torrent %s Completed...", dat["hash"])
                    else:
                        if len(dat["files"]) > 0:
                            self._change_file_prop(core, dat)
                    #self.waiting_dict.pop(dat["hash"])
                    self.working_dict.pop(dat["hash"])
            except Empty:
                pass
        waiting_dict = {}
        downloading_list = core.get_torrents_status({}, {})
        for d_key in downloading_list:
            waiting_dict[d_key] = downloading_list[d_key]
        for d_key in self.working_dict:
            if d_key in waiting_dict:
                waiting_dict.pop(d_key)
        push_len = WorkConfig.MAX_PROCESS - self.waiting_queue.qsize()
        if push_len > 0:
            for dd_key in waiting_dict:
                self.waiting_queue.put(waiting_dict[dd_key], False)
                self.working_dict[dd_key] = downloading_list[dd_key]
        
 
    def _sleep_and_wait(self, stime):
        self.record_lock.acquire()
        if not WorkConfig.disable:
            self.record_lock.release()
            if stime < 1:
                stime = 1
            for i in range(0, stime):
                self.record_lock.acquire()
                if not WorkConfig.disable:
                    self.record_lock.release()
                    time.sleep(1)
                else:
                    self.record_lock.release()
                    return
        else:
            self.record_lock.release()

    @export
    def set_config(self, config):
        """Sets the config dictionary"""
        for key in config.keys():
            self.config[key] = config[key]
        self.config.save()

    @export
    def get_config(self):
        """Returns the config dictionary"""
        return self.config.config
