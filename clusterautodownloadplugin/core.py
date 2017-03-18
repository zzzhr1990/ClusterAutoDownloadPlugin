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
        self.waiting_dict = {}
        self.waiting_queue = multiprocessing.Queue()
        log.info("Cluster downloader init, poolsize %d", WorkConfig.MAX_PROCESS)

    def enable(self):
        """Call when plugin enabled."""
        WorkConfig.disable = False
        for i in range(0, WorkConfig.MAX_PROCESS):
            command_queue = multiprocessing.Queue()
            proccess = TorrentProcesser(i, self.waiting_queue, command_queue)
            self.command_queues.append(command_queue)
            self.processing_pool.append(proccess)
            proccess.daemon = True
            proccess.start()

        #self.looping_thread.start()
        self.task_looping_thread.start()
        log.info("Plugin %s enabled.", self.plugin_name)

    def disable(self):
        """Call when plugin disabled."""
        WorkConfig.disable = True
        log.warn("Trying to shutdown download plugin")
        #
        for queue in self.command_queues:
            log.info("Sending...")
            queue.put(True, block=False)
            log.info("Send")
        log.warn("Trying to shutdown download plugin...success")


    def _task_loop(self):
        while not WorkConfig.disable:
            if self.fetching_task:
                log.warn("Slow fetching task.")
                return
            self.fetching_task = True
            try:
                self.processor.check_tasks()
            except Exception as e:
                log.error("Exception occored in task loop. %s -- \r\n%s", e, traceback.format_exc())
            finally:
                self.fetching_task = False
            self._sleep_and_wait(5)
            log.info("__TASK_LOP_CHK_%d", WorkConfig.disable)

    def _loop(self):
        while not WorkConfig.disable:
            if self.busy:
                log.warn("Slow query found.")
                return
            self.busy = True
            try:
                self._checking_tasks()
            except Exception as e:
                log.error("Exception occored in status loop. %s -- \r\n%s", e, traceback.format_exc())
            finally:
                self.busy = False
            if not WorkConfig.disable:
                self._sleep_and_wait(2)
            #log.info("Trying to fetching tasks...")

    def _checking_tasks(self):
        downloading_list = component.get("Core").get_torrents_status({}, {})
        for d_key in downloading_list:
            self.waiting_dict[d_key] = downloading_list[d_key]
        push_len = WorkConfig.MAX_PROCESS - self.waiting_queue.qsize()
        if push_len > 0:
            f_pop = []
            for dd_key in self.waiting_dict:
                f_pop.append(dd_key)
                self.waiting_queue.put(self.waiting_dict[dd_key], False)
            for a_delete in f_pop:
                self.waiting_dict.pop(a_delete)
 
    def _sleep_and_wait(self, stime):
        if not WorkConfig.disable:
            if stime < 1:
                stime = 1
            for i in range(0, stime):
                if not WorkConfig.disable:
                    time.sleep(1)

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
