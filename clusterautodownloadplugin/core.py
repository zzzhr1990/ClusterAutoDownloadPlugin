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
import uuid
import multiprocessing
from deluge.error import InvalidTorrentError
from deluge.log import LOG as log
from deluge.plugins.pluginbase import CorePluginBase
import deluge.component as component
import deluge.configmanager
from deluge.core.rpcserver import export
from torrentprocessor import TorrentProcessor
from globalconfig import PGlobalConfig
from controllerapi import ControllerApi
from mqservice import MqService
from eventpacher import EventPacher


DEFAULT_PREFS = {
    "test": "NiNiNi"
}


class Core(CorePluginBase):
    '''Init Function'''

    def __init__(self, plugin_name):
        self.core = component.get("Core")
        self.torrent_processor = TorrentProcessor(PGlobalConfig.max_process,
                                                  PGlobalConfig.server_name, self.core)
        self.core.eventmanager.register_event_handler(
            "TorrentFileCompletedEvent", self._on_torrent_file_completed)
        self.core.eventmanager.register_event_handler(
            "TorrentFinishedEvent", self._on_torrent_completed)
        self.checking_array = []
        self.event_patcher = EventPacher(self.core)
        self.event_patcher.patch_events()
        self.plugin_name = plugin_name
        self.processing = False
        self.config = deluge.configmanager\
            .ConfigManager("clusterautodownloadplugin.conf", DEFAULT_PREFS)
        super(Core, self).__init__(plugin_name)
        self.looping_thread = threading.Thread(target=self._loop)
        self.looping_thread.daemon = True
 #       self.task_looping_thread = threading.Thread(target=self._task_loop)
 #       self.task_looping_thread.daemon = True
        self.disabled = True
        self.busy = False
        self.fetching_task = False

        mq_host = "localhost"
        if "mqhost" in self.config:
            mq_host = self.config["mqhost"]

        mq_port = self._get_config_or_default("mqport", 5672)
        mq_user = self._get_config_or_default("mquser", "")
        mq_pass = self._get_config_or_default("mqpass", "")

        self.record_lock = threading.Lock()
        log.info("Cluster downloader init.")
        if "sid" in self.config:
            self.sid = self.config["sid"]
        else:
            self.sid = uuid.uuid4().urn[9:]
        log.info("Set sid %s", self.sid)
        if "name" in self.config:
            self.name = self.config["name"]
        else:
            self.name = "Unknown"
        if "controller" in self.config:
            self.controller = self.config["controller"]
        else:
            self.controller = "http://119.29.174.171:8080"
            log.info("Cluster Controller - Use %s default.", self.controller)
        if "usercontroller" in self.config:
            self.user_controller = self.config["usercontroller"]
        else:
            self.user_controller = "http://tencent2.qiecdn.com:8090"
            log.info("User Controller - Use %s default.", self.user_controller)
        self.mq_service = MqService(self.sid,
                                    mq_host, mq_port, mq_user,
                                    mq_pass, self.core, self.user_controller)
        self.controller_api = ControllerApi(self.controller)

    def _get_config_or_default(self, key, default_value):
        if key in self.config:
            return self.config[key]
        return default_value

    def enable(self):
        """Call when plugin enabled."""
        self.mq_service.start_async()
        self.looping_thread.start()
        self.disabled = False
        return
        c_data = self.controller_api.register_server(self.sid, self.name)
        log.info("Register Server %s", json.dumps(c_data))
        #
        # self.task_looping_thread.start()
        log.info("- Plugin %s enabled.", self.plugin_name)
        # when we start we'd check all downloading files.

    def _on_torrent_completed(self, torrent_id):
        log.info("%s downloaded.!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", torrent_id)

    def _on_torrent_file_completed(self, torrent_id, index):
        # get file info...
        try:
            torrent_info = self.core.get_torrent_status(
                torrent_id,
                ['files', 'save_path', 'move_completed', 'move_completed_path'])
            file_data = torrent_info['files'][index]
#            size = file_data['size']
            dest_path = torrent_info["save_path"]
            if torrent_info["move_completed"]:
                dest_path = torrent_info["move_completed_path"]
            file_path = u'/'.join([dest_path, file_data["path"]])
            # Dispatch to queue.
            file_prop = {'torrent_id': torrent_id, 'try_time': 0,
                         'file_path': file_path, 'file_size': file_data['size'],
                         'file_index': index, 'success': False}
            self.torrent_processor.add_torrent_file(file_prop)

        except RuntimeError as ex:
            log.error(ex)

    def disable(self):
        """Call when plugin disabled."""
        self.mq_service.stop()
        return
        c_data = self.controller_api.shutdown_server(self.sid, self.name)
        log.info("Shutdown Server %s", json.dumps(c_data))
        self.record_lock.acquire()
        self.disabled = True
        log.warn("Trying to shutdown download plugin")
        self.torrent_processor.disable_process()
        self.record_lock.release()
        log.warn("Trying to shutdown download plugin...success")

    """
    def _task_loop(self):
        while True:
            self.record_lock.acquire()
            if self.disabled:
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
                self.processor.check_tasks(component.get("Core"))
            except Exception as e:
                log.error("Exception occored in task loop. %s -- \r\n%s",
                          e, traceback.format_exc())
            finally:
                self.fetching_task = False
            self._sleep_and_wait(5)
    """

    def _loop(self):
        while True:
            self.record_lock.acquire()
            if self.disabled:
                self.record_lock.release()
                log.info("Ternimate check loop...")
                return
            try:
                if self.busy:
                    log.warn("Slow query found.")
                else:
                    self.busy = True
                    self._checking_torrent_status()
            except Exception as e:
                log.error("Exception occored in status loop. %s -- \r\n%s",
                          e, traceback.format_exc())
            finally:
                self.busy = False
                self.record_lock.release()
            self._sleep_and_wait(2)

    def _checking_torrent_status(self):
        #core = component.get("Core")
        self.torrent_processor.update_torrent_info()

    def _sleep_and_wait(self, stime):
        self.record_lock.acquire()
        if not self.disabled:
            self.record_lock.release()
            if stime < 1:
                stime = 1
            for i in range(0, stime):
                self.record_lock.acquire()
                if not self.disabled:
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
