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
from twisted.internet.task import LoopingCall
from deluge.error import InvalidTorrentError
from deluge.log import LOG as log
from deluge.plugins.pluginbase import CorePluginBase
import deluge.component as component
import deluge.configmanager
from deluge.core.rpcserver import export
from workconfig import WorkConfig
from workconfig import get_auth
from multiprocessing.dummy import Pool as ThreadPool
from torrentprocesser import TorrentProcesser


DEFAULT_PREFS = {
    "test":"NiNiNi"
}

class Core(CorePluginBase):
    def __init__(self, plugin_name):
        self.plugin_name = plugin_name
        self.processing = False
        super(Core, self).__init__(plugin_name)

    def enable(self):
        log.info("plugin %s enabled.", self.plugin_name)
        WorkConfig.disable = False
        self.config = deluge.configmanager.ConfigManager("clusterautodownloadplugin.conf", DEFAULT_PREFS)
        self.working_loop()
        self.update_timer = LoopingCall(self.working_loop)
        self.update_timer.start(1)

    def disable(self):
        WorkConfig.disable = True
        log.info("plugin %s disabled.", self.plugin_name)
        try:
            self.update_timer.stop()
        except AssertionError:
            log.warn("stop download plugin error")

    def working_loop(self):
        # Refresh torrents.
        if WorkConfig.disable:
            return
        if self.processing:
            return
        self.processing = True
        try:
            self.process_torrents()
        except Exception as error:
            log.warn("error , %s , traceback \r\n %s", str(error), traceback.format_exc())
        finally:
            self.processing = False

    def process_torrents(self):
        downloading_list = component.get("Core").get_torrents_status({}, {})
        torrent_list = []
        for key in downloading_list:
            torrent_list.append(downloading_list[key])
        if len(torrent_list) > 0:
            proc = TorrentProcesser(torrent_list, WorkConfig.MAX_PROCESS)
            proc.start_process()

    def update_torrent_status(self,torrent_info):
        pass
    

        #filemanager.mgr_host = WorkConfig.MGR_HOST
#        log.info("ddddddd")
#        log.info(filemanager.mgr_host)
#        log.info("fffffff")
#        code, text = filemanager.stat(bucket, file_key)
#        log.info("file get from %s %d, %s",filemanager.mgr_host , code, text)

        # check file
        # confirm if this file uploaded


    def update(self):
        pass

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
