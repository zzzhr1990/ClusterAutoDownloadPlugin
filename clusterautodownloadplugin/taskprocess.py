import requests
import base64
import json
from deluge.log import LOG as log
import deluge.component as component
from deluge.plugins.pluginbase import CorePluginBase


class TaskProcess(object):
    """Task form task server."""
    def __init__(self, base_url):
        self._base_url = base_url

    def exec_requests_data_json(self, req):
        """Execute standard request"""
        if req.status_code == 200:
            json_request = req.json()
            if json_request["errno"] == 0:
                if "data" in json_request:
                    return json_request["data"]
                else:
                    log.warn("No data object found in response JSON. data %s", json_request)
            else:
                log.warn("Data return from server error [%d], msg: %s", \
                json_request["errno"], json_request["errmsg"])
        else:
            log.warn("Data recv error, code:%d", req.status_code)
        return None

    def _task_in_process(self, torrent_id, core):
        return torrent_id in core.torrentmanager.torrents

    def upload_file_info(self, post):
        """Check uploads on server."""
        req = requests.post(self._base_url + '/v1/files'\
        , headers={"X-Task-Token" : "1024tasktoken"}, json=post, timeout=5)
        log.info("Post to LX - %s", json.dumps(post))
        data = self.exec_requests_data_json(req)
        if data != None:
            log.info("Rec from LX %s", json.dumps(data))
    def check_tasks(self):
        """Check tasks on server."""
        req = requests.get(self._base_url + '/v1/task'\
        , headers={"X-Task-Token" : "1024tasktoken"}, timeout=5)
        data = self.exec_requests_data_json(req)
        if data != None:
            if "data" in data:
                data_arr = data["data"]
                for single_data in data_arr:
                    self.fetch_single_task(single_data)
            else:
                log.warn("No data object found in response JSON['data'].\r\n%s", data)
        else:
            log.warn("No data recv from check_tasks.")

    def fetch_single_task(self, single_task):
        """Check single task."""
        task_type = single_task["type"]
        core = component.get("Core")
        if self._task_in_process(single_task["infohash"], core):
            log.info("Torrent %s[%s] already in download list."\
                , single_task["tid"], single_task["infohash"])
            self.change_torrent_status(single_task["tid"]\
                , {"status" : 5, "infohash" : single_task["infohash"]})
            return

 #       task_info = core.get_torrent_status(single_task["infohash"], {})
 #       if task_info != None:
 #           if len(task_info) == 0:
 #               if task_type != "magnet":
 #                   log.warn("Task info has no info, maybe it has been removed? %s"\
 #                       , single_task["tid"])
 #           log.info("Torrent %s[%s] already in download list. %s"\
 #               , single_task["tid"], single_task["infohash"], json.dumps(task_info))
 #           self.change_torrent_status(single_task["tid"]\
 #               , {"status" : 5, "infohash" : single_task["infohash"]})
 #          return
 #       else:
 #           log.info("Adding %s", single_task["source"])
                # Report to task server?
                #self.change_torrent_status(single_task["tid"]\
                #    , {"status" : 5, "infohash" : single_task["infohash"]})
        if task_type == "torrent":
            req = requests.get(single_task["url"], \
            headers={"X-Task-Token" : "1024tasktoken"}, timeout=5)
            if req.status_code == 200:
                try:
                    log.info("Adding torrent %s[%s] to download list."\
                        , single_task["tid"], single_task["infohash"])
                    torrent_id = core.add_torrent_file(single_task["tid"],\
                    base64.encodestring(req.content), {})
                    if torrent_id != None:
                        log.info("Successfly add torrent, tid: %s", single_task["tid"])
                        self.change_torrent_status(single_task["tid"]\
                            , {"status" : 5, "infohash" : torrent_id})
                    else:
                        log.info(\
                            "Torrent %s[%s] already in download list but not figured before."\
                            , single_task["tid"], single_task["infohash"])
                except Exception as ex:
                    log.error("Unable to add torrent file!: %s.", ex)
                    return
            else:
                log.info("Add torrent file error.")
        if task_type == "magnet":
            try:
                torrent_id = core.add_torrent_magnet(single_task["source"], {})
                if torrent_id != None:
                    log.info("Successfly add magnet, tid: %s", single_task["tid"])
                    self.change_torrent_status(single_task["tid"]\
                        , {"status" : 5, "infohash" : torrent_id})
                else:
                    log.warn(\
                        "Magnet %s[%s] already in download list but not figured before."\
                        , single_task["tid"], single_task["infohash"])
            except Exception as ex:
                log.error("Unable to add torrent file!: %s", ex)
        else:
            pass

    def change_torrent_status(self, tid, torrent_info):
        """Change the torrent status on server."""
        req = requests.put(self._base_url + '/v1/task/' + tid\
        , headers={"X-Task-Token" : "1024tasktoken"}, timeout=5, json=torrent_info)
        self.exec_requests_data_json(req)

