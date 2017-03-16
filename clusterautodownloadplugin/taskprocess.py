import requests
import base64
from deluge.log import LOG as log
import deluge.component as component
from deluge.plugins.pluginbase import CorePluginBase


class TaskProcess(object):
    """Task form task server."""
    def __init__(self, base_url):
        self._base_url = base_url

    def check_tasks(self):
        """Check tasks on server."""
        req = requests.get(self._base_url + '/v1/task'\
        , headers={"X-Task-Token" : "1024tasktoken"}, timeout=5)
        if req.status_code == 200:
            json_request = req.json()
            if json_request["errno"] == 0:
                if "data" in json_request:
                    data = json_request["data"]
                    if "data" in data:
                        data_arr = data["data"]
                        for single_data in data_arr:
                            self.fetch_single_task(single_data)
                    else:
                        log.warn("No data object found in response JSON['data'].\r\n%s", data)
                else:
                    log.warn("No data object found in response JSON. data %s", json_request)
            else:
                log.warn("Data return from server error [%d], msg: %s", \
                json_request["errno"], json_request["errmsg"])
        else:
            log.warn("Data recv error, code:%d", req.status_code)

    def fetch_single_task(self, single_task):
        """Check single task."""
        task_type = single_task["type"]
        if task_type == "torrent":
            req = requests.get(single_task["url"], \
            headers={"X-Task-Token" : "1024tasktoken"}, timeout=5)
            if req.status_code == 200:
                try:
                    core = component.get("Core")
                    #find if the task exists
                    task_info = core.get_torrent_status(single_task["infohash"], {})
                    if task_info != None:
                        if len(task_info) == 0:
                            log.warn("Task info has no info, maybe it has been removed? %s"\
                            , single_task["tid"])
                            # Report to task server?
                        else:
                            log.info("Torrent %s[%s] already in download list."\
                            , single_task["tid"], single_task["infohash"])
                    else:
                        log.info("Adding torrent %s[%s] to download list."\
                        , single_task["tid"], single_task["infohash"])
                        torrent_id = core.add_torrent_file(single_task["tid"],\
                        base64.encodestring(req.content), {})
                        if torrent_id != None:
                            log.info("Successfly add torrent, tid: %s", single_task["tid"])
                        self.change_torrent_status(single_task["tid"]\
                        , {"status" : 5, "infohash" : torrent_id})
                except Exception as ex:
                    log.error("Unable to add torrent file!: %s", ex)
                    return

    def change_torrent_status(self, tid, torrent_info):
        """Change the torrent status on server."""
        req = requests.put(self._base_url + '/v1/task/' + tid\
        , headers={"X-Task-Token" : "1024tasktoken"}, timeout=5, json=torrent_info)
        if req.status_code == 200:
            log.info("Received from server %s", req.json())
        else:
            log.info("Received HTTP error %d.", req.status_code)

