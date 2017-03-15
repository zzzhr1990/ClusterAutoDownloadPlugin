import requests
import base64
from io import BytesIO
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
                log.info("json_OK")
                if hasattr(json_request, 'data'):
                    data = json_request["data"]
                    if hasattr(data, 'data'):
                        data_arr = data["data"]
                        for single_data in data_arr:
                            self.fetch_single_task(single_data)
                    else:
                        log.warn("No data object found in response JSON['data'].\r\n%s", data)
                else:
                    log.warn("No data object found in response JSON.")
            else:
                log.warn("Data return from server error [%d], msg: %s", \
                json_request["errno"], json_request["errmsg"])
        else:
            log.warn("Data recv error, code:%d", req.status_code)

    def fetch_single_task(self, single_task):
        """Check single task."""
        task_type = single_task["type"]
        log.info("Task id %s", single_task["tid"])
        if task_type == "torrent":
            req = requests.get(single_task["url"], \
            headers={"X-Task-Token", "1024tasktoken"}, timeout=5)
            if req.status_code == 200:
                raw_data = BytesIO(req.content)
                try:
                    core = component.get("Core")
                    torrent_id = core.get("Core").add_torrent_file(single_task["tid"],\
                    base64.encodebytes(raw_data), {})
                    if torrent_id != None:
                        log.info("Successfly add torrent, tid: %s", single_task["tid"])
                except Exception as ex:
                    log.error("Unable to add torrent file!: %s", ex)
                    return

