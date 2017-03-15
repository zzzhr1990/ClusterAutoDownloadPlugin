
import requests
from deluge.log import LOG as log
from io import BytesIO


class MainServerHelper(object):
    """Comm to server."""
    def __init__(self, base_url):
        self.base_url = base_url

    def check_tasks(self):
        """Check tasks on server."""
        req = requests.get(self.base_url + '/user', \
        headers={"X-Task-Token", "1024tasktoken"}, timeout=5)
        if req.status_code == 200:
            json_result = req.json()
            if json_result["errno"] == 0:
                if hasattr(json_result, 'data'):
                    data = json_result["data"]
                    if hasattr(data, 'data'):
                        data_arr = data["data"]
                        for single_task in data_arr:
                            self.process_single_task(self, single_task)
            else:
                log.warn("Check task error, code %d, message %s", \
                json_result["errno"], json_result["errmsg"])
        else:
            log.warn("Check task HTTP error, HTTP code %d", req.status_code)

    def process_single_tash(self, task):
        log.info("fetched task %s, hash %s", task["url"])
        task_type = task["type"]
        if task_type == "torrent":
            req = requests.get(task["url"], \
            timeout=5)
            if req.status_code == 200:
                BytesIO(req.content)

        

