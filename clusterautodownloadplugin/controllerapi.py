import requests
import base64
import json
import logging
import time


class ControllerApi(object):
    """Task form task server."""

    def __init__(self, base_url):
        self._base_url = base_url

    def exec_requests_data_json(self, req):
        """Execute standard request"""
        if req.status_code == 200:
            json_request = req.json()
            if json_request["success"]:
                logging.info("SUCCCCCCCCCCCCCCC!!!!!!!!!!")
                if "data" in json_request:
                    return json_request["data"]
                else:
                    return None
            else:
                logging.warning("Data return from server [%s] error [%d], msg: %s", req.url,
                                json_request["code"], json_request["message"])
        else:
            logging.warning("Data recv error, code:%d", req.status_code)
        return None

    def register_server(self, sid, name):
        """Register Server"""
        post = {'sid': sid, 'name': name}
        req = requests.post(self._base_url + "/api/server/register",
                            headers={"Content-Type": "application/json"}, json=post, timeout=5)
        data = self.exec_requests_data_json(req)
        if data is None:
            logging.warning("Rec from LX Eempty")
            return None
        else:
            return data

    def shutdown_server(self, sid, name):
        """Register Server"""
        post = {'sid': sid, 'name': name}
        req = requests.post(self._base_url + "/api/server/shutdown",
                            headers={"Content-Type": "application/json"}, json=post, timeout=5)
        data = self.exec_requests_data_json(req)
        if data is None:
            logging.warning("Rec from LX Eempty")
            return None
        else:
            return data

    def create_file_info(self, post):
        """Check uploads on server."""
        try:
            req = requests.post(self._base_url + '/v1/files/',
                                headers={"X-Task-Token": "1024tasktoken"}, json=post, timeout=5)
            data = self.exec_requests_data_json(req)
            if data is None:
                logging.warning("Rec from LX Eempty")
                return False
            else:
                return True
        except Exception:
            logging.error("Request to create_file_info error.")
            return False

    def check_tasks(self, core):
        """Check tasks on server."""
        req = requests.get(self._base_url + '/v1/task',
                           headers={"X-Task-Token": "1024tasktoken"}, timeout=5)
        data = self.exec_requests_data_json(req)
        if data != None:
            if "data" in data:
                data_arr = data["data"]
                for single_data in data_arr:
                    logging.info("tid %s, torrent_hash %s",
                                 single_data["tid"], single_data["infohash"])
                    self.fetch_single_task(single_data, core)
            else:
                logging.warning(
                    "No data object found in response JSON['data'].\r\n%s", data)
        else:
            logging.warning("No data recv from check_tasks.")

    def _task_in_process(self, torrent_id, core):
        return torrent_id in core.torrentmanager.torrents

    def fetch_single_task(self, single_task, core):
        """Check single task."""
        task_type = single_task["type"]
        if self._task_in_process(single_task["infohash"], core):
            logging.info("Torrent %s[%s] already in download list.",
                         single_task["tid"], single_task["infohash"])
            self.change_torrent_status(single_task["tid"], {"status": 5})
            return
        if task_type == "torrent":
            req = requests.get(single_task["url"],
                               headers={"X-Task-Token": "1024tasktoken"}, timeout=5)
            if req.status_code == 200:
                try:
                    logging.info(
                        "Adding torrent %s[%s] to download list.", single_task["tid"], single_task["infohash"])
                    torrent_id = core.add_torrent_file(single_task["tid"],
                                                       base64.encodestring(req.content), {})
                    if torrent_id != None:
                        logging.info("Successfly add torrent, tid: %s Hash:%s, id:%s",
                                     single_task["tid"], single_task["infohash"], torrent_id)
                        self.change_torrent_status(
                            single_task["tid"], {"status": 5})
                        if single_task["infohash"] != torrent_id:
                            logging.warning("Task infohash mismatch tid %s, calc_hash %s, info %s",
                                            single_task["tid"], torrent_id, json.dumps(single_task))
                    else:
                        logging.info(
                            "Torrent %s[%s] already in download list but not figured before.", single_task["tid"], single_task["infohash"])
                except Exception as ex:
                    logging.error("Unable to add torrent file!: %s.", ex)
                    return
            else:
                logging.warning("Add torrent file error.")
        elif task_type == "magnet":
            try:
                torrent_id = core.add_torrent_magnet(single_task["source"], {})
                if torrent_id != None:
                    logging.info("Successfly add magnet, tid: %s",
                                 single_task["tid"])
                    self.change_torrent_status(
                        single_task["tid"], {"status": 5})
                else:
                    logging.warning(
                        "Magnet %s[%s] already in download list but not figured before.", single_task["tid"], single_task["infohash"])
            except Exception as ex:
                logging.error("Unable to add torrent file!: %s", ex)
        else:
            logging.warn("Unsupported %s", task_type)

    def change_torrent_status(self, tid, torrent_info):
        """Change the torrent status on server."""
        req = requests.put(self._base_url + '/v1/task/' + tid,
                           headers={"X-Task-Token": "1024tasktoken"}, timeout=5, json=torrent_info)
        self.exec_requests_data_json(req)

    def get_torrent_status(self, tid):
        """Change the torrent status on server."""
        req = requests.get(self._base_url + '/v1/task/' + tid,
                           headers={"X-Task-Token": "1024tasktoken"}, timeout=5)
        return self.exec_requests_data_json(req)

    def refresh_torrent_progress(self, tid, progress, info):
        """Change the torrent status on server."""
        torrent_info = {"progress": progress, "info": info,
                        "updatedtime": time.time() * 1000}
        req = requests.put(self._base_url + '/v1/task/' + tid,
                           headers={"X-Task-Token": "1024tasktoken"}, timeout=5, json=torrent_info)
        self.exec_requests_data_json(req)

    def update_file_info(self, fid, post):
        """Check uploads on server."""
        try:
            req = requests.put(self._base_url + '/v1/files/source/' + fid,
                               headers={"X-Task-Token": "1024tasktoken"}, json=post, timeout=5)
            data = self.exec_requests_data_json(req)
            if data is None:
                return False
            else:
                return True
        except Exception:
            logging.error("Request to update_file_info error.")
            return False
