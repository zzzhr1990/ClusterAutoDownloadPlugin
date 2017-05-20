import requests
import logging
import json


class TorrentUploader(object):
    """used for upload thins"""

    def __init__(self, url):
        self.post_token_url = url + "/api/upload/token"

    def post_file(self, user_id, file_hash, mime, file_dump):
        """t"""
        token_data = self._try_and_get_content(
            self.post_token_url + '?userId=' + str(user_id))
        if not token_data:
            logging.warning("%s cannot get upload token.", file_hash)
            return
        upload_addr = token_data["uploadUrl"]
        upload_data = {'token': token_data['token']}
        upload_file = {'file': (file_hash, file_dump, mime)}
        return self._try_and_post_file(upload_addr, upload_data, upload_file)

        # Post to ws..

    def _try_and_post_file(self, url, data, files, try_time=10):
        """files = {'file': ('report.xls', open('report.xls', 'rb')
        , 'application/vnd.ms-excel', {'Expires': '0'})}
        """
        req = requests.post(url, data=data, files=files, timeout=5)
        if try_time < 0:
            logging.error("Post file %s error", url)
        try:
            if req.status_code < 400:
                return req.json()
            else:
                logging.warning("File %s , errorcode %d.",
                                str(url), req.status_code)
                return self._try_and_post_file(url, data, files, try_time - 1)
        except RuntimeError as ex:
            logging.error(ex)
            return self._try_and_post_file(url, data, files, try_time - 1)
        return None

    def _try_and_get_content(self, url, try_time=10):
        req = requests.get(url, timeout=5)
        if try_time < 0:
            logging.error("Download file %s error", url)
        try:
            if req.status_code < 400:
                xson = req.json()
                if not xson['success']:
                    logging.warning("API Not success, %s", json.dumps(xson))
                    return self._try_and_get_content(url, try_time - 1)
                return xson['data']
            else:
                logging.warning("File %s , errorcode %d.",
                                str(url), req.status_code)
                return self._try_and_get_content(url, try_time - 1)
        except RuntimeError as ex:
            logging.error(ex)
            return self._try_and_get_content(url, try_time - 1)
