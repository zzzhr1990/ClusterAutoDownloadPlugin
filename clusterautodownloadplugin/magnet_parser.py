import requests
import random
import logging
import base64
import json
from bs4 import BeautifulSoup
from util import Util


class MagnetParser(object):
    def __init__(self):
        self.site_url = 'http://btcache.me/torrent/'
        self.my_session = requests.Session()
        headers = {'User-agent':
                   'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
                   'referer': "http://www.cctv.com.cn/"}
        self.my_session.headers.update(headers)

    def start_parse(self, magnet_hash):
        """REQ"""
        magnet_hash = magnet_hash.upper()
        req = self.my_session.get(self.site_url + magnet_hash)
        content = req.text
        soup = BeautifulSoup(content, "html.parser")
        key_value = soup.form.input.get('value')
        logging.info('value %s', key_value)
        img_src = soup.form.img.get('src')
        logging.info('src value %s', img_src)
        code = self._parse_code(img_src)
        if code:
            self.my_session.headers['referer'] = self.site_url + magnet_hash
            self.my_session.headers.update(self.my_session.headers)
            logging.info("DOWNLOAD_CAPCHA %s", code)
            pseq = self.my_session.post('http://btcache.me/download',
                                        data={'key': key_value, 'code': code})
            cont = pseq.content
            if Util.mime_buffer(cont) == 'text/html':
                logging.info(cont)
        else:
            logging.info("Onshit, code desc error")

    def _parse_code(self, img_src):
        req = self.my_session.get(img_src + "?" + str(random.random()))
        data = req.content
        with open('/tmp/last_capcha.jpeg', 'wb') as fd:
            for chunk in req.iter_content(1024):
                fd.write(chunk)
        return self._decode_code(data)

    def _decode_code(self, image_data):
        req = requests.post(url='http://op.juhe.cn/vercode/index', data={
            'key': '3a1da00cdc6130f56a8868745b33600c',
            'codeType': '1005',
            'base64Str': base64.encodestring(image_data)
        })
        dat = req.json()
        if dat['error_code'] == 0:
            return dat['result'].lower()
        else:
            logging.info("parse error %s", json.dumps(dat))
            return None
