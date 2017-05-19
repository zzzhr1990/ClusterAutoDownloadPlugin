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
        """Get Parse"""
        magnet_hash = magnet_hash.upper()
        surl = self.site_url + magnet_hash
        content = self._try_get(surl, 5)
        if not content:
            return None
        soup = BeautifulSoup(content, "html.parser")
        key_value, img_src = self._find_things(soup)
        if not key_value:
            return None
        if not img_src:
            return None
        self.my_session.headers['referer'] = surl
        rss = self._try_get_content(surl, key_value, img_src)
        self.my_session.headers['referer'] = surl
        return rss

    def _try_get_content(self, url, key, image_url, try_time=3):
        try_time = try_time - 1
        if try_time < 0:
            logging.warn('request %s failed.', url)
            return None
        code = self._parse_code(image_url)
        if not code:
            return None
        cont = self._try_post_json('http://btcache.me/download',
                                   data={'key': key, 'captcha': code})
        if not cont:
            return None
        mime = Util.mime_buffer(cont)
        if mime != 'text/html':
            return cont
        else:
            logging.warn('%s %s', url, mime)
            try:
                soup = BeautifulSoup(cont, "html.parser")
                error_text = soup.div.div.p.text
                logging.warn('Cannot get data...%s', error_text)
            except RuntimeError as ex:
                logging.warn(ex)
                return self._try_get(url, try_time)
        return None

    def _try_get(self, url, try_time=3):
        try_time = try_time - 1
        if try_time < 0:
            logging.warn('request %s failed.', url)
            return None
        try:
            req = self.my_session.get(url)
            if req.status_code < 400:
                return req.content
            else:
                logging.warn('get %s HTTP code %d', url, req.status_code)
                return self._try_get(url, try_time)
        except RuntimeError as ex:
            logging.error(ex)
            return self._try_get(url, try_time)

    def _find_things(self, soup):
        if soup.form:
            if soup.form.input:
                key_value = soup.form.input.get('value')
                img_src = soup.form.img.get('src')
                return (key_value, img_src)
        try:
            error_text = soup.div.div.p.text
            logging.warn('Cannot get data...%s', error_text)
            return None, None
        except RuntimeError as ex:
            logging.error(ex)
            return None, None

    def _parse_code(self, img_src):
        data = self._try_get(img_src + "?" + str(random.random()))
        if not data:
            return None
        return self._decode_code(data)

    def _decode_code(self, image_data):
        dat = self._try_post_json(url='http://op.juhe.cn/vercode/index', data={
            'key': '3a1da00cdc6130f56a8868745b33600c',
            'codeType': '1005',
            'base64Str': base64.encodestring(image_data)
        })
        if not dat:
            return None
        if dat['error_code'] == 0:
            return dat['result'].lower()
        else:
            logging.info("parse error %s", json.dumps(dat))
            return None

    def _try_post_json(self, url, data, try_time=3):
        try_time = try_time - 1
        if try_time < 0:
            logging.warn('request %s failed.', url)
            return None
        try:
            req = self.my_session.post(url=url, data=data)
            if req.status_code < 400:
                logging.warn("Responsed %d", req.status_code)
                logging.info(req.status_code)
                logging.info(req.content)
                return req.json()
            else:
                logging.warn('get %s HTTP code %d', url, req.status_code)
                return self._try_post_json(url, data, try_time)
        except RuntimeError as ex:
            logging.error(ex)
            return self._try_post_json(url, data, try_time)
