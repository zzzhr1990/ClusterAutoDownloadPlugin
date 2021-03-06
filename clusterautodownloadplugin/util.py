import string
import random
import hashlib
import binascii
import six
import base64
import requests
import magic
from io import BytesIO
from requests.exceptions import Timeout
from requests.exceptions import ConnectionError as RConnectionError
from wcsauth import WcsAuth


class Util(object):
    @staticmethod
    def get_random_string(length=32):
        """Get ramdom str"""
        chars = string.ascii_letters + string.digits
        return ''.join([random.choice(chars) for i in range(length)])

    @staticmethod
    def readfile(input_stream, offset, size):
        """Read file fro, offset"""
        input_stream.seek(offset)
        dest = input_stream.read(size)
        if dest:
            return dest

    @staticmethod
    def md5(str_s):
        """calc md5"""
        m_uu = hashlib.md5()
        m_uu.update(str_s)
        return m_uu.hexdigest()

    @staticmethod
    def sha1(data):
        """Calc SHA1"""
        sha_hash = hashlib.sha1()
        sha_hash.update(data)
        return sha_hash.digest()

    @staticmethod
    def wcs_etag(file_path, block_size=1024 * 1024 * 4):
        """Calc WCS Etag(FileHash)"""
        with open(file_path, 'rb') as input_stream:
            array = [Util.sha1(block) for block in Util.file_iter(
                input_stream, 0, block_size)]
            if len(array) == 0:
                array = [Util.sha1(b'')]
            if len(array) == 1:
                data = array[0]
                prefix = b'\x16'
            else:
                sha1_str = six.b('').join(array)
                data = Util.sha1(sha1_str)
                prefix = b'\x96'
            return base64.urlsafe_b64encode(prefix + data)

    @staticmethod
    def wcs_etag_bytes(buff, block_size=1024 * 1024 * 4):
        """Calc WCS Etag(FileHash)"""
        with BytesIO(buff) as input_stream:
            array = [Util.sha1(block) for block in Util.file_iter(
                input_stream, 0, block_size)]
            if len(array) == 0:
                array = [Util.sha1(b'')]
            if len(array) == 1:
                data = array[0]
                prefix = b'\x16'
            else:
                sha1_str = six.b('').join(array)
                data = Util.sha1(sha1_str)
                prefix = b'\x96'
            return base64.urlsafe_b64encode(prefix + data)

    @staticmethod
    def file_iter(input_stream, offset, size):
        """Read input stream"""
        input_stream.seek(offset)
        d_read = input_stream.read(size)
        while d_read:
            yield d_read
            d_read = input_stream.read(size)

    @staticmethod
    def mime(file_path):
        """Get mime"""
        file_mime = u"application/octet-stream"
        try:
            file_mime = magic.from_file(file_path, mime=True)
        except Exception:
            file_mime = u"application/octet-stream"
        return file_mime

    @staticmethod
    def mime_buffer(file_path):
        """Get mime"""
        file_mime = u"application/octet-stream"
        try:
            file_mime = magic.from_buffer(file_path, mime=True)
        except Exception:
            file_mime = u"application/octet-stream"
        return file_mime

    @staticmethod
    def to_ascii(ustr):
        """UTF8 Strings to ascii(py2)"""
        return ustr.encode('utf8')

    @staticmethod
    def crc32(data):
        """Calc crc32"""
        return binascii.crc32(six.b(data)) & 0xffffffff

    @staticmethod
    def do_wcs_post(url, headers, data=None):
        """Post to wcs"""
        try:
            resp = requests.post(url, data=data, headers=headers, timeout=5)
            if Util.wcs_need_retry(resp.status_code):
                return -1, {}
            else:
                try:
                    return resp.status_code, resp.json()
                except Exception:
                    return resp.status_code, {"message": resp.text}
        except Timeout:
            return -1, {"message": "connection Timeout"}
        except RConnectionError:
            return -1, {"message": "connection Error"}

    @staticmethod
    def do_wcs_get(url, headers=None, data=None):
        """Post to wcs"""
        try:
            resp = requests.get(url, data=data, headers=headers, timeout=5)
            if Util.wcs_need_retry(resp.status_code):
                return -1, {}
            else:
                try:
                    return resp.status_code, resp.json()
                except Exception:
                    return resp.status_code, {"message": resp.text}
        except Timeout:
            return -1, {"message": "connection Timeout"}
        except RConnectionError:
            return -1, {"message": "connection Error"}

    @staticmethod
    def wcs_need_retry(code):
        """N"""
        if code == -1:
            return True
        if code // 100 == 5 and code != 579:
            return True
        return False

    @staticmethod
    def wcs_entry(bucket, key):
        """Calc e"""
        if key is None:
            return base64.urlsafe_b64encode('{0}'.format(bucket))
        else:
            return base64.urlsafe_b64encode('{0}:{1}'.format(bucket, key))

    @staticmethod
    def default_wcs_auth():
        """c"""
        return WcsAuth.default_auth()

    @staticmethod
    def sizeof_fmt(num, suffix='B'):
        """nf"""
        for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
            if abs(num) < 1024.0:
                return "%3.1f%s%s" % (num, unit, suffix)
            num /= 1024.0
        return "%.1f%s%s" % (num, 'Yi', suffix)

    @staticmethod
    def get_wcs_avinfo(host, key):
        """Check This File Status"""
        try:
            req = requests.get(host + "/" + key + "?op=avinfo", timeout=60)
            if req.status_code != 200:
                return {}
            return req.json()
        except Exception:
            return {}

#    @staticmethod
#    def file_to_stream(path):
#        """open an stream"""
#        with open(path, "rb") as dest:
#            return dest
