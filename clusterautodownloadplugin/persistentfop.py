import base64
import json
from util import Util

class PersistentFop(object):
    """FOPS"""
    def __init__(self, auth, host, bucket):
        self.bucket = bucket
        self.host = host
        self.auth = auth
    def build_op(self, cmd, first_arg, **kwargs):
        """Build OP"""
        operation = [cmd]
        if first_arg is not None:
            operation.append(first_arg)

        for k, value in kwargs.items():
            operation.append('{0}/{1}'.format(k, value))
        return '/'.join(operation)

    def pipe_cmd(self, *cmds):
        """pipe"""
        return '|'.join(cmds)

    def op_save(self, op, bucket, key):
        """SVE"""
        return self.pipe_cmd(op, 'saveas/' + Util.wcs_entry(bucket, key))

    def build_ops(self,ops,key):
        """save_as"""
        ops_list = []
        for op, params in ops.items():
            ops_list.append(self.op_save(self.build_op(op, params), self.bucket, key))
        return ops_list

    def fops_status(self, persistent_id):
        """dysy"""
        url = '{0}/status/get/prefop?persistentId={1}'.format(self.host, persistent_id)
        code, text = Util.do_wcs_get(url=url)
        return code, text

    def params(self, params):
        """Proc _ p"""
        if params:
            paramlist = []
            for k, v in params.items():
                paramlist.append('{0}={1}'.format(k, v))
            paramlist = '&'.join(paramlist)
        return paramlist

    def headers(self, url, data):
        """sdd"""
        headers = {}
        reqdata = self.params(data)
        headers['Authorization'] = self.auth.managertoken(url, body=reqdata)
        return headers, reqdata

    def execute(self, fops, key, force=None, separate=None, notifyurl=None):
        """exec"""
        data = {'bucket': base64.urlsafe_b64encode(str(self.bucket)),\
         'key': base64.urlsafe_b64encode(str(key)),\
          'fops': base64.urlsafe_b64encode(str(fops))}
        if notifyurl is not None:
            data['notifyURL'] = base64.urlsafe_b64encode(str(notifyurl))
        if force == 1:
            data['force'] = 1
        if separate == 1:
            data['separate'] = 1
        url = '{0}/fops'.format(self.host)
        headers, reqdata = self.headers(url, data)
        code, text = Util.do_wcs_post(url=url, data=reqdata, headers=headers)
        return code, text


