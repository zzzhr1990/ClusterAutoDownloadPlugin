import base64
import json
from wcs.commons.http import _post
from wcs.commons.http import _get
from wcs.commons.util import entry
from wcs.commons.util import urlsafe_base64_encode

class PersistentFop(object):

    def __init__(self,auth,host ,bucket):
        self.bucket = bucket
        self.host = host
        self.auth = auth
      
    def build_op(self,cmd, first_arg, **kwargs):
        op = [cmd]
        if first_arg is not None:
            op.append(first_arg)

        for k, v in kwargs.items():
            op.append('{0}/{1}'.format(k, v))
        return '/'.join(op)

    def pipe_cmd(self,*cmds):
        return '|'.join(cmds)

    def op_save(self,op, bucket, key):
        return pipe_cmd(op, 'saveas/' + entry(bucket, key))

    def build_ops(self,ops,key):
        ops_list = []
        for op,params in ops.items(): 
            ops_list.append(self.op_save(self.build_op(op,params),self.bucket,key))
        return ops_list

    def fops_status(self, persistentId):
        url = '{0}/status/get/prefop?persistentId={1}'.format(self.host, persistentId)
        code, text = _get(url=url)
        return code, text

    def params(self,params):
        if params:
            paramlist = []
            for k, v in params.items():
                paramlist.append('{0}={1}'.format(k,v))
            paramlist = '&'.join(paramlist) 
        return paramlist

    def headers(self,url,data):
        headers = {} 
        reqdata = self.params(data)
        headers['Authorization'] = self.auth.managertoken(url,body=reqdata)
        return headers,reqdata

    def execute(self,fops,key,force=None,separate=None,notifyurl=None):
        data = {'bucket': urlsafe_base64_encode(self.bucket), 'key': urlsafe_base64_encode(key), 'fops': urlsafe_base64_encode(fops)}
        if notifyurl is not None:
            data['notifyURL'] = urlsafe_base64_encode(notifyurl)
        if force == 1:
            data['force'] = 1
        if separate == 1:
            data['separate'] = 1
        url = '{0}/fops'.format(self.host)
        headers,reqdata = self.headers(url,data)
        code,text = _post(url=url, data=reqdata,headers=headers)
        return code,text


