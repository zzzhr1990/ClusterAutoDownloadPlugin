import base64
import json
from wcs.commons.auth import Auth
from wcs.commons.http import _post
from wcs.commons.http import _get
from wcs.commons.util import entry
from deluge.log import LOG as log

class BucketManager(object):

    def __init__(self, auth, host):
        self.auth = auth
        self.mgr_host = host

    def limit_check(self):
        n = 0
        while n < 1000:
            yield n
            n += 1

    def gernerate_headers(self,url,body=None):
        token = self.auth.managertoken(url,body=body)
        headers = {'Authorization': token}
        return headers
    
    def make_delete_url(self, bucket, key):
        return '{0}/delete/{1}'.format(self.mgr_host, entry(bucket, key))

    def delete(self, bucket, key):
        url = self.make_delete_url(bucket, key)
        log.info('Start to post request of delete %s:%s', bucket, key)
        code, text = _post(url=url, headers=self.gernerate_headers(url))
        log.info('The return code is %d and text of delete request is: %s', code, text)
        return code, text

    def make_filestat_url(self, bucket, key):
        return '{0}/stat/{1}'.format(self.mgr_host, entry(bucket, key))

    def stat(self, bucket, key):
        url = self.make_filestat_url(bucket, key)
        log.info('Start to get the stat of %s:%s', bucket, key)
        code, text = _get(url=url, headers=self.gernerate_headers(url))
        log.info('The return code : %d and text : %s', code, text)
        return code, text
 
    def make_list_url(self, param):
        url = ['{0}/list'.format(self.mgr_host)]
        if param:
            url.append(self.params_parse(param))
        url = '?'.join(url)
        return url
    
    def bucketlist(self, bucket, prefix=None, marker=None, limit=None, mode=None):
        options = {
            'bucket': bucket,
        }
        if marker is not None:
            options['marker'] = marker
        if limit is not None:
            if limit in self.limit_check():
                options['limit'] = limit
            else:
                log.error('Invalid limit ! Please redefine limit')
                raise ValueError("Invalid limit")
        if prefix is not None:
            options['prefix'] = prefix
        if mode is not None:
            options['mode'] = mode
        url = self.make_list_url(options)
        if options is not None:
            log.info('List options is %s', options)
        log.info('List bucket %s', bucket)
        code, text = _get(url=url, data=options, headers=self.gernerate_headers(url))
        log.info('The return code : %d and text : %s', code, text)
        return code, text

    def params_parse(self, params):
        if params:
            paramlist = [] 
            for k, v in params.items():
                paramlist.append('{0}={1}'.format(k, v))
            paramlist = '&'.join(paramlist) 
        return paramlist

    def make_move_url(self, srcbucket, srckey, dstbucket, dstkey):
        src = base64.b64encode('%s:%s' % (srcbucket, srckey))
        dst = base64.b64encode('%s:%s' % (dstbucket, dstkey)) 
        url = '{0}/move/{1}/{2}'.format(self.mgr_host, src, dst)
        return url

    def move(self, srcbucket, srckey, dstbucket, dstkey):
        url = self.make_move_url(srcbucket, srckey, dstbucket, dstkey)
        log.info('Move object %s from %s to %s' % (srckey, srcbucket, dstbucket))
        code, text = _post(url=url, headers=self.gernerate_headers(url))
        log.info('The return code : %d and text : %s', code, text)
        return code, text
 
    def make_copy_url(self, srcbucket, srckey, dstbucket, dstkey):
        src = base64.b64encode('%s:%s' % (srcbucket, srckey))
        dst = base64.b64encode('%s:%s' % (dstbucket, dstkey))
        url = '{0}/copy/{1}/{2}'.format(self.mgr_host, src, dst)
        return url

    def copy(self, srcbucket, srckey, dstbucket, dstkey):
        url = self.make_copy_url(srcbucket, srckey, dstbucket, dstkey)
        log.info('Copy object %s from %s to %s' % (srckey, srcbucket, dstbucket))
        code, text = _post(url=url, headers=self.gernerate_headers(url))
        log.info('The return code : %d and text : %s', code, text)
        return code, text

    def setdeadline(self, bucket, key, deadline):
        url = '{0}/setdeadline'.format(self.mgr_host)
        param = {
            'bucket' : base64.b64encode(bucket),
        }
        param['key'] = base64.b64encode(key) 
        param['deadline'] = deadline 
        body = self.params_parse(param)
        log.info('Set deadline of %s to %s' % (key, deadline))
        code, text = _post(url=url, data=body, headers=self.gernerate_headers(url, body))
        log.info('The return code : %d and text : %s', code, text)
        return code, text

#    def decompress(self, bucket, key, fops, notifyurl=None, force=None, separate=None):
#        url = '{0}/fops'.format(self.mgr_host)
#        data = {'fops': fops}
#        if notifyurl is not None:
#            data['notifyURL'] = base64.b64encode(notifyurl) 
#        if separate is not None:
#            data['separate'] = separate
#        if force is not None:
#            data['force'] = force
#        reqdata = self.params_parse(data)
#        log.info('Decompress object %s' % (key))
#        code, text = _post(url=url, headers=self.gernerate_headers(url, body=reqdata))
#        log.info('The return code : %d and text : %s', code, text)
#        return code, text

#    def update_mirror(self, bucket, filelist):
#        if filelist:
#            encodelist = []
#            for key in filelist:
#                encodelist.append(base64.b64encode(key))
#            encodelist = '|'.join(encodelist)
#        param = base64.b64encode('{0}:{1}'.format(bucket, encodelist))
#        url = '{0}/prefetch/{1}'.format(self.mgr_host, param)
#        log.info('Update mirror to bucket: %s' % (bucket))
#        code, text = _post(url=url, headers=self.gernerate_headers(url))
#        log.info('The return code : %d and text : %s', code, text)
#        return code, text
       
