import base64
import time
import json
from workconfig import get_auth
from workconfig import WorkConfig
from deluge.log import LOG as log
from persistentfop import PersistentFop

class VideoConvert(object):
    """V"""
    def __init__(self,fid ,orign_bucket, orign_key, width, height, dest_bucket, duration):
        self.fid = fid
        self.orign_bucket = orign_bucket
        self.orign_key = orign_key
        self.width = width
        self.height = height
        self.dest_bucket = dest_bucket
        self.dest_key_prefix = "sp/m3u8/" + time.strftime("%Y%m%d", time.localtime())
        self.duration = duration

    def _calc_size(self, resize):
        if self.height > resize:
            return self.width * resize / self.height, resize
        else:
            return self.width, self.height

    def _calc_ops_command(self, clear=None):
        if clear:
            _width, _height = self._calc_size(clear)
            ops = "avthumb/m3u8/segtime/5/vcodec/libx264/acodec/libfaac/s/" \
            + str(_width) + "x" + str(_height) + "/autoscale/1|saveas/" + \
            base64.urlsafe_b64encode(self.dest_bucket + ":" + self.dest_key_prefix + "/" +\
            self.orign_key + "-" + str(clear) + ".m3u8")
            return ops
            
        else:
            ops = "avthumb/m3u8/segtime/5/vcodec/libx264/acodec/libfaac|saveas/" + \
            base64.urlsafe_b64encode(self.dest_bucket + ":" + self.dest_key_prefix + "/" +\
            self.orign_key + "-9999" + ".m3u8")
            return ops

    def do_convert_action(self):
        """POST"""
        action_list = []
        max_reslov = 480
        if self.height > 480:
            action_list.append({"clear":480, "cmd":self._calc_ops_command(480)})
            max_reslov = 720
        if self.height > 720:
            action_list.append({"clear":720, "cmd":self._calc_ops_command(720)})
            max_reslov = 1080
        if self.height > 1080:
            action_list.append({"clear":1080, "cmd":self._calc_ops_command(1080)})
            max_reslov = 9999
        action_list.append({"clear":max_reslov, "cmd":self._calc_ops_command()})

        for ops in action_list:
            reslov = ops["clear"]
            fops = ops["cmd"]
            auth = get_auth()
            ops = PersistentFop(auth,WorkConfig.MGR_URL, self.orign_bucket)
            url_prefix = "http://ks.killheaven.com/v1/video/callback/"
            final_url = url_prefix + base64.urlsafe_b64encode(json.\
                dumps({"fid":self.fid, "clear":reslov, "type":"m3u8", "duration":self.duration}))
            log.info("FOPS %s", fops)
            log.info("CALLBACK %s", final_url)
            code,text = ops.execute(fops, self.orign_key, notifyurl=final_url)
            log.info("fops code %d, result %s", code, json.dumps(text))





