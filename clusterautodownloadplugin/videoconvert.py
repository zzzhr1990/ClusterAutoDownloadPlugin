import base64
import time
import json
from workconfig import get_auth
from workconfig import WorkConfig
from deluge.log import LOG as log
from persistentfop import PersistentFop

class VideoConvert(object):
    """V"""
    def __init__(self, fid, orign_bucket, orign_key, width, height, dest_bucket, duration):
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
            file_addr = self.dest_bucket + ":" + self.dest_key_prefix + "/" +\
            self.orign_key + "-" + str(clear) + ".m3u8"
            ops = "avthumb/m3u8/segtime/5/vcodec/libx264/acodec/libfaac/s/" \
            + str(_width) + "x" + str(_height) + "/autoscale/1|saveas/" + \
            base64.urlsafe_b64encode(file_addr)
            return ops, file_addr
        else:
            file_addr = self.dest_bucket + ":" + self.dest_key_prefix + "/" +\
            self.orign_key + "-9999" + ".m3u8"
            ops = "avthumb/m3u8/segtime/5/vcodec/libx264/acodec/libfaac/s/" + str(self.width) \
            + "x" + str(self.height) + "/autoscale/1|saveas/" + \
            base64.urlsafe_b64encode(file_addr)
            return ops, file_addr

    def do_convert_action(self):
        """POST"""
        action_list = []
        max_reslov = 480
        if self.height > 480:
            cmd, file_addr = self._calc_ops_command(480)
            action_list.append({"clear":480, "cmd":cmd, "file":file_addr})
            max_reslov = 720
        if self.height > 720:
            cmd, file_addr = self._calc_ops_command(720)
            action_list.append({"clear":720, "cmd":cmd, "file":file_addr})
            max_reslov = 1080
        if self.height > 1080:
            cmd, file_addr = self._calc_ops_command(1080)
            action_list.append({"clear":1080, "cmd":cmd, "file":file_addr})
            max_reslov = 9999
        h_cmd, h_file_addr = self._calc_ops_command()
        action_list.append({"clear":max_reslov, "cmd":h_cmd, "file":h_file_addr})
        actions = []
        for ops in action_list:
            reslov = ops["clear"]
            fops = ops["cmd"]
            file_addr = ops["file"]
            auth = get_auth()
            ops = PersistentFop(auth, WorkConfig.MGR_URL, self.orign_bucket)
            url_prefix = "http://ks.killheaven.com/v1/video/callback/"
            final_url = url_prefix + base64.urlsafe_b64encode(json.\
                dumps({"fid":self.fid, "clear":reslov, "type":"m3u8", "duration":self.duration}))
#            log.info("CLEAR %d FOPS %s",reslov, fops)
#            log.info("CALLBACK %s", final_url)
            code, text = ops.execute(fops, self.orign_key, notifyurl=final_url)
            actions.append({"clear":reslov, "code":code, "resp":text, "file":file_addr})
            if code != 200:
                log.info("%s:%s exec fops error code %d, response %s, fops %s", \
                self.orign_bucket, self.orign_key, code, text, fops)
        return actions





