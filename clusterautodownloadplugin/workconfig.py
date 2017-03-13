from wcs.commons.auth import Auth

class WorkConfig:
    disable = False
    MGR_URL = "http://qietv.mgr21.v1.wcsapi.com"
    PUT_URL = "http://qietv.up21.v1.wcsapi.com"
    ACCESS_KEY = "0a3836b4ef298e7dc9fc5da291252fc4ac3e0c7f"
    SECRET_KEY = "da17a6ffaeab4ca89ce7275d9a8060206cb3de8e"
    MAX_PROCESS = 16

def get_auth():
    return Auth(WorkConfig.ACCESS_KEY, WorkConfig.SECRET_KEY)
