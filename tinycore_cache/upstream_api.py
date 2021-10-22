import requests


class MirrorAPI(object):
    def __init__(self, base_url):
        self._base_url = base_url
        self._session = requests.Session()
