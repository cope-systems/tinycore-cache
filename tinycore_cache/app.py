import bottle
import os

from tinycore_cache.upstream_mirror_api import MirrorAPI


def attach_views(app, api, cache_dir):
    pass


def build_app(upstream_mirror_url, cache_dir):
    app = bottle.Bottle()
    api = MirrorAPI(upstream_mirror_url)
    attach_views(app, api, cache_dir)
    return app

if __name__ == "__main__":
    app = build_app("http://tinycorelinux.net/", "/home/rcope/tce-cache")