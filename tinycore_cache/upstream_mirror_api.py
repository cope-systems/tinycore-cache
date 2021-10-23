import requests
import gzip
from six.moves.urllib_parse import urljoin


class ResourceUnmodified(requests.HTTPError):
    pass


class MirrorAPI(object):
    def __init__(self, base_url):
        self._base_url = base_url
        self._session = requests.Session()

    def _get(self, sub_url, params=None, headers=None, stream=False, last_cache_data=None):
        headers = headers or {}
        if last_cache_data and "Last-Modified" in last_cache_data:
            headers["If-Modified-Since"] = last_cache_data["Last-Modified"]
        if last_cache_data and "ETag" in last_cache_data:
            headers["If-None-Match"] = last_cache_data["ETag"]
        headers.update({'User-Agent': 'Lab6RESTClient'})
        result = self._session.get(
            urljoin(self._base_url, sub_url),
            params=params,
            headers=headers,
            stream=stream
        )
        if last_cache_data is not None and result.status_code == 304:
            raise ResourceUnmodified("Resource unmodified since last API call.", response=result)
        result.raise_for_status()
        return result, self._extract_cache_headers(result)

    @staticmethod
    def _extract_cache_headers(result):
        headers = result.headers
        cache_data = {}
        if "Last-Modified" in headers:
            cache_data["Last-Modified"] = headers["Last-Modified"]
        if "ETag" in headers:
            cache_data["ETag"] = headers["ETag"]
        return cache_data

    def _get_gzip_text(self, sub_url, params=None, headers=None, last_cache_data=None):
        result, cache_data = self._get(sub_url, params=params, headers=headers, last_cache_data=last_cache_data)
        return gzip.decompress(result.content).decode("utf-8"), cache_data

    def package_list(self, version, arch, compressed=True, last_cache_data=None):
        if compressed:
            data, cache_data = self._get_gzip_text(
                "/{0}/{1}/tcz/info.lst.gz".format(version, arch), last_cache_data=last_cache_data
            )
        else:
            data, cache_data = self._get(
                "/{0}/{1}/tcz/info.lst".format(version, arch), last_cache_data=last_cache_data
            )
            data = data.text
        return [pkg for pkg in data.split("\n") if pkg.strip()], cache_data

    def get_md5_db(self, version, arch, last_cache_data=None):
        data, cache_data = self._get_gzip_text(
            "/{0}/{1}/tcz/md5.db.gz".format(version, arch), last_cache_data=last_cache_data
        )
        md5_db = {}
        for line in data.split("\n"):
            if line.strip():
                md5sum, pkg = line.split(" ", 1)
                md5_db[pkg] = md5sum
        return md5_db, cache_data

    def get_size_list(self, version, arch, compressed=True, last_cache_data=None):
        if compressed:
            data, cache_data = self._get_gzip_text(
                "/{0}/{1}/tcz/sizelist.gz".format(version, arch), last_cache_data=last_cache_data
            )
        else:
            data, cache_data = self._get(
                "/{0}/{1}/tcz/sizelist".format(version, arch), last_cache_data=last_cache_data
            )
            data = data.text
        size_map = {}
        for line in data.split("\n"):
            if line.strip():
                pkg, size = line.split(" ", 1)
                size_map[pkg] = int(size)
        return size_map, cache_data

    def get_tags_list(self, version, arch, compressed=True, last_cache_data=None):
        if compressed:
            data, cache_data = self._get_gzip_text(
                "/{0}/{1}/tcz/tags.db.gz".format(version, arch), last_cache_data=last_cache_data
            )
        else:
            data, cache_data = self._get("/{0}/{1}/tcz/tags.db".format(version, arch))
            data = data.text
        tags_map = {}
        for line in data.split("\n"):
            if line.strip():
                pkg, *tags = line.split()
                tags_map[pkg] = list(tags)
        return tags_map, cache_data

    def get_provides_db(self, version, arch, compressed=True, last_cache_data=None):
        if compressed:
            data, cache_data = self._get_gzip_text(
                "/{0}/{1}/tcz/provides.db.gz".format(version, arch), last_cache_data=last_cache_data
            )
        else:
            data, cache_data = self._get(
                "/{0}/{1}/tcz/provides.db".format(version, arch), last_cache_data=last_cache_data
            )
            data = data.text
        provides_map = {}
        for group in data.split("\n\n"):
            pkg, *provides = group.split("\n")
            if pkg:
                provides_map[pkg] = list(provides)
        return provides_map

    def get_file(self, version, arch, file_name, output_file=None, last_cache_data=None):
        response, cache_data = self._get(
            "/{0}/{1}/tcz/{2}".format(version, arch, file_name),
            stream=output_file is not None,
            last_cache_data=last_cache_data
        )
        if output_file is not None:
            for chunk in response.iter_content(64*1024):
                output_file.write(chunk)
            return None, cache_data
        else:
            return response.content, cache_data
