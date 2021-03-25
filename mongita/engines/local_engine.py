import fcntl
import os
import shutil

import bson

from ..errors import MongitaError
from .engine_common import Engine, StorageObject


class LocalEngine(Engine):
    def __init__(self, base_storage_path):
        if not os.path.exists(base_storage_path):
            print("making directory 1", base_storage_path)
            os.mkdir(base_storage_path)
        self.base_storage_path = base_storage_path
        self._cache = {}

    def _get_gen(self, full_path):
        return int(os.path.getmtime(full_path) * 1000000)

    def _get_full_path(self, location):
        return os.path.join(self.base_storage_path, location.path)

    def create_path(self, location):
        loc_parent = location.rsplit('/', 1)[0]
        full_loc = self._get_full_path(loc_parent)
        if not os.path.exists(full_loc):
            parts = []
            for part in loc_parent.split('/'):
                parts.append(part)
                path = self._get_full_path(os.path.join(*parts))
                if not os.path.exists(path):
                    print("making directory 2", path)
                    os.mkdir(path)

    def upload_doc(self, location, doc, generation=None):
        full_path = self._get_full_path(location)
        data = bson.dumps(doc)

        print("uploading", location, doc, generation)

        if generation and self.doc_exists(location):
            with open(full_path, 'rb+') as f:
                fcntl.lockf(f, fcntl.LOCK_EX)
                if self._get_gen(full_path) > generation:
                    fcntl.lockf(f, fcntl.LOCK_UN)
                    return False
                f.seek(0)
                f.write(data)
                fcntl.lockf(f, fcntl.LOCK_UN)
            return True

        with open(full_path, 'wb') as f:
            fcntl.lockf(f, fcntl.LOCK_EX)
            f.write(data)
            fcntl.lockf(f, fcntl.LOCK_UN)
        return True

    def download_doc(self, location):
        full_path = self._get_full_path(location)
        if not os.path.exists(full_path):
            raise MongitaError("Document %r does not exist" % location)

        doc = self._cache.get(location)
        if doc and doc.generation == self._get_gen(full_path):
            return doc

        with open(full_path, 'rb+') as f:
            fcntl.lockf(f, fcntl.LOCK_EX)
            generation = self._get_gen(full_path)
            doc = f.read()
            fcntl.lockf(f, fcntl.LOCK_UN)

        so = StorageObject(bson.loads(doc), generation)
        self._cache[location] = so
        return so

    def delete_doc(self, location):
        full_path = self._get_full_path(location)
        try:
            os.remove(full_path)
        except FileNotFoundError:
            return False
        try:
            del self._cache[location]
        except KeyError:
            pass
        return True

    def delete_dir(self, location):
        full_path = self._get_full_path(location)
        if not os.path.isdir(full_path):
            return False
        try:
            shutil.rmtree(full_path)
        except OSError:
            return False
        for k in self._cache.keys():
            if k.startswith(location.path):
                del self._cache[k]
        return True

    def doc_exists(self, location):
        full_path = self._get_full_path(location)
        return os.path.exists(full_path)

    def list_ids(self, prefix, limit=None):
        full_path = self._get_full_path(prefix)

        if not os.path.exists(full_path):
            return []

        ret = os.listdir(full_path)
        if limit:
            ret = ret[:limit]
        return ret
