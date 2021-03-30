import fcntl
import pathlib
import os
import shutil
import time

import bson

from ..common import StorageObject, MetaStorageObject, int_from_bytes
from .engine_common import Engine

# TODO https://filelock.readthedocs.io/en/latest/


def _get_mod_time(full_path):
    return int(os.path.getmtime(full_path) * 1000000)


class DiskEngine(Engine):
    def __init__(self, base_storage_path, single_client=False):
        # TODO single_client
        if not os.path.exists(base_storage_path):
            os.mkdir(base_storage_path)
        self.base_storage_path = base_storage_path
        self._cache = {}

    def _get_full_path(self, location):
        # TODO assert path is not relative.
        return os.path.join(self.base_storage_path, location.path)

    def upload_doc(self, location, doc, if_gen_match=False):
        full_path = self._get_full_path(location)

        if if_gen_match and self.doc_exists(location):
            with open(full_path, 'rb+') as f:
                fcntl.lockf(f, fcntl.LOCK_EX)
                existing_generation = int_from_bytes(f.read(8))
                if existing_generation > doc.generation:
                    fcntl.lockf(f, fcntl.LOCK_UN)
                    return False
                f.seek(0)
                f.write(doc.to_storage(True))  # TODO existing_generation + 1
                fcntl.lockf(f, fcntl.LOCK_UN)
            self._cache[location] = doc
            return True

        with open(full_path, 'wb') as f:
            fcntl.lockf(f, fcntl.LOCK_EX)
            f.write(doc.to_storage(True))
            fcntl.lockf(f, fcntl.LOCK_UN)
            self._cache[location] = doc
        return True

    def upload_metadata(self, location, doc):
        return self.upload_doc(location, doc, if_gen_match=True)

    def download_metadata(self, location):
        full_path = self._get_full_path(location)
        if not os.path.exists(full_path):
            return None

        doc_from_cache = self._cache.get(location)
        with open(full_path, 'rb+') as f:
            fcntl.lockf(f, fcntl.LOCK_EX)
            mod_time = _get_mod_time(full_path)
            existing_generation = int_from_bytes(f.read(8))
            if doc_from_cache and doc_from_cache.generation == existing_generation:
                return doc_from_cache, time.time() - mod_time
            doc = bson.decode(f.read())
            fcntl.lockf(f, fcntl.LOCK_UN)

        so = MetaStorageObject(doc, existing_generation)
        so.decode_indexes()
        self._cache[location] = so
        return so, time.time() - mod_time

    def touch_metadata(self, location):
        full_path = self._get_full_path(location)
        pathlib.Path(full_path).touch()
        return True

    def download_doc(self, location):
        full_path = self._get_full_path(location)
        if not os.path.exists(full_path):
            return None

        doc_from_cache = self._cache.get(location)
        with open(full_path, 'rb+') as f:
            fcntl.lockf(f, fcntl.LOCK_EX)
            generation = int_from_bytes(f.read(8))
            if doc_from_cache and doc_from_cache.generation == generation:
                return doc_from_cache
            doc = bson.decode(f.read())
            fcntl.lockf(f, fcntl.LOCK_UN)

        so = StorageObject(doc, generation)
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
        for k in list(self._cache.keys()):
            if k.is_in_collection_incl_metadata(location):
                del self._cache[k]
        return True

    def doc_exists(self, location):
        full_path = self._get_full_path(location)
        return os.path.exists(full_path)

    def list_ids(self, collection_location, limit=None):
        assert collection_location.is_collection()

        full_path = self._get_full_path(collection_location)
        if not os.path.exists(full_path):
            return []

        ret = []
        if limit is None:
            for _id in os.listdir(full_path):
                if not _id.startswith('$'):
                    ret.append(_id)
            return ret
        i = 0
        for _id in os.listdir(full_path):
            if not _id.startswith('$'):
                ret.append(_id)
                i += 1
                if i == limit:
                    break
        return ret

    def create_path(self, location):
        full_loc = os.path.join(self.base_storage_path, location.parent_path())
        if not os.path.exists(full_loc):
            os.makedirs(full_loc)

    def close(self):
        self._cache = {}
