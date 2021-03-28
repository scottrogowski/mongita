import threading
import time

from ..common import StorageObject
from .engine_common import Engine


class MemoryEngine(Engine):
    def __init__(self, strict=False):
        self._strict = strict
        self._storage = {}
        self._lock = threading.RLock()

    def upload_doc(self, location, doc, if_gen_match=False):
        if if_gen_match:
            with self._lock:
                so = self.download_doc(location)
                if so and so.generation != doc.generation:
                    return False
                self._storage[location] = doc.to_storage(self._strict)
                return True
        self._storage[location] = doc.to_storage(self._strict)
        return True

    def download_doc(self, location):
        try:
            obj = self._storage[location]
        except KeyError:
            return None
        return StorageObject.from_storage(obj, self._strict)

    def doc_exists(self, location):
        return location in self._storage

    def list_ids(self, collection_location, limit=None):
        assert collection_location.is_collection()
        ret = []
        if limit is None:
            for k in self._storage.keys():
                if k.is_in_collection(collection_location):
                    ret.append(k._id)
            return ret
        for k in self._storage.keys():
            if k.is_in_collection(collection_location):
                ret.append(k._id)
                if len(ret) == limit:
                    break
        return ret

    def delete_doc(self, location):
        try:
            del self._storage[location]
            return True
        except KeyError:
            return False

    def delete_dir(self, location):
        with self._lock:
            for k in list(self._storage.keys()):
                if k.is_in_collection_incl_metadata(location):
                    del self._storage[k]
        return True

    def upload_metadata(self, location, doc):
        with self._lock:
            so_tup = self.download_metadata(location)
            if so_tup and so_tup[0].generation != doc.generation:
                return False
            self._storage[location] = (doc.to_storage(self._strict), time.time())
        return True

    def download_metadata(self, location):
        try:
            obj, modified = self._storage[location]
        except KeyError:
            return None
        obj = StorageObject.from_storage(obj, self._strict)
        return obj, time.time() - modified

    def touch_metadata(self, location):
        try:
            self._storage[location] = (self._storage[location][0], time.time())
        except KeyError:
            return False
        return True

    def create_path(self, location):
        pass
