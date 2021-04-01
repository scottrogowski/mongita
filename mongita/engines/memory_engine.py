import threading

from ..common import StorageObject, MetaStorageObject
from .engine_common import Engine


class MemoryEngine(Engine):
    def __init__(self, strict=False):
        self._strict = strict
        self._cache = {}
        self._lock = threading.RLock()

    def upload_doc(self, location, doc, if_gen_match=False):
        if if_gen_match:
            with self._lock:
                so = self.download_doc(location)
                if so and so.generation != doc.generation:
                    return False
                self._cache[location] = doc.to_storage(strict=self._strict)
                return True
        self._cache[location] = doc.to_storage(strict=self._strict)
        return True

    def download_doc(self, location):
        try:
            obj = self._cache[location]
        except KeyError:
            return None
        return StorageObject.from_storage(obj, strict=self._strict)

    def doc_exists(self, location):
        return location in self._cache

    def list_ids(self, collection_location, limit=None):
        assert collection_location.is_collection()
        ret = []
        if limit is None:
            for k in self._cache.keys():
                if k.is_in_collection(collection_location):
                    ret.append(k._id)
            return ret
        i = 0
        for k in self._cache.keys():
            if k.is_in_collection(collection_location):
                ret.append(k._id)
                i += 1
                if i == limit:
                    break
        return ret

    def delete_doc(self, location):
        del self._cache[location]
        return True

    def delete_dir(self, location):
        with self._lock:
            for k in list(self._cache.keys()):
                if k.is_in_collection_incl_metadata(location):
                    del self._cache[k]
        return True

    def upload_metadata(self, location, doc):
        with self._lock:
            so_tup = self.download_metadata(location)
            if so_tup:
                assert so_tup.generation == doc.generation
            self._cache[location] = doc.to_storage(strict=self._strict)
        return True

    def download_metadata(self, location):
        try:
            obj = self._cache[location]
        except KeyError:
            return None
        obj = MetaStorageObject.from_storage(obj, strict=self._strict)
        return obj

    def create_path(self, location):
        pass

    def close(self):
        self._cache = {}
