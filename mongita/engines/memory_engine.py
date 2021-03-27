import datetime
import threading

from ..common import StorageObject, MetaStorageObject
from .engine_common import Engine


class MemoryEngine(Engine):
    def __init__(self):
        self._storage = {}
        self._lock = threading.RLock()

    def upload_doc(self, location, doc, if_gen_match=False):
        assert isinstance(doc, StorageObject)  # TODO remove
        if if_gen_match:
            with self._lock:
                so = self.download_doc(location)
                if so and so.generation != doc.generation:
                    print("gen!=", so.generation, doc.generation)
                    return False
                self._storage[location] = doc.to_bytes()
                return True
        self._storage[location] = doc.to_bytes()
        return True

    def upload_metadata(self, location, doc):
        assert isinstance(doc, MetaStorageObject)  # TODO remove
        print("uploading_metadata", doc)
        with self._lock:
            so_tup = self.download_metadata(location)
            if so_tup and so_tup[0].generation != doc.generation:
                print("gen!=", so_tup[0].generation, doc.generation)
                return False
            self._storage[location] = (doc.to_bytes(), datetime.datetime.utcnow())
        return True

    def download_metadata(self, location):
        try:
            obj = self._storage[location]
        except KeyError:
            return None
        ret = MetaStorageObject.from_bytes(obj[0]), (datetime.datetime.utcnow() - obj[1]).total_seconds()
        print("download_metadata", ret)
        return ret

    def touch_metadata(self, location):
        try:
            self._storage[location][1] = datetime.datetime.utcnow()
        except KeyError:
            return False
        return True

    def download_doc(self, location):
        try:
            obj = self._storage[location]
        except KeyError:
            return None
        return StorageObject.from_bytes(obj)

    def doc_exists(self, location):
        return location in self._storage

    def list_ids(self, prefix, limit=None):
        ret = []
        if limit is None:
            for k in self._storage.keys():
                if k.is_in_collection(prefix):
                    ret.append(k._id)
            return ret
        for k in self._storage.keys():
            if k.is_in_collection(prefix):
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

    def create_path(self, location):
        pass
