import bson

from .engine_common import Engine, StorageObject


class MemoryEngine(Engine):
    def __init__(self):
        self._storage = {}

    def upload_doc(self, location, doc, generation=None):
        self._storage[location] = bson.dumps(doc)
        return True

    def download_doc(self, location):
        try:
            obj = self._storage[location]
        except KeyError:
            return None
        return StorageObject(bson.loads(obj), None)

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
        del self._storage[location]
        return True
        # TODO errors for storage failures

    def delete_dir(self, location):
        for k in list(self._storage.keys()):
            if k.is_in_collection_incl_metadata(location):
                del self._storage[k]
        return True

    def create_path(self, location):
        pass
