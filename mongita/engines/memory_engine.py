import collections
import itertools
import threading
from sys import intern as itrn

import bson

from ..common import MetaStorageObject
from .engine_common import Engine


class MemoryEngine(Engine):
    def __init__(self, strict=False):
        self._strict = strict
        self._cache = collections.defaultdict(dict)
        self._metadata = {}
        self.lock = threading.RLock()

    @staticmethod
    def create(strict=False):
        return MemoryEngine(strict)

    def put_doc(self, collection, doc, no_overwrite=False):
        doc_id = str(doc['_id'])
        if no_overwrite and doc_id in self._cache[collection]:
            return False
        if self._strict:
            self._cache[itrn(collection)][itrn(doc_id)] = bson.encode(doc)
        else:
            self._cache[itrn(collection)][itrn(doc_id)] = doc
        return True

    def get_doc(self, collection, doc_id):
        obj = self._cache[collection].get(str(doc_id))
        if self._strict and obj:
            return bson.decode(obj)
        return obj

    def doc_exists(self, collection, doc_id):
        return str(doc_id) in self._cache[collection]

    def list_ids(self, collection, limit=None):
        keys = self._cache.get(collection, {}).keys()
        if limit is None:
            return list(keys)
        return list(itertools.islice(keys, limit))

    def delete_doc(self, collection, doc_id):
        self._cache[collection].pop(str(doc_id), None)
        return True

    def delete_dir(self, collection):
        with self.lock:
            self._cache.pop(collection, None)
            self._metadata.pop(collection, None)

        return True

    def put_metadata(self, collection, doc):
        self._metadata[collection] = doc.to_storage(as_bson=self._strict)
        return True

    def get_metadata(self, collection):
        try:
            obj = self._metadata[collection]
        except KeyError:
            return None
        return MetaStorageObject.from_storage(obj, from_bson=self._strict)

    def create_path(self, collection):
        pass

    def close(self):
        self._cache = collections.defaultdict(dict)
        self._metadata = {}
