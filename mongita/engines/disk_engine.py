import collections
import itertools
import os
import pathlib
import shutil
import threading
from sys import intern as itrn

import bson

from ..common import MetaStorageObject, secure_filename
from .engine_common import Engine


class DiskEngine(Engine):
    def __init__(self, base_storage_path):
        if not os.path.exists(base_storage_path):
            os.mkdir(base_storage_path)
        self.base_storage_path = base_storage_path
        self._cache = collections.defaultdict(dict)
        self._collection_fhs = {}
        self._metadata = {}
        self._loc_idx = collections.defaultdict(dict)
        self.lock = threading.RLock()

    def _get_full_path(self, collection, filename=''):
        return os.path.join(*list(filter(None, (self.base_storage_path,
                                                secure_filename(collection),
                                                filename))))

    def _get_coll_fh(self, collection):
        try:
            return self._collection_fhs[collection]
        except KeyError:
            pass
        data_path = self._get_full_path(collection, '$.data')
        if not os.path.exists(data_path):
            pathlib.Path(data_path).touch()
        fh = open(data_path, 'rb+')
        fh.at_end = False
        self._collection_fhs[itrn(collection)] = fh
        return fh

    def _get_loc_idx(self, collection):
        if collection in self._loc_idx:
            return self._loc_idx[collection]

        loc_idx_path = self._get_full_path(collection, '$.loc_idx')
        try:
            with open(loc_idx_path, 'rb') as f:
                self._loc_idx[itrn(collection)] = bson.decode(f.read())
        except FileNotFoundError:
            self._loc_idx[itrn(collection)] = {}
        return self._loc_idx[collection]

    def _set_loc_idx(self, collection, doc_id, pos):
        if pos is None:
            self._loc_idx[itrn(collection)].pop(doc_id, None)
        else:
            self._loc_idx[itrn(collection)][itrn(doc_id)] = pos

    def doc_exists(self, collection, doc_id):
        if str(doc_id) in self._get_loc_idx(collection):
            return True
        return False

    def get_doc(self, collection, doc_id):
        doc_id = str(doc_id)
        try:
            return self._cache[itrn(collection)][itrn(doc_id)]
        except KeyError:
            pass

        pos = self._get_loc_idx(collection)[str(doc_id)]
        fh = self._get_coll_fh(collection)
        fh.seek(pos)
        fh.at_end = False
        first_byte = fh.read(4)
        doc_len = int.from_bytes(first_byte, 'little', signed=True)
        doc = bson.decode(first_byte + fh.read(doc_len - 4))
        self._cache[itrn(collection)][itrn(doc_id)] = doc
        return doc

    def put_doc(self, collection, doc, no_overwrite=False):
        doc_id = str(doc['_id'])
        if no_overwrite and self.doc_exists(collection, doc_id):
            return False
        self._cache[itrn(collection)][itrn(doc_id)] = doc

        encoded_doc = bson.encode(doc)
        fh = self._get_coll_fh(collection)
        pos = self._get_loc_idx(collection).get(str(doc_id))
        if pos is not None:
            fh.seek(pos)
            fh.at_end = False
            first_byte = fh.read(4)
            spare_bytes = int.from_bytes(first_byte, 'little', signed=True) - len(encoded_doc)
            if spare_bytes >= 0:
                # TODO, need to rewrite when document gets too sparse
                fh.seek(pos)
                fh.write(encoded_doc + b'\x00' * spare_bytes)
                fh.flush()
                return True
        # if not fh.at_end: # TODO
        fh.seek(0, 2)
        fh.at_end = True
        pos = fh.tell()
        fh.write(encoded_doc)
        fh.flush()
        self._set_loc_idx(collection, doc_id, pos)
        return True

    def delete_doc(self, collection, doc_id):
        doc_id = str(doc_id)
        pos = self._get_loc_idx(collection)[doc_id]
        fh = self._get_coll_fh(collection)
        fh.seek(pos)
        fh.at_end = False
        first_byte = fh.read(4)
        doc_len = int.from_bytes(first_byte, 'little', signed=True)
        fh.seek(pos)
        fh.write(b'\x00' * doc_len)
        fh.flush()
        self._set_loc_idx(collection, doc_id, None)
        self._cache[collection].pop(doc_id, None)
        return True

    def get_metadata(self, collection):
        try:
            return self._metadata[collection]
        except KeyError:
            pass

        metadata_path = self._get_full_path(collection, '$.metadata')
        try:
            with open(metadata_path, 'rb') as f:
                metadata = MetaStorageObject.from_storage(f.read(), from_bson=True)
        except FileNotFoundError:
            return None
        self._metadata[collection] = metadata
        return metadata

    # TODO disaster recovery rebuilds
    # def _rebuild_metadata(self, coll_path):
    #     fh = self._get_coll_fh(coll_path)
    #     pos = 0
    #     fh.seek(0)
    #     docs = []
    #     while True:
    #         first_byte = fh.read(4)
    #         if not first_byte:
    #             break
    #         doc_len = int.from_bytes(first_byte, 'little', signed=True)
    #         if not doc_len:
    #             continue
    #         doc = bson.decode(first_byte + fh.read(doc_len - 4))
    #         docs.append((pos, doc))
    #         pos += doc_len

    #     metadata = {}
    #     for pos, doc in docs:
    #         metadata['loc_idx'][doc['_id']] = pos
    #         self._cache[coll_path][doc['_id']] = doc
    #     self._metadata[coll_path] = metadata
    #     return metadata

    def put_metadata(self, collection, metadata):
        self._metadata[itrn(collection)] = metadata
        metadata_path = self._get_full_path(collection, '$.metadata')
        with open(metadata_path, 'wb') as f:
            f.write(metadata.to_storage(as_bson=True))
            f.flush()
        loc_idx_path = self._get_full_path(collection, '$.loc_idx')
        with open(loc_idx_path, 'wb') as f:
            f.write(bson.encode(self._loc_idx.get(collection, {})))
            f.flush()
        return True

    def delete_dir(self, collection):
        full_path = self._get_full_path(collection)
        if not os.path.isdir(full_path):
            return False
        shutil.rmtree(full_path)
        self._cache.pop(collection, None)
        self._metadata.pop(collection, None)
        self._loc_idx.pop(collection, None)
        return True

    def list_ids(self, collection, limit=None):
        keys = self._get_loc_idx(collection).keys()
        if limit is None:
            return list(map(str, keys))
        return list(map(str, itertools.islice(keys, limit)))

    def create_path(self, collection):
        full_loc = self._get_full_path(collection)
        if not os.path.exists(full_loc):
            os.makedirs(full_loc)

    def close(self):
        self._cache = collections.defaultdict(dict)
        self._metadata = {}
        self._loc_idx = {}
        for fh in self._collection_fhs.values():
            fh.close()
        self._collection_fhs = {}
