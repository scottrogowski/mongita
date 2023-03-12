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

DISK_ENGINE_INCUMBENTS = {}


class DiskEngine(Engine):
    def __init__(self, base_storage_path):
        if not os.path.exists(base_storage_path):
            os.mkdir(base_storage_path)
        self.base_storage_path = base_storage_path
        self._cache = collections.defaultdict(dict)
        self._collection_fhs = {}
        self._metadata = {}
        self._file_attrs = collections.defaultdict(dict)
        self.replaced = False
        self.lock = threading.RLock()

    @staticmethod
    def create(base_storage_path):
        if base_storage_path in DISK_ENGINE_INCUMBENTS:
            DISK_ENGINE_INCUMBENTS[base_storage_path].close()
            return DISK_ENGINE_INCUMBENTS[base_storage_path]
        de = DiskEngine(base_storage_path)
        DISK_ENGINE_INCUMBENTS[base_storage_path] = de
        return de

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
            # self.create_path(collection)
            pathlib.Path(data_path).touch()
        fh = open(data_path, 'rb+')
        self._collection_fhs[itrn(collection)] = fh
        return fh

    def _get_file_attrs(self, collection):
        if collection in self._file_attrs:
            return self._file_attrs[collection]['loc_idx']

        file_attrs_path = self._get_full_path(collection, '$.file_attrs')
        try:
            with open(file_attrs_path, 'rb') as f:
                self._file_attrs[itrn(collection)] = bson.decode(f.read())
        except FileNotFoundError:
            self._file_attrs[itrn(collection)] = {
                'loc_idx': {},
                'spare_bytes': 0,
                'total_bytes': 0
            }
        return self._file_attrs[collection]['loc_idx']

    def _set_file_attrs(self, collection, doc_id, pos):
        if pos is None:
            self._file_attrs[itrn(collection)]['loc_idx'].pop(doc_id, None)
        else:
            self._file_attrs[itrn(collection)]['loc_idx'][itrn(doc_id)] = pos

    def doc_exists(self, collection, doc_id):
        if str(doc_id) in self._get_file_attrs(collection):
            return True
        return False

    def get_doc(self, collection, doc_id):
        doc_id = str(doc_id)
        try:
            return self._cache[itrn(collection)][itrn(doc_id)]
        except KeyError:
            pass

        pos = self._get_file_attrs(collection)[str(doc_id)]
        fh = self._get_coll_fh(collection)
        fh.seek(pos)
        doc_len_bytes = fh.read(4)
        doc_len = int.from_bytes(doc_len_bytes, 'little', signed=True)
        assert doc_len
        doc = bson.decode(doc_len_bytes + fh.read(doc_len - 4))
        self._cache[itrn(collection)][itrn(doc_id)] = doc
        return doc

    def put_doc(self, collection, doc, no_overwrite=False):
        doc_id = str(doc['_id'])
        if no_overwrite and self.doc_exists(collection, doc_id):
            return False
        self._cache[itrn(collection)][itrn(doc_id)] = doc

        encoded_doc = bson.encode(doc)
        fh = self._get_coll_fh(collection)
        pos = self._get_file_attrs(collection).get(str(doc_id))
        if pos is not None:
            fh.seek(pos)
            doc_len_bytes = fh.read(4)
            doc_len = int.from_bytes(doc_len_bytes, 'little', signed=True)
            assert doc_len
            spare_bytes = doc_len - len(encoded_doc)
            if spare_bytes >= 0:
                fh.seek(pos)
                fh.write(encoded_doc + b'\x00' * spare_bytes)
                fh.flush()
                self._file_attrs[collection]['spare_bytes'] += spare_bytes
                self._file_attrs[collection]['total_bytes'] -= spare_bytes
                return True
        fh.seek(0, 2)
        pos = fh.tell()
        fh.write(encoded_doc)
        fh.flush()
        self._set_file_attrs(collection, doc_id, pos)
        self._file_attrs[collection]['total_bytes'] += len(encoded_doc)
        return True

    def delete_doc(self, collection, doc_id):
        doc_id = str(doc_id)
        pos = self._get_file_attrs(collection)[doc_id]
        fh = self._get_coll_fh(collection)
        fh.seek(pos)
        doc_len_bytes = fh.read(4)
        doc_len = int.from_bytes(doc_len_bytes, 'little', signed=True)
        assert doc_len
        fh.seek(pos)
        fh.write(b'\x00' * doc_len)
        fh.flush()
        self._set_file_attrs(collection, doc_id, None)
        self._file_attrs[collection]['total_bytes'] -= doc_len
        self._file_attrs[collection]['spare_bytes'] += doc_len
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
    #         doc_len_bytes = fh.read(4)
    #         if not doc_len_bytes:
    #             break
    #         doc_len = int.from_bytes(doc_len_bytes, 'little', signed=True)
    #         if not doc_len:
    #             continue
    #         doc = bson.decode(doc_len_bytes + fh.read(doc_len - 4))
    #         docs.append((pos, doc))
    #         pos += doc_len

    #     metadata = {}
    #     for pos, doc in docs:
    #         metadata['file_attrs'][doc['_id']] = pos
    #         self._cache[coll_path][doc['_id']] = doc
    #     self._metadata[coll_path] = metadata
    #     return metadata
    def _defrag(self, collection):
        fh = self._get_coll_fh(collection)
        encoded_docs = {}
        _cache_collection = self._cache[itrn(collection)]
        for doc_id in self.list_ids(collection):
            try:
                doc = _cache_collection[itrn(doc_id)]
                encoded_docs[doc_id] = bson.encode(doc)
            except KeyError:
                pos = self._get_file_attrs(collection)[str(doc_id)]
                fh = self._get_coll_fh(collection)
                fh.seek(pos)
                doc_len_bytes = fh.read(4)
                doc_len = int.from_bytes(doc_len_bytes, 'little', signed=True)
                assert doc_len
                encoded_docs[doc_id] = doc_len_bytes + fh.read(doc_len - 4)

        pos = 0
        fh.seek(0)
        for doc_id, encoded_doc in encoded_docs.items():
            fh.write(encoded_doc)
            self._set_file_attrs(collection, doc_id, pos)
            pos += len(encoded_doc)
        fh.truncate()

    def put_metadata(self, collection, metadata):
        self._metadata[itrn(collection)] = metadata
        metadata_path = self._get_full_path(collection, '$.metadata')
        with open(metadata_path, 'wb') as f:
            f.write(metadata.to_storage(as_bson=True))
        file_attrs_path = self._get_full_path(collection, '$.file_attrs')
        if self._file_attrs.get(collection, {}).get('spare_bytes', 0) / \
           (1 + self._file_attrs.get(collection, {}).get('total_bytes', 0)) > 0.5:
            self._defrag(collection)
        with open(file_attrs_path, 'wb') as f:
            f.write(bson.encode(self._file_attrs.get(collection, {'total_bytes': 0,
                                                                  'spare_bytes': 0,
                                                                  'loc_idx': {}})))
        return True

    def delete_dir(self, collection):
        full_path = self._get_full_path(collection)
        if not os.path.isdir(full_path):
            return False
        shutil.rmtree(full_path)
        self._cache.pop(collection, None)
        self._metadata.pop(collection, None)
        self._file_attrs.pop(collection, None)
        if collection in self._collection_fhs:
            self._collection_fhs[collection].close()
            self._collection_fhs.pop(collection, None)
        return True

    def list_ids(self, collection, limit=None):
        keys = self._get_file_attrs(collection).keys()
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
        self._file_attrs = {}
        for fh in self._collection_fhs.values():
            fh.close()
        self._collection_fhs = {}
