import collections
import itertools
import os
import pathlib
import shutil
import threading
from sys import intern as itrn
import sqlite3

import bson
import json
import copy
import datetime

from ..common import MetaStorageObject, secure_filename
from .engine_common import Engine

OPEN_ENGINE_INCUMBENTS = {}

def json_default(obj):
    if isinstance(obj, bson.ObjectId):
        return {"MongitaObjectId": str(obj)}
    elif isinstance(obj, datetime.datetime):
        return {"MongitaDateTime": obj.isoformat()}
    else:
        return obj

def json_object_hook(obj):
    if "MongitaObjectId" in obj:
        return bson.ObjectId(obj["MongitaObjectId"])
    elif "MongitaDateTime" in obj:
        return datetime.datetime.fromisoformat(obj["MongitaDateTime"])
    else:
        return obj

def mkparam(obj):
    if isinstance(obj, bson.ObjectId):
        return str(obj)
    elif isinstance(obj, datetime.datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        # TODO. Not sure if this will work
        return str(obj)
    else:
        return obj

def dumps(jobj):
    # json.dumps but convert bson.ObjectId to str and datetime to str
    return json.dumps(jobj, default=json_default)

def loads(jstr):
    # json.loads but convert str to bson.ObjectId and str to datetime
    return json.loads(jstr, object_hook=json_object_hook)


class SqliteEngine(Engine):
    def __init__(self, base_storage_path):
        if not os.path.exists(base_storage_path):
            os.mkdir(base_storage_path)
        self.base_storage_path = base_storage_path
        self._dbs = {}
        # self.con = sqlite3.connect(os.path.join(base_storage_path, 'mongita.db'))
        # self.cur = self.con.cursor()
        # self._cache = collections.defaultdict(dict)
        # self._collection_fhs = {}
        # self._metadata = {}
        # self._file_attrs = collections.defaultdict(dict)
        # self.replaced = False
        self.lock = threading.RLock()

    @staticmethod
    def create(base_storage_path):
        if base_storage_path in OPEN_ENGINE_INCUMBENTS:
            OPEN_ENGINE_INCUMBENTS[base_storage_path].close()
            return OPEN_ENGINE_INCUMBENTS[base_storage_path]
        se = SqliteEngine(base_storage_path)
        OPEN_ENGINE_INCUMBENTS[base_storage_path] = se
        return se

    # def _get_full_path(self, collection, filename=''):
    #     return os.path.join(*list(filter(None, (self.base_storage_path,
    #                                             secure_filename(collection),
    #                                             filename))))

    # def _get_coll_fh(self, collection):
    #     try:
    #         return self._collection_fhs[collection]
    #     except KeyError:
    #         pass
    #     data_path = self._get_full_path(collection, '$.data')
    #     if not os.path.exists(data_path):
    #         # self.create_path(collection)
    #         pathlib.Path(data_path).touch()
    #     fh = open(data_path, 'rb+')
    #     self._collection_fhs[itrn(collection)] = fh
    #     return fh

    # def _get_file_attrs(self, collection):
    #     if collection in self._file_attrs:
    #         return self._file_attrs[collection]['loc_idx']

    #     file_attrs_path = self._get_full_path(collection, '$.file_attrs')
    #     try:
    #         with open(file_attrs_path, 'rb') as f:
    #             self._file_attrs[itrn(collection)] = bson.decode(f.read())
    #     except FileNotFoundError:
    #         self._file_attrs[itrn(collection)] = {
    #             'loc_idx': {},
    #             'spare_bytes': 0,
    #             'total_bytes': 0
    #         }
    #     return self._file_attrs[collection]['loc_idx']

    # def _set_file_attrs(self, collection, doc_id, pos):
    #     if pos is None:
    #         self._file_attrs[itrn(collection)]['loc_idx'].pop(doc_id, None)
    #     else:
    #         self._file_attrs[itrn(collection)]['loc_idx'][itrn(doc_id)] = pos

    def cur(self, dbname):
        try:
            return self._dbs[dbname]
        except KeyError:
            # create a directory at the base storage path
            try:
                os.mkdir(self.base_storage_path)
            except FileExistsError:
                pass

            conn = sqlite3.connect(os.path.join(self.base_storage_path, dbname + '.db'))
            cur = conn.cursor()
            self._dbs[dbname] = (conn, cur)
            return conn, cur

    def doc_exists(self, collection, doc_id):
        doc_id = str(doc_id)
        dbname, tablename = collection.split('.', 1)
        _, cur = self.cur(dbname)

        # check if the table exists
        cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{tablename}'")
        if not cur.fetchone():
            return False

        # TODO injection risk
        cur.execute(f'SELECT count(*) FROM {tablename} WHERE _id = ?', (doc_id,))
        return cur.fetchone()[0] > 0

    def get_doc(self, collection, doc_id):
        dbname, tablename = collection.split('.', 1)
        _, cur = self.cur(dbname)
        doc_id = str(doc_id)
        cur.execute(f'SELECT * FROM {tablename} WHERE _id = ?', (doc_id,))
        return loads(cur.fetchone()[1])

    def put_doc(self, collection, doc, no_overwrite=False):
        dbname, tablename = collection.split('.', 1)
        doc_id = str(doc['_id'])
        if no_overwrite and self.doc_exists(collection, doc_id):
            return False
        con, cur = self.cur(dbname)
        cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{tablename}'")
        if not cur.fetchone():
            cur.execute(f'CREATE TABLE {tablename} (_id TEXT PRIMARY KEY, data BLOB)')
        # TODO json or bson?
        print("inserting", doc_id, dumps(doc))
        cur.execute(f'INSERT INTO {tablename} VALUES (?, ?)', (doc_id, dumps(doc)))
        con.commit()
        return True

        # if no_overwrite and self.doc_exists(collection, doc_id):
        #     return False
        # self._cache[itrn(collection)][itrn(doc_id)] = doc

        # encoded_doc = bson.encode(doc)
        # fh = self._get_coll_fh(collection)
        # pos = self._get_file_attrs(collection).get(str(doc_id))
        # if pos is not None:
        #     fh.seek(pos)
        #     doc_len_bytes = fh.read(4)
        #     doc_len = int.from_bytes(doc_len_bytes, 'little', signed=True)
        #     assert doc_len
        #     spare_bytes = doc_len - len(encoded_doc)
        #     if spare_bytes >= 0:
        #         fh.seek(pos)
        #         fh.write(encoded_doc + b'\x00' * spare_bytes)
        #         fh.flush()
        #         self._file_attrs[collection]['spare_bytes'] += spare_bytes
        #         self._file_attrs[collection]['total_bytes'] -= spare_bytes
        #         return True
        # fh.seek(0, 2)
        # pos = fh.tell()
        # fh.write(encoded_doc)
        # fh.flush()
        # self._set_file_attrs(collection, doc_id, pos)
        # self._file_attrs[collection]['total_bytes'] += len(encoded_doc)
        # return True

    def delete_doc(self, collection, doc_id):
        con, cur = self.cur(collection)
        doc_id = str(doc_id)
        cur.execute('DELETE FROM ? WHERE _id = ?', (collection, doc_id))
        con.commit()
        return True

        # pos = self._get_file_attrs(collection)[doc_id]
        # fh = self._get_coll_fh(collection)
        # fh.seek(pos)
        # doc_len_bytes = fh.read(4)
        # doc_len = int.from_bytes(doc_len_bytes, 'little', signed=True)
        # assert doc_len
        # fh.seek(pos)
        # fh.write(b'\x00' * doc_len)
        # fh.flush()
        # self._set_file_attrs(collection, doc_id, None)
        # self._file_attrs[collection]['total_bytes'] -= doc_len
        # self._file_attrs[collection]['spare_bytes'] += doc_len
        # self._cache[collection].pop(doc_id, None)
        # return True

    def get_metadata(self, collection):
        return None
        # try:
        #     return self._metadata[collection]
        # except KeyError:
        #     pass

        # metadata_path = self._get_full_path(collection, '$.metadata')
        # try:
        #     with open(metadata_path, 'rb') as f:
        #         metadata = MetaStorageObject.from_storage(f.read(), from_bson=True)
        # except FileNotFoundError:
        #     return None
        # self._metadata[collection] = metadata
        # return metadata


    def put_metadata(self, collection, metadata):
        # self._metadata[itrn(collection)] = metadata
        # metadata_path = self._get_full_path(collection, '$.metadata')
        # with open(metadata_path, 'wb') as f:
        #     f.write(metadata.to_storage(as_bson=True))
        # file_attrs_path = self._get_full_path(collection, '$.file_attrs')
        # if self._file_attrs.get(collection, {}).get('spare_bytes', 0) / \
        #    (1 + self._file_attrs.get(collection, {}).get('total_bytes', 0)) > 0.5:
        #     self._defrag(collection)
        # with open(file_attrs_path, 'wb') as f:
        #     f.write(bson.encode(self._file_attrs.get(collection, {'total_bytes': 0,
        #                                                           'spare_bytes': 0,
        #                                                           'loc_idx': {}})))
        return True

    def delete_dir(self, collection):
        # full_path = self._get_full_path(collection)
        # if not os.path.isdir(full_path):
        #     return False
        # shutil.rmtree(full_path)
        # self._cache.pop(collection, None)
        # self._metadata.pop(collection, None)
        # self._file_attrs.pop(collection, None)
        # if collection in self._collection_fhs:
        #     self._collection_fhs[collection].close()
        #     self._collection_fhs.pop(collection, None)
        return True

    def list_ids(self, collection, limit=None):
        dbname, tablename = collection.split('.', 1)
        con, cur = self.cur(dbname)
        cur.execute(f'SELECT _id FROM {tablename}')
        return [row[0] for row in cur.fetchall()]

        # keys = self._get_file_attrs(collection).keys()
        # if limit is None:
        #     return list(map(str, keys))
        # return list(map(str, itertools.islice(keys, limit)))

    def list_ids_filter(self, collection, filter, sort=None, limit=None, skip=None):
        dbname, tablename = collection.split('.', 1)
        _, cur = self.cur(dbname)

        print("FILTER", filter)
        print("LIMIT", limit)
        print("SKIP", skip)
        print("SORT", sort)

        # get columns
        cur.execute(f'PRAGMA table_info({tablename})')
        columns = [row[1] for row in cur.fetchall()]
        columns = [c for c in columns if c != 'data']


        where = []
        params = []

        k_dict = {}

        if not filter:
            where = []
        else:
            for k, query_ops in filter.items():
                if k not in columns:
                    k = f'json_extract({tablename}.data, "$.{k}")'

                if isinstance(query_ops, (str, bson.ObjectId)):
                    where = [f'{k} = ?']
                    params.append(mkparam(query_ops))
                    continue

                if any(k.startswith('$') for k in query_ops.keys()):
                    for op, val in query_ops.items():
                        if op == '$eq':
                            where.append(f'{k} = ?')
                        elif op == '$ne':
                            where.append(f'{k} != ?')
                        elif op == '$gt':
                            where.append(f'{k} > ?')
                        elif op == '$gte':
                            where.append(f'{k} >= ?')
                        elif op == '$lt':
                            where.append(f'{k} < ?')
                        elif op == '$lte':
                            where.append(f'{k} <= ?')
                        elif op == '$in':
                            where.append(f'{k} IN ({",".join(["?"]*len(val))})')
                        elif op == '$nin':
                            where.append(f'{k} NOT IN ({",".join(["?"]*len(val))})')
                        else:
                            raise ValueError(f'Unknown operator {op}')

                        if op in ['$eq', '$ne', '$gt', '$gte', '$lt', '$lte']:
                            params.append(mkparam(val))
                        elif op in ['$in', '$nin']:
                            params += [mkparam(v) for v in val]
                else:
                    where.append(f'{k} = ?')
                    params.append(mkparam(query_ops))

        print("WHERE", where)
        print("PARAMS", params)

        query = f'SELECT _id FROM {tablename}'

        if where:
            where = " AND ".join(where)
            query += f' WHERE {where}'
        if sort:
            sorters = []
            for k, direction in sort:
                if k not in columns:
                    k = f'json_extract({tablename}.data, "$.{k}")'
                sorters.append(f'{k} {"ASC" if direction == 1 else "DESC"}')
            query += f' ORDER BY {",".join(sorters)}'
        if limit is not None:
            query += f' LIMIT {limit}'
        if skip:
            if limit is None:
                query += f' LIMIT -1'
            query += f' OFFSET {skip}'
        print("QUERY IS", query)

        cur.execute(query, params)
        ret = [row[0] for row in cur.fetchall()]
        print("RET", ret)
        return ret

        # keys = self._get_file_attrs(collection).keys()
        # if limit is None:
        #     return list(map(str, keys))
        # return list(map(str, itertools.islice(keys, limit)))

    def create_path(self, collection):
        # full_loc = self._get_full_path(collection)
        # if not os.path.exists(full_loc):
        #     os.makedirs(full_loc)
        pass

    def close(self):
        # self._cache = collections.defaultdict(dict)
        # self._metadata = {}
        # self._file_attrs = {}
        # for fh in self._collection_fhs.values():
        #     fh.close()
        # self._collection_fhs = {}
        while self._dbs:
            _, con = self._dbs.popitem()
            con[0].close()
