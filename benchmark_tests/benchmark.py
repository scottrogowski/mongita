#!/usr/bin/env python3

import cProfile

from datetime import datetime
import functools
import json
import os
import random
import shutil
import time

import sqlite3

import bson
from bson import json_util
import coolname
# import plotly
import msgpack
import mongita
import pymongo
import lorem

from tinymongo import TinyMongoClient
from montydb import MontyClient

NOW_TS = int(datetime.now().timestamp())


def get_doc():
    return {
        '_id': str(bson.ObjectId()),
        'name': coolname.generate_slug(),
        'dt': str(datetime.fromtimestamp(random.randint(0, NOW_TS))),
        'count': random.randint(0, 5000),
        'city': random.choice(('Philly', 'Santa Fe', 'Reno')),
        'content': ' '.join(lorem.paragraph() for _ in range(3)),
        'dict': {
            'name': coolname.generate_slug(),
            'count': random.randint(0, 5000),
            'elements': ['a', 'b', 'c'],
        },
        'percent': random.random(),
    }


def get_docs(cnt):
    return [get_doc() for _ in range(cnt)]


def to_sqlite_row(doc):
    doc = dict(doc)
    doc['_id'] = str(doc['_id'])
    return doc['_id'], json.dumps(doc, default=json_util.default)


def insert_many(client, docs):
    pass


def update_many(client):
    pass


def find_many(client):
    pass


SQLITE_LOC = 'tmp.sqlite'


class SqliteWrapper():
    def __init__(self):
        self._sqlite_database_wrapper = None

    def __getattr__(self, attr):
        if self._sqlite_database_wrapper:
            return self._sqlite_database_wrapper
        self._sqlite_database_wrapper = SqliteDatabaseWrapper()
        return self._sqlite_database_wrapper

    def drop_database(self, db):
        try:
            os.remove(SQLITE_LOC)
        except FileNotFoundError:
            pass

class SqliteDatabaseWrapper():
    def __init__(self):
        self._sqlite_collection_wrapper = None

    def __getattr__(self, attr):
        if self._sqlite_collection_wrapper:
            return self._sqlite_collection_wrapper
        self._sqlite_collection_wrapper = SqliteCollectionWrapper()
        return self._sqlite_collection_wrapper


class SqliteCollectionWrapper():
    def __init__(self):
        self.con = sqlite3.connect(SQLITE_LOC)
        cur = self.con.cursor()
        cur.execute("""DROP TABLE IF EXISTS docs;""")
        self.con.commit()
        cur.execute("""CREATE TABLE docs (idd TEXT, doc JSON);""")
        self.con.commit()
        self.cur = self.con.cursor()

    def insert_many(self, documents):
        cur = self.con.cursor()
        cur.executemany('INSERT INTO docs VALUES(?,?);', [to_sqlite_row(d) for d in documents])
        self.con.commit()
        cur.execute("SELECT count() from docs")
        print("sqlite count", cur.fetchone()[0])

    def find_one(self, filter):
        idd = filter['_id']
        self.cur.execute("SELECT doc from docs WHERE idd=(?) LIMIT 1;", (idd,))
        return self.cur.fetchone()

    def find(self, filter):
        cur = self.con.cursor()
        if not filter:
            cur.execute("SELECT doc FROM docs")
            ret = [json.loads(r[0]) for r in cur.fetchall()]
            return ret
        if filter == {'city': 'Reno'}:
            cur.execute("SELECT doc FROM docs where json_extract(doc, '$.city')='Reno'")
            ret = [json.loads(r[0]) for r in cur.fetchall()]
            return ret
        if filter == {'percent': {'$lt': .33}}:
            cur.execute("SELECT doc FROM docs where json_extract(doc, '$.percent')<.33")
            ret = [json.loads(r[0]) for r in cur.fetchall()]
            return ret
        raise AssertionError(filter)

    def create_index(self, index_name):
        pass


def benchmark():
    print("Generating 10,000 random docs (about 1KB each)...")
    docs = get_docs(10000)
    print("Got docs. Average bson length=%.2f" % sum(len(bson.encode(doc)) for doc in docs)/10000)

    clients = [mongita.MongitaClientMemory, mongita.MongitaClientDisk, pymongo.MongoClient]
    # for iteration in
    for client_cls in clients:
        client = client_cls()
        client_name = repr(client)
        print("Processing %s")

def test_write_open_file_bson(insert_docs):
    with open('/tmp/10kdocs.bson', 'wb') as f:
        start_write_10k = time.perf_counter()
        f.write(bson.encode({'docs': insert_docs}))
    print("Wrote 10k docs in %.2f (bson)" % (time.perf_counter() - start_write_10k))

def test_write_open_file_json(insert_docs):
    with open('/tmp/10kdocs.json', 'w') as f:
        start_write_10k = time.perf_counter()
        f.write(json.dumps({'docs': insert_docs}))
    print("Wrote 10k docs in %.2f (json)" % (time.perf_counter() - start_write_10k))

def test_write_open_file_msgpack(insert_docs):
    with open('docs.msgpack', 'wb') as f:
        start_write_10k = time.perf_counter()
        f.write(msgpack.dumps({'docs': insert_docs}))
    print("Wrote 10k docs in %.2f (msgpack)" % (time.perf_counter() - start_write_10k))

def test_write_individual_docs(insert_docs):
    shutil.rmtree('/tmp/docs/')
    os.mkdir('/tmp/docs/')
    start_write_10k = time.perf_counter()
    for doc in insert_docs:
        with open(f'/tmp/docs/{doc["name"]}.bson', 'wb') as f:
            f.write(bson.encode(doc))
    print("Wrote 10k docs individually in %.2f" % (time.perf_counter() - start_write_10k))

def test_write_append_docs(insert_docs):
    try:
        os.remove('/tmp/doc_append')
    except:
        pass
    with open('/tmp/doc_append', 'wb') as f:
        f.write(b'')
    start_write_10k = time.perf_counter()
    for doc in insert_docs:
        with open('/tmp/doc_append', 'ab') as f:
            f.write(bson.encode(doc))
    print("Wrote 10k docs individually APPEND in %.2f" % (time.perf_counter() - start_write_10k))


def test_write_sqlite_docs(insert_docs):
    try:
        os.remove('/tmp/tmp.sqlite')
    except:
        pass
    conn = sqlite3.connect('/tmp/tmp.sqlite')
    c = conn.cursor()
    c.execute("""DROP TABLE IF EXISTS docs;""")
    conn.commit()
    c.execute("""CREATE TABLE docs (idd TEXT, doc JSON);""")
    conn.commit()
    c = conn.cursor()
    start_write_10k = time.perf_counter()
    records = [(str(d['_id']), bson.encode(d)) for d in insert_docs]
    c.executemany('INSERT INTO docs VALUES(?,?);', records)
    conn.commit()
    conn.close()
    print("Wrote 10k docs sqlite in %.2f" % (time.perf_counter() - start_write_10k))


def bm():
    print("Generating 10,000 random docs (about 1KB each)...")
    insert_docs = get_docs(10000)
    insert_doc_ids = [str(d['_id']) for d in insert_docs]
    avg_len = sum(len(bson.encode(doc)) for doc in insert_docs) / 10000
    print("Got docs. Average bson length=%.2f" % avg_len)
    print()

    test_write_open_file_bson(insert_docs)
    # test_write_open_file_json(insert_docs)
    test_write_open_file_msgpack(insert_docs)
    # test_write_individual_docs(insert_docs)
    # test_write_append_docs(insert_docs)
    # test_write_sqlite_docs(insert_docs)
    clients = [
        # TinyMongoClient,
        # functools.partial(MontyClient, ":memory:"),
        mongita.MongitaClientMemory,
        functools.partial(mongita.MongitaClientDisk, '/tmp/mongita_benchmarks'),
        SqliteWrapper,
        pymongo.MongoClient,
    ]

    for cli in clients:
        cli = cli()
        print(cli)
        print('=' * 20)
        try:
            cli.drop_database('mongita_benchmark')
        except:
            pass


        start = time.perf_counter()
        cli.bm.bm.insert_many(insert_docs)
        print("insert: %.2f" % (time.perf_counter() - start))

        if isinstance(cli, mongita.MongitaClientDisk):
            pr = cProfile.Profile()
            pr.enable()

        start = time.perf_counter()
        list(cli.bm.bm.find({}))
        print("find all docs: %.2f" % (time.perf_counter() - start))

        if isinstance(cli, mongita.MongitaClientDisk):
            pr.disable()
            import io, pstats
            s = io.StringIO()
            sortby = pstats.SortKey.CUMULATIVE
            ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
            ps.print_stats()
            print(s.getvalue())


        start = time.perf_counter()
        list(cli.bm.bm.find_one({'_id': _id})
             for _id in random.sample(insert_doc_ids, 1000))
        print("find_one 1000 random elements: %.2f" % (time.perf_counter() - start))


        start = time.perf_counter()
        list(cli.bm.bm.find({'city': 'Reno'}))
        print("find category to list: %.2f" % (time.perf_counter() - start))



        start = time.perf_counter()
        list(cli.bm.bm.find({'percent': {'$lt': .33}}))
        print("find float to list: %.2f" % (time.perf_counter() - start))

        cli.bm.bm.create_index('city')
        cli.bm.bm.create_index('percent')

        start = time.perf_counter()
        list(cli.bm.bm.find({'city': 'Reno'}))
        print("find category to list (indexed): %.2f" % (time.perf_counter() - start))

        start = time.perf_counter()
        list(cli.bm.bm.find({'percent': {'$lt': .33}}))
        print("find float to list (indexed): %.2f" % (time.perf_counter() - start))
        print()


if __name__ == '__main__':
    bm()

