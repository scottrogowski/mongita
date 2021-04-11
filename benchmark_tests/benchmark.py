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
import plotly.graph_objects as go

from tinymongo import TinyMongoClient
from montydb import MontyClient

NOW_TS = int(datetime.now().timestamp())

# if isinstance(cli, mongita.MongitaClientDisk):
#     pr = cProfile.Profile()
#     pr.enable()

# if isinstance(cli, mongita.MongitaClientDisk):
#     pr.disable()
#     import io, pstats
#     s = io.StringIO()
#     sortby = pstats.SortKey.CUMULATIVE
#     ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
#     ps.print_stats()
#     print(s.getvalue())

def get_doc():
    return {
        '_id': str(bson.ObjectId()),
        'name': coolname.generate_slug(),
        'dt': datetime.fromtimestamp(random.randint(0, NOW_TS)),
        'count': random.randint(0, 5000),
        'city': random.choice(('Philly', 'Santa Fe', 'Reno')),
        'content': ' '.join(lorem.paragraph() for _ in range(3)),
        'percent': random.random(),
        'dict': {
            'name': coolname.generate_slug(),
            'count': random.randint(0, 5000),
            'elements': ['a', 'b', 'c'],
        },
    }


def get_docs(cnt):
    return [get_doc() for _ in range(cnt)]


def to_sqlite_row(doc):
    doc['_id'] = str(doc['_id'])
    return (doc['_id'], doc['name'], doc['dt'], doc['count'], doc['city'],
            doc['content'], doc['percent'],
            json.dumps(doc['dict'], default=json_util.default))


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
        # cur.execute("""DROP TABLE  docs;""")
        # self.con.commit()
        cur.execute("""CREATE TABLE IF NOT EXISTS docs (idd TEXT, name TEXT, dt DATETIME, count int, city TEXT, content TEXT, percent REAL, dict JSON);""")
        self.con.commit()
        self.cur = self.con.cursor()

    def insert_many(self, documents):
        cur = self.con.cursor()
        cur.executemany('INSERT INTO docs VALUES(?,?,?,?,?,?,?,?);', [to_sqlite_row(d) for d in documents])
        self.con.commit()
        cur.execute("SELECT count() from docs")
        print("sqlite count", cur.fetchone()[0])

    def find_one(self, filter):
        idd = filter['_id']
        self.cur.execute("SELECT * from docs WHERE idd=(?) LIMIT 1;", (idd,))
        return self.cur.fetchone()

    # def find(self, filter):
    #     cur = self.con.cursor()
    #     if not filter:
    #         cur.execute("SELECT doc FROM docs")
    #         ret = [json.loads(r[0]) for r in cur.fetchall()]
    #         return ret
    #     if filter == {'city': 'Reno'}:
    #         cur.execute("SELECT doc FROM docs where json_extract(doc, '$.city')='Reno'")
    #         ret = [json.loads(r[0]) for r in cur.fetchall()]
    #         return ret
    #     if filter == {'percent': {'$lt': .33}}:
    #         cur.execute("SELECT doc FROM docs where json_extract(doc, '$.percent')<.33")
    #         ret = [json.loads(r[0]) for r in cur.fetchall()]
    #         return ret
    #     raise AssertionError(filter)

    def find(self, filter):
        cur = self.con.cursor()
        if not filter:
            cur.execute("SELECT * FROM docs")
            ret = list(cur.fetchall())
            return ret
        if filter == {'city': 'Reno'}:
            cur.execute("SELECT * FROM docs where city='Reno'")
            ret = list(cur.fetchall())
            return ret
        if filter == {'percent': {'$lt': .33}}:
            cur.execute("SELECT * FROM docs where percent<.33")
            ret = list(cur.fetchall())
            return ret
        raise AssertionError(filter)

    def update_many(self, filter, update):
        cur = self.con.cursor()
        content = update['$set']['content']
        if filter['city'] == 'Reno':
            cur.execute(f"UPDATE docs SET content='{content}' where city='Reno';")
            return True
        elif filter['city'] == 'Philly':
            cur.execute(f"UPDATE docs SET content='{content}' where city='Philly';")
            return True
        raise AssertionError(filter)

    def create_index(self, index_name):
        pass


def benchmark():
    print("Generating 10,000 random docs (about 1KB each)...")
    docs = get_docs(10000)
    print("Got docs. Average bson length=%.3f" % sum(len(bson.encode(doc)) for doc in docs)/10000)

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
    print("Wrote 10k docs in %.3f (bson)" % (time.perf_counter() - start_write_10k))

def test_write_open_file_json(insert_docs):
    with open('/tmp/10kdocs.json', 'w') as f:
        start_write_10k = time.perf_counter()
        f.write(json.dumps({'docs': insert_docs}))
    print("Wrote 10k docs in %.3f (json)" % (time.perf_counter() - start_write_10k))

def test_write_open_file_msgpack(insert_docs):
    with open('docs.msgpack', 'wb') as f:
        start_write_10k = time.perf_counter()
        f.write(msgpack.dumps({'docs': insert_docs}))
    print("Wrote 10k docs in %.3f (msgpack)" % (time.perf_counter() - start_write_10k))

def test_write_individual_docs(insert_docs):
    shutil.rmtree('/tmp/docs/')
    os.mkdir('/tmp/docs/')
    start_write_10k = time.perf_counter()
    for doc in insert_docs:
        with open(f'/tmp/docs/{doc["name"]}.bson', 'wb') as f:
            f.write(bson.encode(doc))
    print("Wrote 10k docs individually in %.3f" % (time.perf_counter() - start_write_10k))

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
    print("Wrote 10k docs individually APPEND in %.3f" % (time.perf_counter() - start_write_10k))


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
    print("Wrote 10k docs sqlite in %.3f" % (time.perf_counter() - start_write_10k))


class Timer:
    def __init__(self, stats, name):
        self.stats = stats
        self.name = name
        print(f"Running {name}...")

    def __enter__(self):
        self.start = time.perf_counter()

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.stats[self.name] = time.perf_counter() - self.start

def bm():
    print("Generating 10,000 random docs (about 1KB each)...")
    insert_docs = get_docs(10000)
    insert_doc_ids = [str(d['_id']) for d in insert_docs]
    avg_len = sum(len(bson.encode(doc)) for doc in insert_docs) / 10000
    print("Got docs. Average bson length=%.3f" % avg_len)
    print()

    test_write_open_file_bson(insert_docs)
    # test_write_open_file_json(insert_docs)
    # test_write_open_file_msgpack(insert_docs)
    # test_write_individual_docs(insert_docs)
    # test_write_append_docs(insert_docs)
    # test_write_sqlite_docs(insert_docs)
    clients = {
        'Mongita Memory': mongita.MongitaClientMemory,
        'Mongita Disk': functools.partial(mongita.MongitaClientDisk, '/tmp/mongita_benchmarks'),
        'Sqlite': SqliteWrapper,
        'MongoDB+PyMongo': pymongo.MongoClient,
    }

    all_stats = {}
    for cli_name, cli_cls in clients.items():
        print("\nRunning loop for %s" % cli_name)
        stats = {}
        all_stats[cli_name] = stats
        cli = cli_cls()
        try:
            cli.drop_database('bm')
        except:
            print("Could not drop database for %s" % cli)


        with Timer(stats, "Insert 10k"):
            cli.bm.bm.insert_many(insert_docs)

        with Timer(stats, "Retrieve all"):
            retrieved_docs = list(cli.bm.bm.find({}))
        assert len(retrieved_docs) == len(insert_docs)

        with Timer(stats, "Access 1000 random elements"):
            list(cli.bm.bm.find_one({'_id': _id})
                 for _id in random.sample(insert_doc_ids, 1000))

        with Timer(stats, "Find categorically (~1/3 of documents)"):
            list(cli.bm.bm.find({'city': 'Reno'}))

        with Timer(stats, "Find by float comparison (~1/3 of documents)"):
            list(cli.bm.bm.find({'percent': {'$lt': .33}}))

        cli.bm.bm.create_index('city')
        cli.bm.bm.create_index('percent')

        with Timer(stats, "Find categorically (~1/3 of documents) (indexed)"):
            list(cli.bm.bm.find({'city': 'Reno'}))

        with Timer(stats, "Find by float comparison (~1/3 of documents) (indexed)"):
            list(cli.bm.bm.find({'percent': {'$lt': .33}}))

        with Timer(stats, "Update text content (~1/3 of documents twice) (indexed)"):
            assert cli.bm.bm.update_many(
                {'city': 'Reno'},
                {'$set': {'content': ' '.join(lorem.paragraph() for _ in range(1))}})
            assert cli.bm.bm.update_many(
                {'city': 'Philly'},
                {'$set': {'content': ' '.join(lorem.paragraph() for _ in range(5))}})


        if isinstance(cli, mongita.MongitaClientMemory):
            print()
            print(cli)
            print('=' * 20)
            for k, v in stats.items():
                print("%s: %.3f" % (k, v))
            continue

        if isinstance(cli, pymongo.MongoClient):
            assert os.system('brew services restart mongodb-community') == 0
        cli = cli_cls()
        if cli.engine._cache:
            print('\a'); import ipdb; ipdb.set_trace()

        with Timer(stats, "Retrieve all (cold)"):
            print("cold retrieve")
            retrieved_docs = list(cli.bm.bm.find({}))
        assert len(retrieved_docs) == len(insert_docs)

        with Timer(stats, "Access 1000 random elements (cold)"):
            list(cli.bm.bm.find_one({'_id': _id})
                 for _id in random.sample(insert_doc_ids, 1000))

        with Timer(stats, "Find categorically (~1/3 of documents) (cold)"):
            list(cli.bm.bm.find({'city': 'Reno'}))

        with Timer(stats, "Find by float comparison (~1/3 of documents) (cold)"):
            list(cli.bm.bm.find({'percent': {'$lt': .33}}))

        cli.bm.bm.create_index('city')
        cli.bm.bm.create_index('percent')

        with Timer(stats, "Find categorically (~1/3 of documents) (indexed) (cold)"):
            list(cli.bm.bm.find({'city': 'Reno'}))

        with Timer(stats, "Find by float comparison (~1/3 of documents) (indexed) (cold)"):
            list(cli.bm.bm.find({'percent': {'$lt': .33}}))

        with Timer(stats, "Update text content (~1/3 of documents twice) (indexed) (cold)"):
            assert cli.bm.bm.update_many(
                {'city': 'Reno'},
                {'$set': {'content': ' '.join(lorem.paragraph() for _ in range(1))}})
            assert cli.bm.bm.update_many(
                {'city': 'Philly'},
                {'$set': {'content': ' '.join(lorem.paragraph() for _ in range(5))}})

        print()
        print(cli)
        print('=' * 20)
        for k, v in stats.items():
            print("%s: %.3f" % (k, v))

    dat = []
    for stat_name, stat_dict in all_stats.items():
        dat.append(go.Bar(name=stat_name, x=list(stat_dict.keys()), y=list(stat_dict.values())))
    fig = go.Figure(data=dat)
    fig.update_layout(barmode='group')
    fig.show()


if __name__ == '__main__':
    bm()

