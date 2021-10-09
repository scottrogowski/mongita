#!/usr/bin/env python3

import cProfile
from datetime import datetime
import functools
import json
import os
import random
import pstats
import shutil
import time

import sqlite3

import bson
from bson import json_util
import coolname
import msgpack
import pymongo
import lorem
import plotly.graph_objects as go

import mongita


NOW_TS = int(datetime.now().timestamp())

CLIENT_COLORS = {
    'Mongita Memory': '#D8A753',
    'Mongita Disk': '#82592C',
    'Sqlite (traditional)': '#0B3647',
    'Sqlite (JSON1)': '#439FD8',
    'MongoDB+PyMongo': '#449B45',
}


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


def _to_sqlite_row(doc):
    doc['_id'] = str(doc['_id'])
    return (doc['_id'], doc['name'], doc['dt'], doc['count'], doc['city'],
            doc['content'], doc['percent'],
            json.dumps(doc['dict'], default=json_util.default))


def _to_sqlite_row_json(doc):
    doc['_id'] = str(doc['_id'])
    return (doc['_id'], json.dumps(doc['dict'], default=json_util.default))


SQLITE_LOC = 'tmp.sqlite'


class SqliteWrapper():
    def __init__(self, use_json):
        self._sqlite_database_wrapper = None
        self.use_json = use_json

    def __getattr__(self, attr):
        if self._sqlite_database_wrapper:
            return self._sqlite_database_wrapper
        self._sqlite_database_wrapper = SqliteDatabaseWrapper(self.use_json)
        return self._sqlite_database_wrapper

    def drop_database(self, db):
        try:
            os.remove(SQLITE_LOC)
        except FileNotFoundError:
            pass


class SqliteDatabaseWrapper():
    def __init__(self, use_json):
        self._sqlite_collection_wrapper = None
        self.use_json = use_json

    def __getattr__(self, attr):
        if self._sqlite_collection_wrapper:
            return self._sqlite_collection_wrapper
        if self.use_json:
            self._sqlite_collection_wrapper = SqliteCollectionWrapperJson()
        else:
            self._sqlite_collection_wrapper = SqliteCollectionWrapper()
        return self._sqlite_collection_wrapper


class SqliteCollectionWrapperJson():
    def __init__(self):
        self.con = sqlite3.connect(SQLITE_LOC)
        cur = self.con.cursor()
        # cur.execute("""DROP TABLE  docs;""")
        # self.con.commit()
        cur.execute("""CREATE TABLE IF NOT EXISTS docs (idd TEXT, dict JSON, PRIMARY KEY(idd));""")
        self.con.commit()
        self.cur = self.con.cursor()

    def insert_many(self, documents):
        cur = self.con.cursor()
        cur.executemany('INSERT INTO docs VALUES(?,?);', [_to_sqlite_row_json(d) for d in documents])
        self.con.commit()
        cur.execute("SELECT count() from docs")

    def find_one(self, filter):
        idd = filter['_id']
        self.cur.execute("SELECT dict from docs WHERE idd=(?) LIMIT 1;", (idd,))
        return json.loads(self.cur.fetchone()[0])

    def find(self, filter):
        cur = self.con.cursor()
        if not filter:
            cur.execute("SELECT dict FROM docs")
            return [json.loads(row[0]) for row in cur.fetchall()]
        if filter == {'city': 'Reno'}:
            cur.execute("SELECT idd, dict FROM docs where json_extract(dict, '$.city') = 'Reno'")
            return [json.loads(row[1]) for row in cur.fetchall()]
        if filter == {'percent': {'$lt': .33}}:
            cur.execute("SELECT idd, dict FROM docs where json_extract(dict, '$.value') < 0.33")
            return [json.loads(row[1]) for row in cur.fetchall()]
        raise AssertionError(filter)

    def update_many(self, filter, update):
        cur = self.con.cursor()
        content = update['$set']['content']
        if filter['city'] == 'Reno':
            cur.execute(f"UPDATE docs SET dict=(select json_set(docs.dict, '$.content', '{content}') from docs) where json_extract(dict, '$.city')='Reno';")
            return True
        elif filter['city'] == 'Philly':
            cur.execute(f"UPDATE docs SET dict=(select json_set(docs.dict, '$.content', '{content}') from docs) where json_extract(dict, '$.city')='Philly';")
            return True
        raise AssertionError(filter)

    def delete_many(self, filter):
        assert filter == {}
        cur = self.con.cursor()
        cur.execute("DELETE FROM docs where true=true;")
        return True

    def create_index(self, index_name):
        # cur = self.con.cursor()
        # cur.execute(f"CREATE INDEX {index_name}_idx ON docs({index_name})")
        return True


class SqliteCollectionWrapper():
    def __init__(self):
        self.con = sqlite3.connect(SQLITE_LOC)
        cur = self.con.cursor()
        # cur.execute("""DROP TABLE  docs;""")
        # self.con.commit()
        cur.execute("""CREATE TABLE IF NOT EXISTS docs (idd TEXT, name TEXT, dt DATETIME, count int, city TEXT, content TEXT, percent REAL, dict JSON, PRIMARY KEY(idd));""")
        self.con.commit()
        self.cur = self.con.cursor()

    def insert_many(self, documents):
        cur = self.con.cursor()
        cur.executemany('INSERT INTO docs VALUES(?,?,?,?,?,?,?,?);', [_to_sqlite_row(d) for d in documents])
        self.con.commit()
        cur.execute("SELECT count() from docs")

    def find_one(self, filter):
        idd = filter['_id']
        self.cur.execute("SELECT * from docs WHERE idd=(?) LIMIT 1;", (idd,))
        return self.cur.fetchone()

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

    def delete_many(self, filter):
        assert filter == {}
        cur = self.con.cursor()
        cur.execute("DELETE FROM docs where true=true;")
        return True

    def create_index(self, index_name):
        cur = self.con.cursor()
        cur.execute(f"CREATE INDEX {index_name}_idx ON docs({index_name})")
        return True


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
    def __init__(self, stats, name, profiler=False):
        self.stats = stats
        self.name = name
        self.do_profiler = profiler
        print(f"Running {name}...")

    def __enter__(self):
        if self.do_profiler:
            self.profiler = cProfile.Profile()
            self.profiler.enable()
        self.start = time.perf_counter()

    def __exit__(self, exc_type, exc_value, exc_tb):
        if self.do_profiler:
            self.profiler.disable()
            stats = pstats.Stats(self.profiler).sort_stats('cumtime')
            stats.print_stats()
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
        'Sqlite (traditional)': functools.partial(SqliteWrapper, use_json=False),
        'Sqlite (JSON1)': functools.partial(SqliteWrapper, use_json=True),
        'MongoDB+PyMongo': pymongo.MongoClient,
    }

    assert os.getcwd().split('/')[-1] == 'mongita' and os.path.exists('assets')

    all_stats = {}
    for cli_name, cli_cls in clients.items():
        print("\nRunning loop for %s" % cli_name)
        print('=' * 20)
        stats = {}
        all_stats[cli_name] = stats
        cli = cli_cls()
        try:
            cli.drop_database('bm')
            print("Dropped database for %s" % cli)
        except:
            print("Could not drop database for %s" % cli)

        with Timer(stats, "Insert 10k"):
            cli.bm.bm.insert_many(insert_docs)

        with Timer(stats, "Retrieve all documents"):
            retrieved_docs = list(cli.bm.bm.find({}))
        assert len(retrieved_docs) == len(insert_docs)

        with Timer(stats, "Get 1000 docs by id"):
            list(cli.bm.bm.find_one({'_id': _id})
                 for _id in random.sample(insert_doc_ids, 1000))

        with Timer(stats, "Find categorically (~3300 docs)"):
            list(cli.bm.bm.find({'city': 'Reno'}))

        with Timer(stats, "Find numerically (~3300 docs)"):
            list(cli.bm.bm.find({'percent': {'$lt': .33}}))

        if 'JSON1' not in cli_name:
            cli.bm.bm.create_index('city')
            cli.bm.bm.create_index('percent')

            with Timer(stats, "Find categorically (~3300 docs) (indexed)"):
                list(cli.bm.bm.find({'city': 'Reno'}))

            with Timer(stats, "Find numerically (~3300 docs) (indexed)"):
                list(cli.bm.bm.find({'percent': {'$lt': .33}}))

        # TODO updating is the one thing that is embarassingly slow.
        # need to find optimizations in it
        with Timer(stats, "Update text field (~3300 docs x2) (indexed)"):
            assert cli.bm.bm.update_many(
                {'city': 'Reno'},
                {'$set': {'content': ' '.join(lorem.paragraph() for _ in range(1))}})
            assert cli.bm.bm.update_many(
                {'city': 'Philly'},
                {'$set': {'content': ' '.join(lorem.paragraph() for _ in range(5))}})

        if not isinstance(cli, mongita.MongitaClientMemory):

            if isinstance(cli, pymongo.MongoClient):
                assert os.system('brew services restart mongodb-community') == 0
            cli = cli_cls()

            with Timer(stats, "Retrieve all documents "):
                print("cold retrieve")
                retrieved_docs = list(cli.bm.bm.find({}))
            assert len(retrieved_docs) == len(insert_docs)

            if isinstance(cli, pymongo.MongoClient):
                assert os.system('brew services restart mongodb-community') == 0
            cli = cli_cls()

            with Timer(stats, "Get 1000 docs by id "):
                list(cli.bm.bm.find_one({'_id': _id})
                     for _id in random.sample(insert_doc_ids, 1000))

            if isinstance(cli, pymongo.MongoClient):
                assert os.system('brew services restart mongodb-community') == 0
            cli = cli_cls()

            with Timer(stats, "Find categorically (~3300 docs) "):
                list(cli.bm.bm.find({'city': 'Reno'}))

            if isinstance(cli, pymongo.MongoClient):
                assert os.system('brew services restart mongodb-community') == 0
            cli = cli_cls()

            with Timer(stats, "Find numerically (~3300 docs) "):
                list(cli.bm.bm.find({'percent': {'$lt': .33}}))

            if isinstance(cli, pymongo.MongoClient):
                assert os.system('brew services restart mongodb-community') == 0
            cli = cli_cls()

            with Timer(stats, "Update text field (~3300 docs x2) "):
                assert cli.bm.bm.update_many(
                    {'city': 'Reno'},
                    {'$set': {'content': ' '.join(lorem.paragraph() for _ in range(1))}})
                assert cli.bm.bm.update_many(
                    {'city': 'Philly'},
                    {'$set': {'content': ' '.join(lorem.paragraph() for _ in range(5))}})

        with Timer(stats, "Delete all documents"):
            assert cli.bm.bm.delete_many({})

        print()
        print("Final stats for", cli)
        for k, v in stats.items():
            print("%s: %.3f" % (k, v))
        print("\n")

    all_keys = list(all_stats['Mongita Disk'].keys())
    chart_types = {
        'Performance comparison: inserts and access': all_keys[:3],
        'Performance comparison: finds': all_keys[3:7],
        'Performance comparison: updates and deletes': all_keys[7:8] + all_keys[13:],
        'Performance comparison: cold starts': all_keys[8:13],
    }

    for chart_title, chart_keys in chart_types.items():
        dat = []
        for client_name, _stat_dict in all_stats.items():
            stat_dict = {k: v for k, v in _stat_dict.items() if k in chart_keys}
            dat.append(go.Bar(name=client_name,
                              x=list(stat_dict.keys()),
                              y=list(stat_dict.values()),
                              # text=[round(v, 3) for v in stat_dict.values()],
                              # textposition='outside',
                              marker_color=CLIENT_COLORS[client_name]))
        fig = go.Figure(data=dat)
        fig.update_layout(
            template='seaborn',
            title_text=chart_title,
            yaxis_title="Seconds",
            barmode='group',
            paper_bgcolor='rgba(240,240,240,120)',
            plot_bgcolor='rgba(240,240,240,120)',
            font_family="Garamond"
        )
        # fig.update_yaxes(range=[0, 1])
        fig.show()
        chart_title = chart_title.lower().replace(' ', '_').replace(':', '')
        fig.write_image(f"assets/{chart_title}.svg")


if __name__ == '__main__':
    bm()
