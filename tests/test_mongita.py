from datetime import datetime, date
import functools
from numbers import Number
import os
import random
import sys
import shutil
from concurrent.futures import ThreadPoolExecutor

import bson
import pytest

sys.path.append(os.getcwd().split('/tests')[0])

import mongita
from mongita import (MongitaClientMemory, MongitaClientDisk, ASCENDING, DESCENDING,
                     errors, results, cursor, collection, command_cursor, database,
                     engines, read_concern, write_concern)
random.seed(42)

TEST_DOCS = [
    {
        'name': 'Meercat',
        'family': 'Herpestidae',
        'kingdom': 'mammal',
        'spotted': datetime(1994, 4, 23),
        'weight': 0.75,
        'val': random.random(),
        'continents': ['AF'],
        'attrs': {
            'colors': ['brown'],
            'species': 'Suricata suricatta'
        }
    },
    {
        'name': 'Indian grey mongoose',
        'family': 'Herpestidae',
        'kingdom': 'mammal',
        'spotted': datetime(2003, 12, 1),
        'weight': 1.4,
        'val': random.random(),
        'continents': ['EA'],
        'attrs': {
            'colors': ['grey'],
            'species': 'Herpestes edwardsii'
        }
    },
    {
        'name': 'Honey Badger',
        'family': 'Mustelidae',
        'kingdom': 'mammal',
        'spotted': datetime(1987, 2, 13),
        'weight': 10.0,
        'val': random.random(),
        'continents': ['EA', 'AF'],
        'attrs': {
            'colors': ['grey', 'black'],
            'species': 'Mellivora capensis'
        }
    },
    {
        'name': 'King Cobra',
        'family': 'Elapidae',
        'kingdom': 'reptile',
        'spotted': datetime(1997, 2, 22),
        'weight': 6.0,
        'val': random.random(),
        'continents': ['EA'],
        'attrs': {
            'colors': ['black', 'grey'],
            'species': 'Ophiophagus hannah'
        }
    },
    {
        'name': 'Secretarybird',
        'family': 'Sagittariidae',
        'kingdom': 'bird',
        'spotted': datetime(1992, 7, 3),
        'weight': 4.0,
        'val': random.random(),
        'continents': ['AF'],
        'attrs': {
            'colors': ['white', 'black'],
            'species': 'Sagittarius serpentarius'
        }
    },
    {
        'name': 'Human',
        'family': 'Hominidae',
        'kingdom': 'mammal',
        'spotted': datetime(2021, 3, 25),
        'weight': 70.0,
        'val': random.random(),
        'continents': ['NA', 'SA', 'EA', 'AF'],
        'attrs': {
            'colors': 'brown',
            'species': 'Homo sapien'
        }
    },
    {
        'name': 'Placeholder',
        'spotted': datetime(2021, 3, 25),
    },
    {
        'name': 'Placeholder2',
        'spotted': datetime(2021, 3, 25),
        'weight': 'catweight',
        'kingdom': 'cat',
    },
]

TEST_DIR = '/tmp/mongita_unittests'
_MongitaClientDisk = functools.partial(MongitaClientDisk, TEST_DIR)
LEN_TEST_DOCS = len(TEST_DOCS)
CLIENTS = (MongitaClientMemory, _MongitaClientDisk)


def remove_test_dir():
    if os.path.exists(TEST_DIR):
        shutil.rmtree(TEST_DIR)


def setup_one(client_class):
    remove_test_dir()
    assert not os.path.exists(TEST_DIR)
    client = client_class()
    coll = client.db.snake_hunter
    ior = coll.insert_one(TEST_DOCS[0])
    assert '_id' not in TEST_DOCS[0]
    return client, coll, ior


def setup_many(client_class):
    remove_test_dir()
    assert not os.path.exists(TEST_DIR)
    client = client_class()
    coll = client.db.snake_hunter
    imr = coll.insert_many(TEST_DOCS)
    assert not any(['_id' in d for d in TEST_DOCS])
    return client, coll, imr


@pytest.mark.parametrize("client_class", CLIENTS)
def test_insert_one(client_class):
    client, coll, ior = setup_one(client_class)

    # document required
    with pytest.raises(TypeError):
        coll.insert_one()

    # kwarg support_alert
    with pytest.raises(errors.MongitaError):
        coll.insert_one({'doc': 'doc'}, bypass_document_validation=True)
    assert isinstance(ior, results.InsertOneResult)
    assert isinstance(repr(ior), str)
    assert isinstance(ior.inserted_id, bson.ObjectId)

    assert coll.count_documents({}) == 1
    assert coll.count_documents({'_id': ior.inserted_id}) == 1
    assert coll.find_one()['_id'] == ior.inserted_id

    td = dict(TEST_DOCS[1])
    td['_id'] = 'uniqlo'
    ior = coll.insert_one(td)
    assert ior.inserted_id == 'uniqlo'
    assert coll.count_documents({'_id': 'uniqlo'}) == 1
    assert coll.count_documents({}) == 2

    td = dict(TEST_DOCS[1])
    td['_id'] = bson.ObjectId()
    ior = coll.insert_one(td)
    assert ior.inserted_id == td['_id']
    assert coll.count_documents({'_id': td['_id']}) == 1
    assert coll.count_documents({}) == 3

    # already exists
    doc = coll.find_one()
    with pytest.raises(errors.PyMongoError):
        coll.insert_one(doc)


@pytest.mark.parametrize("client_class", CLIENTS)
def test_insert_many(client_class):
    client, coll, imr = setup_many(client_class)
    with pytest.raises(TypeError):
        coll.insert_many()
    with pytest.raises(errors.PyMongoError):
        coll.insert_many({'doc': 'doc'})
    assert isinstance(imr, results.InsertManyResult)
    assert isinstance(repr(imr), str)
    assert len(imr.inserted_ids) == LEN_TEST_DOCS
    assert all(isinstance(_id, bson.ObjectId) for _id in imr.inserted_ids)
    assert coll.count_documents({}) == LEN_TEST_DOCS
    assert coll.count_documents({'_id': imr.inserted_ids[0]}) == 1
    assert coll.count_documents({'_id': {'$in': imr.inserted_ids}}) == LEN_TEST_DOCS

    # Bad docs don't trigger. Ordered continues after errors
    coll.insert_one({'_id': 'id0'})
    with pytest.raises(errors.PyMongoError):
        imr = coll.insert_many([{'reason': 'duplicateid', '_id': 'id0'},
                                {'reason': 'ok', '_id': 'id1'}])
    assert not coll.find_one({'_id': 'id1'})
    with pytest.raises(errors.PyMongoError):
        imr = coll.insert_many([{'reason': 'duplicateid', '_id': 'id0'},
                                {'reason': 'ok', '_id': 'id2'}], ordered=False)
    assert coll.find_one({'_id': 'id2'})


@pytest.mark.parametrize("client_class", CLIENTS)
def test_count_documents_one(client_class):
    client, coll, ior = setup_one(client_class)
    with pytest.raises(TypeError):
        coll.count_documents()
    assert coll.count_documents({}) == 1
    assert coll.count_documents({'_id': ior.inserted_id}) == 1
    assert coll.count_documents({'_id': bson.ObjectId(str(ior.inserted_id))}) == 1
    assert coll.count_documents({'_id': 'abc'}) == 0
    assert coll.count_documents({'kingdom': 'mammal'}) == 1
    assert coll.count_documents({'kingdom': 'reptile'}) == 0
    assert coll.count_documents({'blah': 'blah'}) == 0


@pytest.mark.parametrize("client_class", CLIENTS)
def test_find_one(client_class):
    client, coll, imr = setup_many(client_class)
    with pytest.raises(errors.PyMongoError):
        coll.find_one(['hi'])
    with pytest.raises(errors.PyMongoError):
        coll.find_one({'weight': {'$bigger': 7}})
    with pytest.raises(errors.PyMongoError):
        coll.find_one({'weight': {'$bigger': 7}})
    with pytest.raises(errors.PyMongoError):
        coll.find_one({5: 'hi'})

    # comparison by dictionary is actually fine
    assert not coll.find_one({'weight': {'bigger': 7}})

    doc = coll.find_one()
    assert isinstance(doc, dict) #and not isinstance(doc, StorageObject)
    assert isinstance(doc['continents'], list)
    assert isinstance(doc['_id'], bson.objectid.ObjectId)
    del doc['_id']
    assert doc == [d for d in TEST_DOCS if d['name'] == doc['name']][0]

    doc = coll.find_one({'_id': imr.inserted_ids[1]})
    assert doc['name'] == 'Indian grey mongoose'
    assert doc['attrs']['colors'][0] == 'grey'
    assert doc['attrs']['species'].startswith('Herpestes')

    # nested find
    assert coll.find_one({'attrs.species': 'Homo sapien'})['name'] == 'Human'

    # sort
    assert coll.find_one({}, sort='name')['name'] == 'Honey Badger'
    assert coll.find_one({}, sort=[('name', DESCENDING)])['name'] == 'Secretarybird'

    # bson or str should both work for _id lookups
    doc = coll.find_one({'_id': str(imr.inserted_ids[1])})
    assert doc['name'] == 'Indian grey mongoose'

    doc = coll.find_one({'family': 'Herpestidae'})
    assert doc['name'] in ('Meercat', 'Indian grey mongoose')

    doc = coll.find_one({'family': 'matters'})
    assert doc is None

    doc = coll.find_one({'family': 'Herpestidae', 'weight': {'$lt': 1}})
    assert doc['name'] == 'Meercat'

    doc = coll.find_one({'family': 'Herpestidae', 'weight': {'$lt': 0}})
    assert not doc

    doc = coll.find_one({'family': 'Herpestidae', 'weight': {'$gt': 20}})
    assert not doc

    # _id must be string or ObjectId
    with pytest.raises(errors.PyMongoError):
        doc = coll.find_one({'_id': 5})


@pytest.mark.parametrize("client_class", CLIENTS)
def test_find(client_class):
    client, coll, imr = setup_many(client_class)
    doc_cursor = coll.find()
    assert isinstance(doc_cursor, cursor.Cursor)
    docs = list(doc_cursor)
    assert len(docs) == LEN_TEST_DOCS
    assert all(isinstance(doc, dict) for doc in docs)
    # assert all(not isinstance(doc, StorageObject) for doc in docs)
    assert all(isinstance(doc['_id'], bson.objectid.ObjectId) for doc in docs)
    docs = list(coll.find())

    doc_cursor = coll.find({'family': 'Herpestidae'})
    assert len(list(doc_cursor)) == 2
    assert all(doc['family'] == 'Herpestidae' for doc in doc_cursor)

    doc_cursor = coll.find({'family': 'Herpestidae', 'weight': {'$lt': 1}})
    assert len(list(doc_cursor)) == 1

    doc_cursor = coll.find({'family': 'Herpestidae', 'weight': {'$gt': 20}})
    assert len(list(doc_cursor)) == 0

    doc_cursor = coll.find({'family': 'Herpestidae', 'weight': {'$lt': 20}})
    assert len(list(doc_cursor)) == 2

    _sort_func = functools.partial(collection._sort_func, sort_key='weight')
    sorted_weight = [d.get('weight') for d in sorted(TEST_DOCS, key=_sort_func)]
    finds = list(coll.find({}, sort='weight', limit=3))
    assert [d.get('weight') for d in finds] == sorted_weight[:3]

    finds = list(coll.find({}, sort=[('weight', DESCENDING)], limit=3))
    assert [d.get('weight') for d in finds] == list(reversed(sorted_weight))[:3]

    # string limit
    with pytest.raises(TypeError):
        list(coll.find({}, sort=[('weight', DESCENDING)], limit='3'))

    # multiple ids
    finds = list(coll.find({'_id': {'$in': imr.inserted_ids + ['not_real_id']}}))
    assert len(finds) == LEN_TEST_DOCS


@pytest.mark.parametrize("client_class", CLIENTS)
def test_find_in_list(client_class):
    """Test for fix with find in list"""
    client, coll, imr = setup_many(client_class)
    assert coll.count_documents({'continents': 'EA'}) == 4
    assert coll.find_one({'continents': 'NA'})['name'] == "Human"

    client, coll, imr = setup_many(client_class)
    coll.create_index('continents')
    assert coll.count_documents({'continents': 'EA'}) == 4
    assert coll.find_one({'continents': 'NA'})['name'] == "Human"


@pytest.mark.parametrize("client_class", CLIENTS)
def test_cursor(client_class):
    client, coll, imr = setup_many(client_class)
    doc_cursor = coll.find()

    with pytest.raises(errors.MongitaNotImplementedError):
        doc_cursor.allow_disk_use()
    with pytest.raises(errors.MongitaNotImplementedError):
        doc_cursor.count()
    with pytest.raises(AttributeError):
        doc_cursor.made_up_method()

    # this should be implemented soonish
    with pytest.raises(errors.MongitaNotImplementedError):
        doc_cursor[0]

    assert isinstance(doc_cursor, cursor.Cursor)
    assert len(list(doc_cursor))
    assert not len(list(doc_cursor))  # cursor must be exhaused after one call

    # not asc or desc
    with pytest.raises(errors.PyMongoError):
        list(coll.find().sort('weight', 2))

    # pass in a string
    with pytest.raises(errors.PyMongoError):
        list(coll.find().sort('weight', "ASCENDING"))

    sorted_docs = list(coll.find().sort('weight'))
    assert sorted_docs[0]['name'] == 'Placeholder'
    assert sorted_docs[1]['name'] == 'Meercat'
    assert sorted_docs[-2]['name'] == 'Human'
    assert sorted_docs[-1]['name'] == 'Placeholder2'

    sorted_docs = list(coll.find().sort('weight', ASCENDING))
    assert sorted_docs[0]['name'] == 'Placeholder'
    assert sorted_docs[1]['name'] == 'Meercat'
    assert sorted_docs[-2]['name'] == 'Human'
    assert sorted_docs[-1]['name'] == 'Placeholder2'

    sorted_docs = list(coll.find().sort('weight', DESCENDING))
    assert sorted_docs[0]['name'] == 'Placeholder2'
    assert sorted_docs[1]['name'] == 'Human'
    assert sorted_docs[-2]['name'] == 'Meercat'
    assert sorted_docs[-1]['name'] == 'Placeholder'

    sorted_docs = list(coll.find().sort('name', DESCENDING))
    assert sorted_docs[0]['name'] == 'Secretarybird'
    assert sorted_docs[-1]['name'] == 'Honey Badger'

    sorted_docs = list(coll.find().sort([('name', ASCENDING)]))
    assert sorted_docs[0]['name'] == 'Honey Badger'
    assert sorted_docs[-1]['name'] == 'Secretarybird'

    sorted_docs = list(coll.find().sort([('name', ASCENDING)]))
    assert sorted_docs[0]['name'] == 'Honey Badger'
    assert sorted_docs[-1]['name'] == 'Secretarybird'

    sorted_docs = list(coll.find().sort([('kingdom', ASCENDING),
                                         ('weight', DESCENDING)]))
    assert sorted_docs[0].get('kingdom') == None
    assert sorted_docs[1].get('kingdom') == 'bird'
    assert sorted_docs[-1].get('kingdom') == 'reptile'
    assert sorted_docs[2].get('kingdom') == 'cat'
    assert sorted_docs[3].get('kingdom') == 'mammal'
    assert sorted_docs[3]['name'] == 'Human'
    assert sorted_docs[-2].get('kingdom') == 'mammal'
    assert sorted_docs[-2]['name'] == 'Meercat'

    sorted_docs = list(coll.find().sort([('kingdom', DESCENDING),
                                         ('weight', ASCENDING)]))
    assert sorted_docs[0].get('kingdom') == 'reptile'
    assert sorted_docs[-2].get('kingdom') == 'bird'
    assert sorted_docs[-1].get('kingdom') == None
    assert sorted_docs[1].get('kingdom') == 'mammal'
    assert sorted_docs[1]['name'] == 'Meercat'
    assert sorted_docs[-4].get('kingdom') == 'mammal'
    assert sorted_docs[-4]['name'] == 'Human'

    # next should would as expected
    doc_cursor = coll.find().sort('name')
    assert next(doc_cursor)['name'] == 'Honey Badger'
    assert next(doc_cursor)['name'] == 'Human'
    assert doc_cursor.next()['name'] == 'Indian grey mongoose'

    # clone should reset
    doc_cursor = doc_cursor.clone()
    assert next(doc_cursor)['name'] == 'Honey Badger'
    assert next(doc_cursor)['name'] == 'Human'
    assert doc_cursor.next()['name'] == 'Indian grey mongoose'

    # sorting shouldn't happen after iteration has begun
    with pytest.raises(errors.InvalidOperation):
        doc_cursor.sort('kingdom')

    # test bad format
    with pytest.raises(errors.PyMongoError):
        coll.find().sort([(ASCENDING, 'kingdom')])

    # test close
    doc_cursor.close()
    with pytest.raises(StopIteration):
        next(doc_cursor)


@pytest.mark.parametrize("client_class", CLIENTS)
def test_limit(client_class):
    client, coll, imr = setup_many(client_class)
    with pytest.raises(TypeError):
        coll.find().limit(2.0)

    assert len(list(coll.find().limit(0))) == 0
    assert len(list(coll.find().limit(3))) == 3
    assert set(d['name'] for d in
               coll.find().sort('name').limit(2)) == set(['Honey Badger', 'Human'])
    assert set(d['name'] for d in
               coll.find().limit(2).sort('name')) == set(['Honey Badger', 'Human'])

    # limiting shouldn't happen after iteration has begun
    doc_cursor = coll.find()
    next(doc_cursor)
    with pytest.raises(errors.InvalidOperation):
        doc_cursor.limit(2)


@pytest.mark.parametrize("client_class", CLIENTS)
def test_replace_one(client_class):
    client, coll, imr = setup_many(client_class)
    with pytest.raises(TypeError):
        coll.replace_one()
    with pytest.raises(TypeError):
        coll.replace_one({'_id': 'a'})

    doc = coll.find_one({'name': TEST_DOCS[0]['name']})
    assert doc['name'] == TEST_DOCS[0]['name']
    assert '_id' not in TEST_DOCS[1]
    ur = coll.replace_one({'_id': doc['_id']}, TEST_DOCS[1])
    assert '_id' not in TEST_DOCS[1]
    assert isinstance(ur, results.UpdateResult)
    assert coll.count_documents({'name': 'Indian grey mongoose'}) == 2

    coll.replace_one({'name': 'Indian grey mongoose'}, TEST_DOCS[2])
    assert coll.count_documents({'name': 'Indian grey mongoose'}) == 1
    assert coll.count_documents({'name': 'Honey Badger'}) == 2

    assert set(imr.inserted_ids) == set([d['_id'] for d in coll.find()])

    ur = coll.replace_one({'name': 'Fake Mongoose'},
                          {'name': 'Fake Mongoose', 'weight': 5},
                          upsert=True)
    assert ur.matched_count == 0
    assert ur.modified_count == 1
    assert ur.upserted_id and isinstance(ur.upserted_id, bson.ObjectId)
    assert coll.count_documents({'name': 'Fake Mongoose'}) == 1

    # upsert an existing document
    fake_mongoose = coll.find_one({'name': 'Fake Mongoose'})
    assert fake_mongoose
    with pytest.raises(errors.PyMongoError):
        ur = coll.replace_one({'name': 'Other mongoose'},
                              fake_mongoose,
                              upsert=True)

    # fail gracefully
    assert not coll.find_one({'name': 'not exists'})
    ur = coll.replace_one({'name': 'not exists'}, {'kingdom': 'fake kingdom'})
    assert ur.matched_count == 0
    assert ur.modified_count == 0


@pytest.mark.parametrize("client_class", CLIENTS)
def test_filters(client_class):
    client, coll, imr = setup_many(client_class)
    with pytest.raises(errors.PyMongoError):
        coll.find({'kingdom': {'$bigger': 'bird'}})

    assert coll.count_documents({'weight': {'$eq': 4}}) == 1
    assert coll.count_documents({'weight': {'$ne': 4}}) == LEN_TEST_DOCS - 1

    assert set(d['name'] for d in coll.find({'weight': {'$lt': 4}})) == \
           set(d['name'] for d in TEST_DOCS if isinstance(d.get('weight'), (int, float)) and d.get('weight') < 4)
    assert set(d['name'] for d in coll.find({'weight': {'$lte': 4}})) == \
           set(d['name'] for d in TEST_DOCS if isinstance(d.get('weight'), (int, float)) and d.get('weight') <= 4)
    assert set(d['name'] for d in coll.find({'weight': {'$gt': 4}})) == \
           set(d['name'] for d in TEST_DOCS if isinstance(d.get('weight'), (int, float)) and d.get('weight') > 4)
    assert set(d['name'] for d in coll.find({'weight': {'$gte': 4}})) == \
           set(d['name'] for d in TEST_DOCS if isinstance(d.get('weight'), (int, float)) and d.get('weight') >= 4)

    assert set(d['name'] for d in coll.find({'kingdom': {'$in': ['reptile', 'bird']}})) == \
           set(d['name'] for d in TEST_DOCS if d.get('kingdom') in ['reptile', 'bird'])
    assert set(d['name'] for d in coll.find({'kingdom': {'$nin': ['reptile', 'bird']}})) == \
           set(d['name'] for d in TEST_DOCS if d.get('kingdom') not in ['reptile', 'bird'])
    with pytest.raises(errors.PyMongoError):
        list(coll.find({'kingdom': {'$in': 'bird'}}))
    with pytest.raises(errors.PyMongoError):
        list(coll.find({'kingdom': {'$in': 5}}))
    with pytest.raises(errors.PyMongoError):
        list(coll.find({'kingdom': {'$in': None}}))
    with pytest.raises(errors.PyMongoError):
        list(coll.find({'kingdom': {'$nin': 'bird'}}))


@pytest.mark.parametrize("client_class", CLIENTS)
def test_update_one(client_class):
    client, coll, imr = setup_many(client_class)
    with pytest.raises(errors.PyMongoError):
        coll.update_one({}, {}, upsert=True)
    with pytest.raises(errors.PyMongoError):
        coll.update_one({}, {'name': 'Mongooose'})
    with pytest.raises(errors.PyMongoError):
        coll.update_one({}, ['name', 'Mongooose'])
    with pytest.raises(errors.PyMongoError):
        coll.update_one({}, {'$set': ['name', 'Mongooose']})
    with pytest.raises(errors.MongitaNotImplementedError):
        coll.update_one({}, {'$unset': ['name', '']})

    ur = coll.update_one({'name': 'Meercat'}, {'$set': {'name': 'Mongooose'}})
    assert isinstance(ur, results.UpdateResult)
    assert isinstance(repr(ur), str)
    assert ur.matched_count == 1
    assert ur.modified_count == 1
    assert ur.upserted_id is None
    assert coll.find_one({'name': 'Mongooose'})
    assert isinstance(imr.inserted_ids[0], bson.ObjectId)

    ur = coll.update_one({'kingdom': 'bird'}, {'$set': {'name': 'Pidgeotto'}})
    assert ur.matched_count == 1
    assert ur.modified_count == 1
    pidgeotto = coll.find_one({'kingdom': 'bird'})
    assert pidgeotto['name'] == 'Pidgeotto'

    ur = coll.update_one({'kingdom': 'bird'}, {'$set': {'kingdom': 'reptile'}})
    assert ur.matched_count == 1
    assert ur.modified_count == 1
    assert coll.find_one({'kingdom': 'bird'}) is None
    assert coll.count_documents({'kingdom': 'reptile'}) == 2
    pidgeotto2 = coll.find_one({'name': 'Pidgeotto'})
    assert pidgeotto2.get('kingdom') == 'reptile'
    assert pidgeotto2['_id'] == pidgeotto['_id']

    tmp_doc_cnt = coll.count_documents({'kingdom': 'mammal', 'name': {'$ne': 'Mongooose'}})
    ur = coll.update_one({'kingdom': 'mammal', 'name': {'$ne': 'Mongooose'}}, {'$set': {'kingdom': 'reptile'}})
    assert ur.matched_count == tmp_doc_cnt
    assert ur.modified_count == 1
    assert coll.count_documents({'kingdom': 'reptile'}) == 3

    mongoose = coll.find_one({'name': 'Mongooose'})
    mongoose_before_weight = mongoose.get('weight')
    ur = coll.update_one({'name': 'Mongooose'}, {'$inc': {'weight': -1}})
    assert ur.matched_count == 1
    assert ur.modified_count == 1
    mongoose = coll.find_one({'name': 'Mongooose'})
    assert mongoose.get('weight') == mongoose_before_weight - 1

    ur = coll.update_one({'name': 'fake'}, {'$set': {'name': 'Mngse'}})
    assert ur.matched_count == 0
    assert ur.modified_count == 0
    assert not coll.count_documents({'name': 'Mngse'})

    # corner case updating ids must be strings
    coll.update_one({'name': 'fake'}, {'$set': {'_id': 'uniqlo'}})
    coll.update_one({'name': 'fake'}, {'$set': {'_id': bson.ObjectId()}})
    with pytest.raises(errors.PyMongoError):
        coll.update_one({'name': 'fake'}, {'$set': {'_id': 5}})
    with pytest.raises(errors.PyMongoError):
        coll.update_one({'name': 'fake'}, {'$set': {'_id': 5}})


@pytest.mark.parametrize("client_class", CLIENTS)
def test_update_many(client_class):
    client, coll, imr = setup_many(client_class)
    with pytest.raises(errors.PyMongoError):
        coll.update_many({}, {}, upsert=True)
    with pytest.raises(errors.PyMongoError):
        coll.update_many({}, {'name': 'Mongooose'})

    ur = coll.update_many({}, {'$set': {'name': 'Mongooose'}})
    assert ur.matched_count == LEN_TEST_DOCS
    assert ur.modified_count == LEN_TEST_DOCS
    assert ur.upserted_id is None

    assert coll.distinct('name') == ['Mongooose']
    assert set(d['name'] for d in coll.find({})) == {'Mongooose'}
    assert coll.count_documents({'name': 'Mongooose'}) == LEN_TEST_DOCS

    ur = coll.update_many({'kingdom': {'$in': ['reptile', 'bird', 'cat']}},
                          {'$set': {'kingdom': 'mammal'}})
    assert ur.matched_count == 3
    assert ur.modified_count == 3
    assert coll.distinct('kingdom') == ['mammal']

    ur = coll.update_many({'weight': {'$lt': 4}},
                          {'$inc': {'weight': 1}})
    assert set(d.get('weight') for d in coll.find({})) == \
           set(d['weight'] + 1 if isinstance(d.get('weight'), Number) and d.get('weight') < 4 else d.get('weight') for d in TEST_DOCS)


@pytest.mark.parametrize("client_class", CLIENTS)
def test_distinct(client_class):
    client, coll, imr = setup_many(client_class)
    with pytest.raises(errors.PyMongoError):
        coll.distinct(5)
    dist = coll.distinct('name')
    assert isinstance(dist, list)
    assert set(dist) == set(d['name'] for d in TEST_DOCS)

    dist = coll.distinct('family')
    assert set(dist) == set(filter(None, [d.get('family') for d in TEST_DOCS]))

    dist = coll.distinct('family', {'kingdom': 'mammal'})
    assert set(dist) == set(d['family'] for d in TEST_DOCS if d.get('kingdom') == 'mammal')


@pytest.mark.parametrize("client_class", CLIENTS)
def test_delete_one(client_class):
    client, coll, imr = setup_many(client_class)
    with pytest.raises(TypeError):
        coll.delete_one()
    dor = coll.delete_one({})
    assert isinstance(dor, results.DeleteResult)
    assert isinstance(repr(dor), str)
    assert dor.deleted_count == 1
    remaining_ids = [d['_id'] for d in coll.find({})]
    assert len(remaining_ids) == len(TEST_DOCS) - 1
    dor = coll.delete_one({'_id': remaining_ids[-1]})
    assert dor.deleted_count == 1
    dor = coll.delete_one({'_id': remaining_ids[-1]})
    assert dor.deleted_count == 0
    assert coll.count_documents({}) == LEN_TEST_DOCS - 2

    dor = coll.delete_one({'_id': 'never_existed'})
    assert dor.deleted_count == 0


@pytest.mark.parametrize("client_class", CLIENTS)
def test_delete_many(client_class):
    client, coll, imr = setup_many(client_class)
    with pytest.raises(TypeError):
        coll.delete_many()
    dor = coll.delete_many({})
    assert isinstance(dor, results.DeleteResult)
    assert dor.deleted_count == LEN_TEST_DOCS
    assert coll.find_one({'_id': imr.inserted_ids[0]}) is None
    assert coll.find_one() is None
    assert coll.count_documents({}) == 0

    imr = coll.insert_many(TEST_DOCS)
    assert coll.count_documents({}) == LEN_TEST_DOCS
    dor = coll.delete_many({'_id': imr.inserted_ids[0]})
    assert coll.find_one({'_id': imr.inserted_ids[0]}) is None
    assert coll.count_documents({}) == LEN_TEST_DOCS - 1

    dor = coll.delete_many({'kingdom': 'mammal'})
    num_mammals = sum(1 for d in TEST_DOCS if d.get('kingdom') == 'mammal')
    assert dor.deleted_count == num_mammals - 1
    assert coll.count_documents({}) == LEN_TEST_DOCS - num_mammals

    dor = coll.delete_many({'kingdom': 'fish'})
    assert dor.deleted_count == 0
    assert coll.count_documents({}) == LEN_TEST_DOCS - num_mammals


@pytest.mark.parametrize("client_class", CLIENTS)
def test_nested(client_class):
    client, coll, imr = setup_many(client_class)

    assert set(coll.distinct('attrs.species')) == set([d['attrs']['species'] for d in TEST_DOCS if 'attrs' in d])

    assert not coll.find_one({'attrs.species': 'fake'})
    assert coll.find_one({'attrs.species': 'Suricata suricatta'})
    two_species = list(coll.find({'attrs.species': {'$in': ['Suricata suricatta',
                                                            'Mellivora capensis']}}))
    assert len(two_species) == 2
    assert set([s['name'] for s in two_species]) == {'Meercat', "Honey Badger"}

    assert coll.update_one({'attrs.species': 'Suricata suricatta'},
                           {'$set': {'attrs.adorable': True}})
    adorbs = coll.find_one({'attrs.adorable': True})
    assert adorbs
    assert adorbs['attrs']['adorable']
    assert coll.count_documents({'attrs.adorable': True}) == 1

    assert coll.update_one({'attrs.species': 'Suricata suricatta'},
                           {'$set': {'attrs.adorb_points': [1, 2, 3]}}).modified_count == 1

    assert coll.find_one({'attrs.adorb_points.0': 1})['attrs']['adorb_points'] == [1, 2, 3]

    assert coll.update_one({'attrs.species': 'Suricata suricatta'},
                           {'$set': {'attrs.adorb_points.5': 10}}).modified_count == 1

    assert coll.find_one({'attrs.adorb_points.0': 1})['attrs']['adorb_points'] == [1, 2, 3, None, None, 10]

    assert coll.update_one({'attrs.species': 'Suricata suricatta'},
                           {'$set': {'attrs.adorb_points.6.is.so': 'cute'}}).modified_count == 1

    # setting a list the wrong ways
    with pytest.raises(errors.MongitaError):
        assert coll.update_one({'attrs.species': 'Suricata suricatta'},
                               {'$set': {'attrs.adorb_points.seven': 'cute'}})
    with pytest.raises(errors.MongitaError):
        assert coll.update_one({'attrs.species': 'Suricata suricatta'},
                               {'$set': {'attrs.adorb_points.eight.boo': 'cute'}})
    with pytest.raises(errors.MongitaError):
        assert coll.update_one({'attrs.species': 'Suricata suricatta'},
                               {'$set': {'attrs.adorb_points.seven': 'cute'}})
    with pytest.raises(errors.MongitaError):
        assert coll.update_one({'attrs.species': 'Suricata suricatta'},
                               {'$set': {'attrs.adorb_points.-1.imaginary.boo': 'cute'}})
    with pytest.raises(errors.MongitaError):
        assert coll.update_one({'attrs.species': 'Suricata suricatta'},
                               {'$set': {'attrs.adorb_points.0.boo.hoo': 'cute'}})

    # addressing a list the wrong ways
    assert not coll.count_documents({'attrs.adorb_points.seven': 8})
    assert not coll.count_documents({'attrs.adorb_points.11': 8})
    assert not coll.count_documents({'attrs.adorb_points.0.hoo': 8})

    coll.create_index('species')
    assert coll.count_documents({'attrs.species': 'Suricata suricatta'}) == 1
    assert coll.update_one({'attrs.species': 'Suricata suricatta'},
                           {'$set': {'attrs.adorb_points': [1, 2, 3],
                                     'attrs.dict': {'hello': ['w', 'orld']}}}).modified_count == 1
    coll.create_index('attrs.adorb_points')
    coll.create_index('attrs.dict')

    assert coll.count_documents({}) == LEN_TEST_DOCS
    assert coll.count_documents({'attrs.adorb_points': {'$eq': [1, 2, 3]}}) == 1
    assert coll.count_documents({'attrs.adorb_points': [1, 2, 3]}) == 1
    assert coll.count_documents({'attrs.dict': {'hello': ['w', 'orld']}}) == 1
    assert coll.count_documents({'attrs.adorb_points': [1, 2]}) == 0
    assert coll.count_documents({'attrs.dict': {'hello': ['w', 'orld!']}}) == 0


@pytest.mark.parametrize("client_class", CLIENTS)
def test_basic_validation(client_class):
    client, coll, imr = setup_many(client_class)

    with pytest.raises(errors.PyMongoError):
        coll.insert_one('')
    with pytest.raises(errors.PyMongoError):
        coll.insert_one('blah')
    with pytest.raises(errors.PyMongoError):
        coll.insert_one([])
    with pytest.raises(errors.PyMongoError):
        coll.insert_one(['sdasd'])
    with pytest.raises(errors.PyMongoError):
        coll.insert_one([{'a': 'b'}])
    with pytest.raises(errors.PyMongoError):
        coll.insert_one({'_id': 5, 'param': 'param'})


@pytest.mark.parametrize("client_class", CLIENTS)
def test_collection(client_class):
    client, coll, imr = setup_many(client_class)
    assert coll.name == 'snake_hunter'
    assert coll.full_name == 'db.snake_hunter'
    assert isinstance(repr(coll), str)
    with pytest.raises(errors.MongitaNotImplementedError):
        coll.aggregate_raw_batches()
    with pytest.raises(errors.MongitaNotImplementedError):
        coll.count()
    coll2 = coll.blah
    assert isinstance(coll2, collection.Collection)
    assert coll2.name == 'snake_hunter.blah'


@pytest.mark.parametrize("client_class", CLIENTS)
def test_database(client_class):
    client, coll, imr = setup_many(client_class)
    db = client.db
    assert db.name == 'db'
    assert isinstance(repr(db), str)
    assert db.list_collection_names() == ['snake_hunter']
    cc = db.list_collections()
    assert isinstance(cc, command_cursor.CommandCursor)
    with pytest.raises(errors.PyMongoError):
        cc.batch_size()
    with pytest.raises(AttributeError):
        cc.not_real_attr()

    coll2 = next(cc)
    cc.close()
    with pytest.raises(StopIteration):
        cc.next()

    assert isinstance(coll2, collection.Collection)
    assert coll2.name == 'snake_hunter'
    assert coll == coll2

    assert db['snake_hunter'] == db.snake_hunter
    assert db['snake_hunter_2'] == db.snake_hunter_2
    assert db.list_collection_names() == ['snake_hunter']
    assert isinstance(db.snake_hunter_3, collection.Collection)
    assert db.snake_hunter_2.count_documents({}) == 0
    assert db.snake_hunter.count_documents({}) == LEN_TEST_DOCS
    db.drop_collection('snake_hunter')
    assert db.snake_hunter.count_documents({}) == 0

    # we don't have a collection initialized called db2 but this should run all the same
    db.drop_collection(db.snake_hunter_4)

    with pytest.raises(errors.MongitaNotImplementedError):
        db.add_son_manipulator()
    with pytest.raises(errors.MongitaNotImplementedError):
        db.dereference()

    with pytest.raises(errors.PyMongoError):
        db['$reserved']
    with pytest.raises(errors.PyMongoError):
        db['system.']
    with pytest.raises(errors.PyMongoError):
        db['']


@pytest.mark.parametrize("client_class", CLIENTS)
def test_client(client_class):
    client, coll, imr = setup_many(client_class)
    db = coll.database
    assert isinstance(repr(client), str)
    assert isinstance(db, database.Database)
    assert isinstance(client.engine, engines.engine_common.Engine)
    assert client.list_database_names() == ['db']
    cc = client.list_databases()
    assert isinstance(cc, command_cursor.CommandCursor)
    db2 = list(cc)[0]
    assert isinstance(db2, database.Database)
    assert db2.name == 'db'
    assert db == db2

    assert client['db'] == client.db
    assert client['db2'] == client.db2
    assert isinstance(db.db3, collection.Collection)
    assert client.db.list_collection_names() == ['snake_hunter']
    assert client.db2.list_collection_names() == []
    assert client.db2.snake_hunter.count_documents({}) == 0
    client.drop_database('db')
    assert client.db.list_collection_names() == []
    assert client.db.snake_hunter.count_documents({}) == 0

    # we don't have a database initialized called db4 but this should run all the same
    client.drop_database(client.db4)

    with pytest.raises(errors.MongitaNotImplementedError):
        client.close_cursor()

    with pytest.raises(errors.PyMongoError):
        client['$reserved']
    with pytest.raises(errors.PyMongoError):
        db['system.']
    with pytest.raises(errors.PyMongoError):
        client['']


@pytest.mark.parametrize("client_class", CLIENTS)
def test_empty_client(client_class):
    client = MongitaClientMemory()
    assert client.list_database_names() == []
    assert client.db.list_collection_names() == []
    assert client.db.coll.count_documents({}) == 0


@pytest.mark.parametrize("client_class", CLIENTS)
def test_bad_import(client_class):
    with pytest.raises(mongita.errors.MongitaNotImplementedError):
        mongita.InsertOne()
    with pytest.raises(AttributeError):
        mongita.mongodb_fictional_method()


# @pytest.mark.parametrize("client_class", CLIENTS)
# def test_common(client_class):
#     assert isinstance(repr(Location('a')), str)
#     assert Location(database='a', collection='b').path == 'a/b'
#     assert Location(database='a', collection='b', _id='c').path == 'a/b/c'
#     assert Location(database='a', _id='c').path == 'a/c'
#     assert Location(_id='c').path == 'c'


@pytest.mark.parametrize("client_class", CLIENTS)
def test_two_dbs_and_collections(client_class):
    client, coll, imr = setup_many(client_class)
    client.db.snakes_on_a_plane.insert_one({'motherfucking': 'snakes'})
    assert len(client.db.list_collection_names()) == 2
    assert 'snakes_on_a_plane' in client.db.list_collection_names()
    assert client.db.snakes_on_a_plane.count_documents({}) == 1

    client.test_db.snakes_on_a_plane.insert_one({'motherfucking': 'plane'})
    assert len(client.list_database_names()) == 2
    assert 'test_db' in client.list_database_names()
    assert client.test_db.snakes_on_a_plane.count_documents({}) == 1
    client.drop_database('test_db')


@pytest.mark.parametrize("client_class", CLIENTS)
def test_not_real_drops(client_class):
    client = client_class()
    client.drop_database('not_real')
    client.db.drop_collection('not_real')


@pytest.mark.parametrize("client_class", CLIENTS)
def test_list_dbs_collections(client_class):
    remove_test_dir()
    client = client_class()
    assert len(list(client.list_databases())) == 0
    assert len(list(client.db.list_collections())) == 0
    assert len(list(client.list_databases())) == 0

    client.db.coll.insert_one({'test': 'test'})
    client.close()

    client = client_class()
    if isinstance(client, MongitaClientMemory):
        expected_cnt = 0
    else:
        expected_cnt = 1

    assert len(list(client.list_databases())) == expected_cnt
    assert len(list(client.db.list_collections())) == expected_cnt
    assert len(list(client.list_databases())) == expected_cnt


@pytest.mark.parametrize("client_class", CLIENTS)
def test_indicies_basic(client_class):
    client, coll, imr = setup_many(client_class)

    with pytest.raises(mongita.errors.PyMongoError):
        coll.create_index('')
    with pytest.raises(mongita.errors.PyMongoError):
        coll.create_index(5)
    with pytest.raises(mongita.errors.PyMongoError):
        coll.create_index([])
    with pytest.raises(mongita.errors.PyMongoError):
        coll.create_index({'key': 1})
    with pytest.raises(mongita.errors.PyMongoError):
        coll.create_index([('key', ASCENDING), ('key2', ASCENDING)])
    with pytest.raises(mongita.errors.PyMongoError):
        coll.create_index([('key', 2)])
    with pytest.raises(mongita.errors.PyMongoError):
        coll.create_index('kingdom', background=True)


    idx_name = coll.create_index('kingdom')
    assert idx_name == 'kingdom_1'
    assert len(coll.index_information()) == 2
    coll.index_information()[1] == {'kingdom_1': {'key': [('kingdom', 1)]}}
    assert coll.count_documents({'kingdom': 'mammal'}) == \
        sum(1 for d in TEST_DOCS if d.get('kingdom') == 'mammal')
    assert coll.count_documents({'kingdom': 'reptile'}) == \
        sum(1 for d in TEST_DOCS if d.get('kingdom') == 'reptile')
    assert coll.count_documents({'kingdom': 'fish'}) == 0
    coll.drop_index(idx_name)

    idx_name = coll.create_index([('kingdom', -1)])
    assert idx_name == 'kingdom_-1'
    assert coll.count_documents({'kingdom': 'mammal'}) == \
        sum(1 for d in TEST_DOCS if d.get('kingdom') == 'mammal')
    assert coll.count_documents({'kingdom': 'reptile'}) == \
        sum(1 for d in TEST_DOCS if d.get('kingdom') == 'reptile')
    assert coll.count_documents({'kingdom': 'fish'}) == 0

    assert coll.insert_one(TEST_DOCS[0]).inserted_id
    assert coll.count_documents({'kingdom': 'mammal'}) == \
        sum(1 for d in TEST_DOCS if d.get('kingdom') == 'mammal') + 1
    assert coll.delete_one({'kingdom': 'reptile'}).deleted_count == 1
    assert coll.count_documents({'kingdom': 'reptile'}) == \
        sum(1 for d in TEST_DOCS if d.get('kingdom') == 'reptile') - 1

    coll.drop_index(idx_name)

    idx_name = coll.create_index('kingdom')
    coll.drop_index('kingdom_1')

    idx_name = coll.create_index('kingdom')
    coll.drop_index([('kingdom', 1)])

    idx_name = coll.create_index('kingdom')
    with pytest.raises(mongita.errors.PyMongoError):
        coll.drop_index(None)
    with pytest.raises(mongita.errors.PyMongoError):
        coll.drop_index('kingdom1')
    with pytest.raises(mongita.errors.PyMongoError):
        coll.drop_index('kingdom__1')
    with pytest.raises(mongita.errors.PyMongoError):
        coll.drop_index('kingdom_a')


@pytest.mark.parametrize("client_class", CLIENTS)
def test_indicies_filters(client_class):
    client, coll, imr = setup_many(client_class)

    coll.create_index('weight')
    coll.create_index('kingdom')
    assert coll.count_documents({'weight': {'$eq': 6}}) == \
        sum(1 for d in TEST_DOCS if d.get('weight') == 6)
    assert coll.count_documents({'weight': {'$ne': 6}}) == \
        sum(1 for d in TEST_DOCS if d.get('weight') != 6)
    assert coll.count_documents({'weight': {'$lt': 6}}) == \
        sum(1 for d in TEST_DOCS if isinstance(d.get('weight'), Number) and d.get('weight') < 6)
    assert coll.count_documents({'weight': {'$lte': 6}}) == \
        sum(1 for d in TEST_DOCS if isinstance(d.get('weight'), Number) and d.get('weight') <= 6)
    assert coll.count_documents({'weight': {'$gt': 6}}) == \
        sum(1 for d in TEST_DOCS if isinstance(d.get('weight'), Number) and d.get('weight') > 6)
    assert coll.count_documents({'weight': {'$gte': 6}}) == \
        sum(1 for d in TEST_DOCS if isinstance(d.get('weight'), Number) and d.get('weight') >= 6)
    assert coll.count_documents({'weight': {'$gte': 6}, 'kingdom': 'bird'}) == 0

    assert coll.count_documents({'kingdom': {'$in': ['bird', 'reptile']}}) == \
        sum(1 for d in TEST_DOCS if d.get('kingdom') in ['bird', 'reptile'])
    assert coll.count_documents({'kingdom': {'$nin': ['bird', 'reptile']}}) == \
        sum(1 for d in TEST_DOCS if d.get('kingdom') not in ['bird', 'reptile'])

    assert coll.count_documents({'weight': {'$gt': 6, '$eq': .75}}) == 0

    with pytest.raises(mongita.errors.PyMongoError):
        coll.count_documents({'weight': {'$eqq': 6}})

    with pytest.raises(mongita.errors.PyMongoError):
        coll.count_documents({'kingdom': {'$in': 'bird'}})
    with pytest.raises(mongita.errors.PyMongoError):
        coll.count_documents({'kingdom': {'$nin': 'bird'}})


@pytest.mark.parametrize("client_class", CLIENTS)
def test_indicies_flow(client_class):
    remove_test_dir()
    client = client_class()
    coll = client.db.coll
    coll.create_index("weight")
    assert len(coll.index_information()) == 2

    assert coll.count_documents({}) == 0
    inserted_ids = coll.insert_many(TEST_DOCS).inserted_ids
    assert len(inserted_ids) == LEN_TEST_DOCS

    assert len(coll.index_information()) == 2

    weight_lt_3 = len([d for d in TEST_DOCS if isinstance(d.get('weight'), Number) and d.get('weight') < 3])
    assert coll.count_documents({'weight': {'$lt': 3}}) == weight_lt_3
    ur = coll.update_many({'weight': {'$lt': 3}}, {'$set': {'weight': 5}})
    assert ur.matched_count == weight_lt_3
    assert ur.modified_count == weight_lt_3

    assert coll.count_documents({'weight': {'$lt': 3}}) == 0
    assert coll.count_documents({'weight': {'$gte': 3}}) == LEN_TEST_DOCS - 2

    ro = coll.replace_one({'weight': {'$lt': 3}}, {})
    assert ro.matched_count == 0
    assert ro.modified_count == 0

    ro = coll.replace_one({'_id': inserted_ids[0]}, {'weight': 1.5})
    assert ro.matched_count == 1
    assert ro.modified_count == 1

    assert coll.count_documents({'weight': {'$lt': 3}}) == 1
    assert coll.find_one({'_id': inserted_ids[0]}).get('weight') == 1.5

    for doc_id in inserted_ids:
        ro = coll.replace_one({'_id': doc_id}, {'no_weight': 'here'})
        assert ro.matched_count == 1
        assert ro.modified_count == 1

    assert coll.count_documents({'weight': {'$lt': 3}}) == 0
    assert coll.count_documents({'weight': {'$gte': 3}}) == 0

    for doc_id in inserted_ids:
        ro = coll.replace_one({'_id': doc_id}, {'weight': 7})
        assert ro.matched_count == 1
        assert ro.modified_count == 1

    assert coll.count_documents({'weight': {'$lt': 3}}) == 0
    assert coll.count_documents({'weight': {'$gte': 3}}) == LEN_TEST_DOCS

    dmr = coll.delete_many({'weight': 7})
    assert dmr.deleted_count == LEN_TEST_DOCS
    assert coll.count_documents({'weight': 7}) == 0
    assert coll.count_documents({}) == 0


@pytest.mark.parametrize("client_class", CLIENTS)
def test_thread_safe_io(client_class):
    remove_test_dir()
    client = client_class()
    def insert_one(doc):
        client.db.snake_hunter.insert_one(doc)

    docs = [dict(d) for d in TEST_DOCS * 8]
    for i, doc in enumerate(docs):
        doc['i'] = i
    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(insert_one, docs)
    for doc in docs:
        assert client.db.snake_hunter.find_one({'i': i})
    assert client.db.snake_hunter.count_documents({}) == LEN_TEST_DOCS * 8
    assert client.db.snake_hunter.count_documents({'name': 'Human'}) == 8

def flatten(list_of_lists):
    return [y for x in list_of_lists for y in x]


@pytest.mark.parametrize("client_class", CLIENTS)
def test_thread_safe_im(client_class):
    remove_test_dir()
    client = client_class()
    def insert_many(docs):
        client.db.snake_hunter.insert_many(docs)

    list_docs = []
    cur_list = []
    for i, doc in enumerate(TEST_DOCS * 8):
        doc['i'] = i
        cur_list.append(doc)
        if i % 4 == 0:
            list_docs.append(cur_list)
            cur_list = []
    list_docs.append(cur_list)

    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(insert_many, list_docs)
    for doc in flatten(list_docs):
        assert client.db.snake_hunter.find_one({'i': doc['i']})
    assert client.db.snake_hunter.count_documents({}) == LEN_TEST_DOCS * 8
    assert client.db.snake_hunter.count_documents({'name': 'Human'}) == 8


@pytest.mark.parametrize("client_class", CLIENTS)
def test_thread_safe_uo(client_class):
    client, coll, imr = setup_many(client_class)

    def update_one(tup):
        filter, replacement = tup
        coll.update_one(filter, replacement)

    tups = []
    for doc in coll.find({}):
        tups.append(({'_id': doc['_id']}, {'$set': {'name': 'BOOP'}}))
        tups.append(({'_id': doc['_id']}, {'$set': {'attrs.species': 'BEEP'}}))
    random.shuffle(tups)

    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(update_one, tups)

    assert coll.count_documents({}) == LEN_TEST_DOCS
    assert coll.count_documents({'name': 'BOOP'}) == LEN_TEST_DOCS
    assert coll.count_documents({'attrs.species': 'BEEP'}) == LEN_TEST_DOCS

    coll.create_index('kingdom')
    coll.create_index('weight')
    tups = []
    for doc in coll.find({}):
        tups.append(({'_id': doc['_id']}, {'$set': {'kingdom': 'BAM'}}))
        tups.append(({'_id': doc['_id']}, {'$set': {'weight': 7}}))
    random.shuffle(tups)

    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(update_one, tups)

    assert coll.count_documents({}) == LEN_TEST_DOCS
    assert coll.count_documents({'name': 'BOOP'}) == LEN_TEST_DOCS
    assert coll.count_documents({'attrs.species': 'BEEP'}) == LEN_TEST_DOCS
    assert coll.count_documents({'kingdom': 'BAM'}) == LEN_TEST_DOCS
    assert coll.count_documents({'weight': 7}) == LEN_TEST_DOCS
    assert coll.count_documents({'weight': 8}) == 0

    tups = []
    for doc in coll.find({}):
        tups.append(({'_id': doc['_id']}, {'$inc': {'weight': 1}}))
    random.shuffle(tups)

    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(update_one, tups)

    assert coll.count_documents({}) == LEN_TEST_DOCS
    assert coll.count_documents({'weight': 8}) == LEN_TEST_DOCS


@pytest.mark.parametrize("client_class", CLIENTS)
def test_thread_safe_update_many(client_class):
    client, coll, imr = setup_many(client_class)

    def um(tup):
        filter, update = tup
        coll.update_many(filter, update)

    weights = coll.distinct('weight')
    assert set(weights) == set(filter(None, [d.get('weight') for d in TEST_DOCS]))
    weights = [w for w in weights if isinstance(w, Number)]
    mid_weight = sorted(weights)[int(len(weights) / 2)]

    um(({}, {'$set': {'age': 5}}))
    assert coll.distinct('age') == [5]

    tups = [
        ({'weight': {'$lt': mid_weight}}, {'$inc': {'age': 2}}),
        ({'weight': {'$gte': mid_weight}}, {'$inc': {'age': 5}}),
        ({'weight': {'$lt': mid_weight}}, {'$inc': {'age': 5}}),
        ({'weight': {'$gte': mid_weight}}, {'$inc': {'age': 2}}),
        ({'weight': {'$gte': 0}}, {'$inc': {'age': 8}}),
        ({'weight': {'$eq': 'catweight'}}, {'$set': {'age': 20}}),
        ({'name': 'Placeholder'}, {'$set': {'age': -1}}),
    ]
    random.shuffle(tups)

    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(um, tups)

    assert set(coll.distinct('age')) == {20, -1}

    # Upsert should trigger an error because we don't support it because it's dumb
    # (we test this elsewhere but this seems like a pytest / concurrency bug)
    # We are losing coverage in the "no upsert" line after running this
    tups = [(), ()]
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(lambda x: coll.update_many({}, {'$set': {'age': 30}}, upsert=True), tups)
    assert set(coll.distinct('age')) == {20, -1}


@pytest.mark.parametrize("client_class", CLIENTS)
def test_thread_safe_replace_one(client_class):
    client, coll, imr = setup_many(client_class)

    def ro(tup):
        filter, doc = tup
        coll.replace_one(filter, doc)

    assert coll.count_documents({'name': 'Meercat'})
    ro(({'name': 'Meercat'}, {'hello': 'world', 'a': 'b'}))
    assert coll.find_one({'hello': 'world'})['a'] == 'b'
    assert not coll.count_documents({'name': 'Meercat'})

    tups = [
        ({'name': 'Secretarybird'}, {'bird': 'bird1'}),
        ({'name': 'Secretarybird'}, {'bird': 'bird2'})
    ]
    random.shuffle(tups)
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(ro, tups)
    assert coll.count_documents({'bird': 'bird1'}) + \
           coll.count_documents({'bird': 'bird2'}) == 1

    ro(({}, {'goodbye': 'world', 'y': 'z'}))
    assert coll.find_one({'goodbye': 'world'})['y'] == 'z'

    # Test replacing two documents with no filter
    # This might feel like we should have both bird3 and bird4 but actually
    # Concurrency allows multiple replaces on the same document just fine
    tups = [
        ({}, {'bird': 'bird3'}),
        ({}, {'bird': 'bird4'})
    ]
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(ro, tups)
    assert coll.count_documents({'bird': 'bird3'}) + \
           coll.count_documents({'bird': 'bird4'}) == 1

    # Now this should definitely result in two different replaces. We are
    # pulling two birds with weight <2 and changing their weight to > 2
    client, coll, imr = setup_many(client_class)
    tups = [
        ({'weight': {'$lt': 3}}, {'weight': 4, 'bird': 'bird5'}),
        ({'weight': {'$lt': 3}}, {'weight': 5, 'bird': 'bird6'}),
    ]
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(ro, tups)
    assert coll.count_documents({'bird': 'bird5'}) + \
           coll.count_documents({'bird': 'bird6'}) == 2


@pytest.mark.parametrize("client_class", CLIENTS)
def test_thread_safe_replace_one_upsert(client_class):
    client, coll, imr = setup_many(client_class)

    def ro(tup):
        filter, doc = tup
        coll.replace_one(filter, doc, upsert=True)

    # upsert should happen exactly once since one now exists
    tups = [
        ({'name': 'fake_bird'}, {'name': 'fake_bird', 'bird': 'bird1'}),
        ({'name': 'fake_bird'}, {'name': 'fake_bird', 'bird': 'bird2'})
    ]
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(ro, tups)
    assert coll.count_documents({'bird': 'bird1'}) + \
           coll.count_documents({'bird': 'bird2'}) == 1

    assert coll.count_documents({}) == LEN_TEST_DOCS + 1


@pytest.mark.parametrize("client_class", CLIENTS)
def test_thread_safe_delete_one(client_class):
    client, coll, imr = setup_many(client_class)

    def do(f):
        coll.delete_one(f)

    filters = [{}, {}, {}]
    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(do, filters)

    assert coll.count_documents({}) == LEN_TEST_DOCS - 3

    client, coll, imr = setup_many(client_class)
    filters = [
        {'name': 'Secretarybird'},
        {'name': 'Secretarybird'},
        {'name': 'Secretarybird'}]
    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(do, filters)
    assert coll.count_documents({}) == LEN_TEST_DOCS - 1


@pytest.mark.parametrize("client_class", CLIENTS)
def test_thread_safe_delete_many(client_class):
    client, coll, imr = setup_many(client_class)

    def dm(f):
        coll.delete_many(f)

    filters = [{}, {}, {}]
    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(dm, filters)

    assert coll.count_documents({}) == 0

    client, coll, imr = setup_many(client_class)
    filters = [
        {'weight': {'$lt': 3}},
        {'weight': {'$lt': 3}},
        {'weight': {'$lt': 3}}
    ]
    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(dm, filters)
    assert coll.count_documents({}) == len([d for d in TEST_DOCS if not isinstance(d.get('weight'), Number) or d.get('weight') >= 3])


@pytest.mark.parametrize("client_class", CLIENTS)
def test_thread_safe_index(client_class):
    client = client_class()
    coll = client.db.coll
    coll.create_index('weight')
    coll.create_index('kingdom')

    def io(doc):
        coll.insert_one(doc)

    def um(tup):
        filter, update = tup
        coll.update_many(filter, update)

    def ro(tup):
        filter, doc = tup
        coll.replace_one(filter, doc, upsert=True)

    def do(f):
        coll.delete_one(f)

    lwg3 = len([d for d in TEST_DOCS if isinstance(d.get('weight'), Number) and d.get('weight') >= 3])
    lmammal = len([d for d in TEST_DOCS if d.get('kingdom') == 'mammal'])

    test_docs = list(TEST_DOCS)
    random.shuffle(test_docs)
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(io, test_docs)

    assert coll.count_documents({}) == LEN_TEST_DOCS
    assert coll.count_documents({'weight': {'$gte': 3}}) == lwg3

    tups = [
        ({'weight': {'$gte': 3}}, {'$set': {'weight': 5}}),
        ({'kingdom': 'mammal'}, {'$set': {'kingdom': 'fakemammal'}}),
    ]
    random.shuffle(tups)
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(um, tups)

    assert coll.count_documents({}) == LEN_TEST_DOCS
    assert coll.count_documents({'weight': {'$gte': 3}}) == lwg3
    assert coll.count_documents({'weight': 5}) == lwg3
    assert coll.count_documents({'kingdom': 'mammal'}) == 0
    assert coll.count_documents({'kingdom': 'fakemammal'}) == lmammal

    tups = [
        ({'kingdom': 'fakemammal', 'weight': {'$ne': 5}}, {'kingdom': 'fm1'}),
        ({'weight': 5, 'kingdom': {'$ne': 'fakemammal'}}, {'weight': -1}),
    ]
    random.shuffle(tups)
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(ro, tups)

    assert coll.count_documents({}) == LEN_TEST_DOCS
    assert coll.count_documents({'kingdom': 'fm1'}) == 1
    assert coll.count_documents({'weight': 5}) == lwg3 - 1
    assert coll.count_documents({'weight': -1}) == 1

    tups = [{'weight': 5} for _ in range(lwg3 - 1)]
    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(do, tups)
    assert coll.count_documents({}) == LEN_TEST_DOCS - len(tups)
    assert coll.count_documents({'weight': 5}) == 0
    assert coll.count_documents({'weight': {'$ne': 5}}) == LEN_TEST_DOCS - lwg3 + 1


@pytest.mark.parametrize("client_class", CLIENTS)
def test_immediate_index(client_class):
    remove_test_dir()
    client = client_class()
    assert len(client.a.b.index_information()) == 1

    client = client_class()
    client.a.c.create_index("hello")
    client.a.c.insert_one({'hello': 'world'})
    assert client.a.c.count_documents({'hello': 'world'}) == 1

    client = client_class()
    with pytest.raises(errors.OperationFailure):
        client.a.d.drop_index("hello")


@pytest.mark.parametrize("client_class", CLIENTS)
def test_read_write_concern(client_class):
    """ These are mostly dummies """
    client = client_class()
    assert isinstance(client.read_concern, read_concern.ReadConcern)
    assert isinstance(client.write_concern, write_concern.WriteConcern)
    assert client.read_concern.document == {}
    assert client.write_concern.document == {}

    coll = client.db.coll
    assert isinstance(coll.read_concern, read_concern.ReadConcern)
    assert isinstance(coll.write_concern, write_concern.WriteConcern)
    assert coll.read_concern.document == {}
    assert coll.write_concern.document == {}

    with pytest.raises(errors.MongitaNotImplementedError):
        read_concern.ReadConcern(level="majority")
    with pytest.raises(errors.MongitaNotImplementedError):
        write_concern.WriteConcern(w=3)


@pytest.mark.parametrize("client_class", CLIENTS)
def test_with_options(client_class):
    """ With option is a dummy for now """
    client = client_class()
    coll = client.db.coll

    coll2 = coll.with_options(read_concern=read_concern.ReadConcern())
    assert coll.name == coll2.name
    assert coll.database == coll2.database

    with pytest.raises(errors.MongitaNotImplementedError):
        coll.with_options(codec_options="TEST")


def test_close_memory():
    """ Close on memory should delete everything """
    client, coll, imr = setup_many(MongitaClientMemory)
    assert coll.count_documents({}) == LEN_TEST_DOCS
    assert client.engine._cache
    client.close()
    assert not client.engine._cache
    assert coll.count_documents({}) == 0
    assert set(coll.distinct('name')) == set()
    client.close()
    assert not client.engine._cache
    client = MongitaClientMemory()
    assert not client.engine._cache
    assert client.db.snake_hunter.count_documents({}) == 0
    assert set(coll.distinct('name')) == set()


def test_secure_disk():
    try:
        shutil.rmtree(TEST_DIR)
    except:
        pass
    client = _MongitaClientDisk()
    with pytest.raises(errors.InvalidName):
        client['../evil']['./very'].insert_one({'boo': 'boo'})
    client.close()

    client['CON']['NUL'].insert_one({'boo': 'boo'})
    assert 'CON.NUL' in client.engine._cache
    full_path = client.engine._get_full_path('CON.NUL')
    assert set(client.list_database_names()) == {'CON'}
    assert list(client.CON.list_collection_names()) == ['NUL']
    full_path = client.engine._get_full_path('CON.NUL')
    assert not full_path.startswith('CON')
    assert not full_path[len(client.engine.base_storage_path) + 1:].startswith('CON')

    # long name
    with pytest.raises(errors.InvalidName):
        client.db.longlonglonglonglonglonglonglonglonglonglonglonglonglonglonglongl.insert_one({'boo': 'boo'})

    # bad key. Cannot start with $
    with pytest.raises(errors.InvalidName):
        client.db.c.insert_one({'$boo': 'boo'})


def test_close_disk():
    """ Close on disk should delete only the cache """
    client, coll, imr = setup_many(_MongitaClientDisk)
    assert client.engine._cache
    assert coll.count_documents({}) == LEN_TEST_DOCS
    coll.create_index('kingdom')
    client.close()
    assert not client.engine._cache
    assert len(list(client.list_databases())) == 1
    assert len(list(client.db.list_collections())) == 1
    assert coll.count_documents({}) == LEN_TEST_DOCS
    assert client.engine._cache
    assert set(coll.distinct('name')) == set(d['name'] for d in TEST_DOCS)
    client.close()
    assert not client.engine._cache
    client = _MongitaClientDisk()
    assert not client.engine._cache
    assert client.db.snake_hunter.count_documents({}) == LEN_TEST_DOCS
    assert client.engine._cache
    assert set(coll.distinct('name')) == set(d['name'] for d in TEST_DOCS)
    client.close()

    # remove the whole thing. It should get recreated
    shutil.rmtree(TEST_DIR)
    client = _MongitaClientDisk()
    assert not client.engine._cache
    assert client.db.snake_hunter.count_documents({}) == 0
    dor = client.db.snake_hunter.delete_one({'_id': 'fake_id'})
    assert dor.deleted_count == 0


def test_graceful_deletion():
    client, coll, imr = setup_many(_MongitaClientDisk)
    assert coll.find_one({'_id': imr.inserted_ids[0]})
    assert len(list(coll.find({}))) == LEN_TEST_DOCS
    client.close()

    shutil.rmtree(TEST_DIR)
    client = _MongitaClientDisk()
    assert list(client.list_databases()) == []
    coll = client.db.snake_hunter
    assert not coll.find_one({'_id': imr.inserted_ids[0]})
    dor = coll.delete_one({'_id': imr.inserted_ids[1]})
    assert dor.deleted_count == 0
    assert coll.count_documents({}) == 0


def test_strict():
    client, coll, imr = setup_many(MongitaClientMemory)
    assert not client.engine._strict

    im = coll.insert_one({'reason': 'bad date', 'dt': date(2020, 1, 1)})
    assert im.inserted_id
    imr = coll.insert_many([{'reason': 'bad date', 'dt': date(2020, 1, 1)},
                            {'reason': 'ok', 'dt': '2020-01-01'}])
    assert len(imr.inserted_ids) == 2
    client.close()

    client = MongitaClientMemory(strict=True)
    assert client.engine._strict
    coll = client.db.snake_hunter
    with pytest.raises(bson.errors.InvalidDocument):
        coll.insert_one({'reason': 'bad date', 'dt': date(2020, 1, 1), 'myid': 0})
    with pytest.raises(errors.PyMongoError):
        coll.insert_many([{'reason': 'ok', 'dt': '2020-01-01', 'myid': 1},
                          {'reason': 'bad date', 'dt': date(2020, 1, 1), 'myid': 2},
                          {'reason': 'ok', 'dt': '2020-01-01', 'myid': 3}])
    assert coll.count_documents({'myid': {'$in': [1, 3]}}) == 1
    assert coll.count_documents({'myid': {'$in': [2]}}) == 0
    with pytest.raises(errors.PyMongoError):
        coll.insert_many([{'reason': 'ok', 'dt': '2020-01-01', 'myid': 4},
                          {'reason': 'bad date', 'dt': date(2020, 1, 1), 'myid': 5},
                          {'reason': 'ok', 'dt': '2020-01-01', 'myid': 6}], ordered=False)
    assert coll.count_documents({'myid': {'$in': [4, 6]}}) == 2
    assert coll.count_documents({'myid': {'$in': [5]}}) == 0

    coll.create_index("weight")
    client.close()
    client = MongitaClientMemory(strict=True)
    coll = client.db.snake_hunter
    assert len(coll.index_information()) == 1


def test_flush():
    # Addresses a bug when we don't flush fast enough
    remove_test_dir()
    client = _MongitaClientDisk()
    client.db.coll.insert_many(TEST_DOCS * 1000)
    client = _MongitaClientDisk()
    assert client.db.coll.count_documents({}) == LEN_TEST_DOCS * 1000


def test_defrag():
    remove_test_dir()
    client = _MongitaClientDisk()
    client.db.coll.create_index('val')
    client.db.coll.insert_many(TEST_DOCS * 100)
    fh = client.engine._get_coll_fh('db.coll')
    fh.seek(0, 2)
    pos = fh.tell()
    ur = client.db.coll.delete_many({'val': {'$lt': .25}})
    fh.seek(0, 2)
    assert fh.tell() == pos

    client.close()
    client = _MongitaClientDisk()
    ur = client.db.coll.update_many({'val': {'$gt': .5}}, {'$set': {'attrs': {}, 'spotted': None}})
    assert ur.modified_count == len([d for d in TEST_DOCS if d.get('val') and d['val'] > .5]) * 100
    fh = client.engine._get_coll_fh('db.coll')
    fh.seek(0, 2)
    assert fh.tell() < pos


def test_two_connections_memory():
    client1 = MongitaClientMemory()
    client1.db.snake_hunter.insert_one({'hello': 'world'})
    client2 = MongitaClientMemory()
    assert client1.engine is not client2.engine
    assert client2.db.snake_hunter.count_documents({}) == 0


def test_two_connections_disk():
    remove_test_dir()
    client1 = _MongitaClientDisk()
    client1.db.snake_hunter.insert_one({'hello': 'world'})
    client2 = _MongitaClientDisk()
    assert client1.engine is client2.engine
    assert client2.db.snake_hunter.count_documents({}) == 1



