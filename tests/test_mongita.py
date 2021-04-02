from datetime import datetime, date
import functools
import os
import sys
import shutil
from concurrent.futures import ThreadPoolExecutor

import bson
import pytest

sys.path.append(os.getcwd().split('/tests')[0])

import mongita
from mongita import (MongitaClientMemory, MongitaClientDisk, ASCENDING, DESCENDING,
                     errors, results, cursor, collection, command_cursor, database,
                     engines)
from mongita.common import Location, StorageObject


# TODO test multi-level finds / inserts / etc

TEST_DOCS = [
    {
        'name': 'Meercat',
        'family': 'Herpestidae',
        'kingdom': 'mammal',
        'spotted': datetime(1994, 4, 23),
        'weight': 0.75,
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
        'continents': ['NA', 'SA', 'EA', 'AF'],
        'attrs': {
            'colors': 'brown',
            'species': 'Homo sapien'
        }
    }
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
    with pytest.raises(TypeError):
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
        coll.find_one({'weight': {'bigger': 7}})
    with pytest.raises(errors.PyMongoError):
        coll.find_one({5: 'hi'})

    doc = coll.find_one()
    assert isinstance(doc, dict) and not isinstance(doc, StorageObject)
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
    assert all(not isinstance(doc, StorageObject) for doc in docs)
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
    assert sorted_docs[0]['name'] == 'Meercat'
    assert sorted_docs[-1]['name'] == 'Human'

    sorted_docs = list(coll.find().sort('weight', ASCENDING))
    assert sorted_docs[0]['name'] == 'Meercat'
    assert sorted_docs[-1]['name'] == 'Human'

    sorted_docs = list(coll.find().sort('weight', DESCENDING))
    assert sorted_docs[0]['name'] == 'Human'
    assert sorted_docs[-1]['name'] == 'Meercat'

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
    assert sorted_docs[0]['kingdom'] == 'bird'
    assert sorted_docs[-1]['kingdom'] == 'reptile'
    assert sorted_docs[1]['kingdom'] == 'mammal'
    assert sorted_docs[1]['name'] == 'Human'
    assert sorted_docs[-2]['kingdom'] == 'mammal'
    assert sorted_docs[-2]['name'] == 'Meercat'

    sorted_docs = list(coll.find().sort([('kingdom', DESCENDING),
                                         ('weight', ASCENDING)]))
    assert sorted_docs[0]['kingdom'] == 'reptile'
    assert sorted_docs[-1]['kingdom'] == 'bird'
    assert sorted_docs[1]['kingdom'] == 'mammal'
    assert sorted_docs[1]['name'] == 'Meercat'
    assert sorted_docs[-2]['kingdom'] == 'mammal'
    assert sorted_docs[-2]['name'] == 'Human'

    # next should would as expected
    doc_cursor = coll.find().sort('name')
    assert next(doc_cursor)['name'] == 'Honey Badger'
    assert next(doc_cursor)['name'] == 'Human'
    assert doc_cursor.next()['name'] == 'Indian grey mongoose'

    # sorting shouldn't happen after iteration has begun
    with pytest.raises(errors.InvalidOperation):
        doc_cursor.sort('kingdom')

    # test bad format
    with pytest.raises(errors.PyMongoError):
        coll.find().sort([(ASCENDING, 'kingdom')])


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
           set(d['name'] for d in TEST_DOCS if d['weight'] < 4)
    assert set(d['name'] for d in coll.find({'weight': {'$lte': 4}})) == \
           set(d['name'] for d in TEST_DOCS if d['weight'] <= 4)
    assert set(d['name'] for d in coll.find({'weight': {'$gt': 4}})) == \
           set(d['name'] for d in TEST_DOCS if d['weight'] > 4)
    assert set(d['name'] for d in coll.find({'weight': {'$gte': 4}})) == \
           set(d['name'] for d in TEST_DOCS if d['weight'] >= 4)

    assert set(d['name'] for d in coll.find({'kingdom': {'$in': ['reptile', 'bird']}})) == \
           set(d['name'] for d in TEST_DOCS if d['kingdom'] in ['reptile', 'bird'])
    assert set(d['name'] for d in coll.find({'kingdom': {'$nin': ['reptile', 'bird']}})) == \
           set(d['name'] for d in TEST_DOCS if d['kingdom'] not in ['reptile', 'bird'])
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
    assert pidgeotto2['kingdom'] == 'reptile'
    assert pidgeotto2['_id'] == pidgeotto['_id']

    tmp_doc_cnt = coll.count_documents({'kingdom': 'mammal', 'name': {'$ne': 'Mongooose'}})
    ur = coll.update_one({'kingdom': 'mammal', 'name': {'$ne': 'Mongooose'}}, {'$set': {'kingdom': 'reptile'}})
    assert ur.matched_count == tmp_doc_cnt
    assert ur.modified_count == 1
    assert coll.count_documents({'kingdom': 'reptile'}) == 3

    mongoose = coll.find_one({'name': 'Mongooose'})
    mongoose_before_weight = mongoose['weight']
    ur = coll.update_one({'name': 'Mongooose'}, {'$inc': {'weight': -1}})
    assert ur.matched_count == 1
    assert ur.modified_count == 1
    mongoose = coll.find_one({'name': 'Mongooose'})
    assert mongoose['weight'] == mongoose_before_weight - 1

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

    ur = coll.update_many({'kingdom': {'$in': ['reptile', 'bird']}},
                          {'$set': {'kingdom': 'mammal'}})
    assert ur.matched_count == 2
    assert ur.modified_count == 2
    assert coll.distinct('kingdom') == ['mammal']

    ur = coll.update_many({'weight': {'$lt': 4}},
                          {'$inc': {'weight': 1}})
    assert set(d['weight'] for d in coll.find({})) == \
           set(d['weight'] + 1 if d['weight'] < 4 else d['weight'] for d in TEST_DOCS)


@pytest.mark.parametrize("client_class", CLIENTS)
def test_distinct(client_class):
    client, coll, imr = setup_many(client_class)
    with pytest.raises(errors.PyMongoError):
        coll.distinct(5)
    dist = coll.distinct('name')
    assert isinstance(dist, list)
    assert set(dist) == set(d['name'] for d in TEST_DOCS)

    dist = coll.distinct('family')
    assert set(dist) == set(d['family'] for d in TEST_DOCS)

    dist = coll.distinct('family', {'kingdom': 'mammal'})
    assert set(dist) == set(d['family'] for d in TEST_DOCS if d['kingdom'] == 'mammal')


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
    num_mammals = sum(1 for d in TEST_DOCS if d['kingdom'] == 'mammal')
    assert dor.deleted_count == num_mammals - 1
    assert coll.count_documents({}) == LEN_TEST_DOCS - num_mammals

    dor = coll.delete_many({'kingdom': 'fish'})
    assert dor.deleted_count == 0
    assert coll.count_documents({}) == LEN_TEST_DOCS - num_mammals


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
    coll2 = list(cc)[0]
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


@pytest.mark.parametrize("client_class", CLIENTS)
def test_common(client_class):
    assert isinstance(repr(Location('a')), str)
    assert Location(database='a', collection='b').path == 'a/b'
    assert Location(database='a', collection='b', _id='c').path == 'a/b/c'
    assert Location(database='a', _id='c').path == 'a/c'
    assert Location(_id='c').path == 'c'


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

    idx_name = coll.create_index('kingdom')
    assert idx_name == 'kingdom_1'
    assert len(coll.index_information()) == 2
    coll.index_information()[1] == {'kingdom_1': {'key': [('kingdom', 1)]}}
    assert coll.count_documents({'kingdom': 'mammal'}) == \
        sum(1 for d in TEST_DOCS if d['kingdom'] == 'mammal')
    assert coll.count_documents({'kingdom': 'reptile'}) == \
        sum(1 for d in TEST_DOCS if d['kingdom'] == 'reptile')
    assert coll.count_documents({'kingdom': 'fish'}) == 0
    coll.drop_index(idx_name)

    idx_name = coll.create_index([('kingdom', -1)])
    assert idx_name == 'kingdom_-1'
    assert coll.count_documents({'kingdom': 'mammal'}) == \
        sum(1 for d in TEST_DOCS if d['kingdom'] == 'mammal')
    assert coll.count_documents({'kingdom': 'reptile'}) == \
        sum(1 for d in TEST_DOCS if d['kingdom'] == 'reptile')
    assert coll.count_documents({'kingdom': 'fish'}) == 0

    assert coll.insert_one(TEST_DOCS[0]).inserted_id
    assert coll.count_documents({'kingdom': 'mammal'}) == \
        sum(1 for d in TEST_DOCS if d['kingdom'] == 'mammal') + 1
    assert coll.delete_one({'kingdom': 'reptile'}).deleted_count == 1
    assert coll.count_documents({'kingdom': 'reptile'}) == \
        sum(1 for d in TEST_DOCS if d['kingdom'] == 'reptile') - 1

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


@pytest.mark.parametrize("client_class", CLIENTS)
def test_indicies_filters(client_class):
    client, coll, imr = setup_many(client_class)

    coll.create_index('weight')
    coll.create_index('kingdom')
    assert coll.count_documents({'weight': {'$eq': 6}}) == \
        sum(1 for d in TEST_DOCS if d['weight'] == 6)
    assert coll.count_documents({'weight': {'$ne': 6}}) == \
        sum(1 for d in TEST_DOCS if d['weight'] != 6)
    assert coll.count_documents({'weight': {'$lt': 6}}) == \
        sum(1 for d in TEST_DOCS if d['weight'] < 6)
    assert coll.count_documents({'weight': {'$lte': 6}}) == \
        sum(1 for d in TEST_DOCS if d['weight'] <= 6)
    assert coll.count_documents({'weight': {'$gt': 6}}) == \
        sum(1 for d in TEST_DOCS if d['weight'] > 6)
    assert coll.count_documents({'weight': {'$gte': 6}}) == \
        sum(1 for d in TEST_DOCS if d['weight'] >= 6)

    assert coll.count_documents({'kingdom': {'$in': ['bird', 'reptile']}}) == \
        sum(1 for d in TEST_DOCS if d['kingdom'] in ['bird', 'reptile'])
    assert coll.count_documents({'kingdom': {'$nin': ['bird', 'reptile']}}) == \
        sum(1 for d in TEST_DOCS if d['kingdom'] not in ['bird', 'reptile'])

    with pytest.raises(mongita.errors.PyMongoError):
        coll.count_documents({'weight': {'$eqq': 6}})

    with pytest.raises(mongita.errors.PyMongoError):
        coll.count_documents({'kingdom': {'$in': 'bird'}})
    with pytest.raises(mongita.errors.PyMongoError):
        coll.count_documents({'kingdom': {'$nin': 'bird'}})

# TODO all of the different ways to insert, update, replace, delete documents
# and validating that the indicies stay solid


@pytest.mark.parametrize("client_class", CLIENTS)
def test_thread_safe(client_class):
    remove_test_dir()
    client = client_class()
    def insert_one(doc):
        print("inserting %d" % doc['i'])
        try:
            client.db.snake_hunter.insert_one(doc)
        except:
            # TODO I think what is happening is that I'm getting excepts
            # and not uploading all of the files. If this is to be thread-safe,
            # I need to have built-in retries for modifying docs and metadata
            # This could be like the GIL so that only one thread can do a write
            # operation at a time.
            print("EXCEPT!!")

    docs = [dict(d) for d in TEST_DOCS * 8]
    for i, doc in enumerate(docs):
        doc['i'] = i
    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(insert_one, docs)
    for doc in docs:
        print(doc['i'])
        assert client.db.snake_hunter.find_one({'i': i})
    assert client.db.snake_hunter.count_documents({}) == LEN_TEST_DOCS * 8
    assert client.db.snake_hunter.count_documents({'name': 'Human'}) == 8


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



    # imr = coll.insert_many([{'reason': 'bad date', 'dt': date(2020, 1, 1)},
    #                         {'reason': 'ok', 'dt': '2020-01-01'}], ordered=False)

