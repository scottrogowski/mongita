from datetime import datetime, timedelta
import os
import sys
import shutil

import bson
import pytest

sys.path.append(os.getcwd().split('/tests')[0])

import mongita
from mongita import (MongitaClientMemory, ASCENDING, DESCENDING, errors,
                     results, cursor, collection, command_cursor, database, engines)
from mongita.common import Location

TEST_DIR = 'mongita_unittest_storage'

if os.path.exists(TEST_DIR):
    shutil.rmtree(TEST_DIR)

TEST_DOCS = [
    {
        'name': 'Meercat',
        'family': 'Herpestidae',
        'kingdom': 'mammal',
        'weight': 0.75,
        'continents': ['AF'],
    },
    {
        'name': 'Indian grey mongoose',
        'family': 'Herpestidae',
        'kingdom': 'mammal',
        'weight': 1.4,
        'continents': ['EA'],
    },
    {
        'name': 'Honey Badger',
        'family': 'Mustelidae',
        'kingdom': 'mammal',
        'weight': 10,
        'continents': ['EA', 'AF'],
    },
    {
        'name': 'King Cobra',
        'family': 'Elapidae',
        'kingdom': 'reptile',
        'weight': 6,
        'continents': ['EA'],
    },
    {
        'name': 'Secretarybird',
        'family': 'Sagittariidae',
        'kingdom': 'bird',
        'weight': 4,
        'continents': ['AF'],
    },
    {
        'name': 'Human',
        'family': 'Hominidae',
        'kingdom': 'mammal',
        'weight': 70,
        'continents': ['NA', 'SA', 'EA', 'AF']
    }
]
LEN_TEST_DOCS = len(TEST_DOCS)


def setup_one():
    client = MongitaClientMemory()
    coll = client.db.snake_hunter
    return client, coll, coll.insert_one(TEST_DOCS[0])


def setup_many():
    client = MongitaClientMemory()
    coll = client.db.snake_hunter
    return client, coll, coll.insert_many(TEST_DOCS)


def test_insert_one():
    client, coll, ior = setup_one()

    # document required
    with pytest.raises(TypeError):
        coll.insert_one()

    # kwarg support_alert
    with pytest.raises(TypeError):
        coll.insert_one({'doc': 'doc'}, bypass_document_validation=True)
    assert isinstance(ior, results.InsertOneResult)
    assert isinstance(repr(ior), str)
    assert len(ior.inserted_id) == len(str(bson.ObjectId()))
    assert coll.count_documents({}) == 1
    assert coll.count_documents({'_id': ior.inserted_id}) == 1
    assert coll.find_one()['_id'] == ior.inserted_id

    td = TEST_DOCS[1]
    td['_id'] = 'uniqlo'
    ior = coll.insert_one(td)
    assert ior.inserted_id == 'uniqlo'
    assert coll.count_documents({'_id': 'uniqlo'}) == 1
    assert coll.count_documents({}) == 2

    td = TEST_DOCS[1]
    td['_id'] = bson.ObjectId()
    ior = coll.insert_one(td)
    assert ior.inserted_id == td['_id']
    assert coll.count_documents({'_id': td['_id']}) == 1
    assert coll.count_documents({}) == 3

    # already exists
    doc = coll.find_one()
    with pytest.raises(errors.MongitaError):
        coll.insert_one(doc)


def test_insert_many():
    client, coll, imr = setup_many()
    with pytest.raises(TypeError):
        coll.insert_many()
    with pytest.raises(errors.MongitaError):
        coll.insert_many({'doc': 'doc'})
    assert isinstance(imr, results.InsertManyResult)
    assert isinstance(repr(imr), str)
    assert len(imr.inserted_ids) == LEN_TEST_DOCS
    assert coll.count_documents({}) == LEN_TEST_DOCS
    assert coll.count_documents({'_id': imr.inserted_ids[0]}) == 1
    assert coll.count_documents({'_id': {'$in': imr.inserted_ids}}) == LEN_TEST_DOCS

    # Bad docs don't trigger. Ordered continues after errors
    imr = coll.insert_many([{'_id': 5, 'reason': 'bad_id'}, {'this_doc_is': 'ok'},
                            'string_instead_of_doc'])
    assert len(imr.inserted_ids) == 0
    imr = coll.insert_many([{'_id': 5, 'reason': 'bad_id'}, {'this_doc_is': 'ok'},
                            'string_instead_of_doc'], ordered=False)
    assert len(imr.inserted_ids) == 1


def test_count_documents_one():
    client, coll, ior = setup_one()
    with pytest.raises(TypeError):
        coll.count_documents()
    assert coll.count_documents({}) == 1
    assert coll.count_documents({'_id': ior.inserted_id}) == 1
    assert coll.count_documents({'_id': bson.ObjectId(str(ior.inserted_id))}) == 1
    assert coll.count_documents({'_id': 'abc'}) == 0
    assert coll.count_documents({'kingdom': 'mammal'}) == 1
    assert coll.count_documents({'kingdom': 'reptile'}) == 0
    assert coll.count_documents({'blah': 'blah'}) == 0


def test_find_one():
    client, coll, imr = setup_many()
    with pytest.raises(errors.MongitaError):
        coll.find_one(['hi'])
    with pytest.raises(errors.MongitaError):
        coll.find_one({'weight': {'$bigger': 7}})
    with pytest.raises(errors.MongitaError):
        coll.find_one({'weight': {'$bigger': 7}})
    with pytest.raises(errors.MongitaError):
        coll.find_one({'weight': {'bigger': 7}})
    with pytest.raises(errors.MongitaError):
        coll.find_one({5: 'hi'})

    doc = coll.find_one()
    assert doc['name'] == 'Meercat'
    assert isinstance(doc['continents'], list)
    del doc['_id']
    assert doc == TEST_DOCS[0]

    doc = coll.find_one({'_id': imr.inserted_ids[1]})
    assert doc['name'] == 'Indian grey mongoose'

    doc = coll.find_one({'family': 'Herpestidae'})
    assert doc['name'] == 'Meercat'

    doc = coll.find_one({'family': 'matters'})
    assert doc is None

    doc = coll.find_one({'family': 'Herpestidae', 'weight': {'$lt': 1}})
    assert doc['name'] == 'Meercat'

    doc = coll.find_one({'family': 'Herpestidae', 'weight': {'$lt': 0}})
    assert not doc

    doc = coll.find_one({'family': 'Herpestidae', 'weight': {'$gt': 20}})
    assert not doc

    with pytest.raises(errors.MongitaError):
        doc = coll.find_one({'_id': 5})


def test_find():
    client, coll, imr = setup_many()
    doc_cursor = coll.find()
    assert isinstance(doc_cursor, cursor.Cursor)
    docs = list(doc_cursor)
    assert len(docs) == LEN_TEST_DOCS
    assert all(doc.get('_id') for doc in docs)
    docs = list(coll.find())
    assert docs[0]['name'] == 'Meercat'
    assert docs[-1]['name'] == 'Human'

    doc_cursor = coll.find({'family': 'Herpestidae'})
    assert len(list(doc_cursor)) == 2
    assert all(doc['family'] == 'Herpestidae' for doc in doc_cursor)

    doc_cursor = coll.find({'family': 'Herpestidae', 'weight': {'$lt': 1}})
    assert len(list(doc_cursor)) == 1

    doc_cursor = coll.find({'family': 'Herpestidae', 'weight': {'$gt': 20}})
    assert len(list(doc_cursor)) == 0

    doc_cursor = coll.find({'family': 'Herpestidae', 'weight': {'$lt': 20}})
    assert len(list(doc_cursor)) == 2


def test_cursor():
    client, coll, imr = setup_many()
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

    assert coll.find().next()['name'] == TEST_DOCS[0]['name']

    # not asc or desc
    with pytest.raises(errors.MongitaError):
        list(coll.find().sort('weight', 2))

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

    # sorting shouldn't happen after iteration has begun
    with pytest.raises(errors.InvalidOperation):
        doc_cursor.sort('kingdom')

    # test bad format
    with pytest.raises(errors.MongitaError):
        coll.find().sort('kingdom', ASCENDING, True)
    with pytest.raises(errors.MongitaError):
        coll.find().sort([(ASCENDING, 'kingdom')])

def test_limit():
    client, coll, imr = setup_many()
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

def test_replace_one():
    client, coll, imr = setup_many()
    with pytest.raises(TypeError):
        coll.replace_one()
    with pytest.raises(TypeError):
        coll.replace_one({'_id': 'a'})
    doc = coll.find_one()

    ur = coll.replace_one({'_id': doc['_id']}, TEST_DOCS[1])
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
    assert ur.upserted_id and isinstance(ur.upserted_id, str)
    assert coll.count_documents({'name': 'Fake Mongoose'}) == 1

    # upsert an existing document
    fake_mongoose = coll.find_one({'name': 'Fake Mongoose'})
    assert fake_mongoose
    with pytest.raises(errors.MongitaError):
        ur = coll.replace_one({'name': 'Other mongoose'},
                              fake_mongoose,
                              upsert=True)


def test_filters():
    client, coll, imr = setup_many()
    with pytest.raises(errors.MongitaError):
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
    with pytest.raises(errors.MongitaError):
        list(coll.find({'kingdom': {'$in': 'bird'}}))
    with pytest.raises(errors.MongitaError):
        list(coll.find({'kingdom': {'$in': 5}}))
    with pytest.raises(errors.MongitaError):
        list(coll.find({'kingdom': {'$in': None}}))
    with pytest.raises(errors.MongitaError):
        list(coll.find({'kingdom': {'$nin': 'bird'}}))


def test_update_one():
    client, coll, imr = setup_many()
    with pytest.raises(errors.MongitaError):
        coll.update_one({}, {}, upsert=True)
    with pytest.raises(errors.MongitaError):
        coll.update_one({}, {'name': 'Mongooose'})
    with pytest.raises(errors.MongitaError):
        coll.update_one({}, ['name', 'Mongooose'])
    with pytest.raises(errors.MongitaError):
        coll.update_one({}, {'$set': ['name', 'Mongooose']})
    ur = coll.update_one({}, {'$set': {'name': 'Mongooose'}})
    assert isinstance(ur, results.UpdateResult)
    assert isinstance(repr(ur), str)
    assert ur.matched_count == LEN_TEST_DOCS
    assert ur.modified_count == 1
    assert ur.upserted_id is None
    assert coll.find_one({'name': 'Mongoose'}) is None
    assert coll.find_one({'name': 'Mongooose'})['_id'] == imr.inserted_ids[0]

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

    ur = coll.update_one({'kingdom': 'mammal'}, {'$set': {'kingdom': 'reptile'}})
    assert ur.matched_count == sum(1 for d in TEST_DOCS if d['kingdom'] == 'mammal')
    assert ur.modified_count == 1
    assert coll.count_documents({'kingdom': 'reptile'}) == 3
    assert set(d['name'] for d in coll.find({'kingdom': 'reptile'})) == \
           set(['Mongooose', 'Pidgeotto', 'King Cobra'])

    ur = coll.update_one({'name': 'Mongooose'}, {'$inc': {'weight': -1}})
    assert ur.matched_count == 1
    assert ur.modified_count == 1
    mongoose = coll.find_one({'name': 'Mongooose'})
    assert mongoose['weight'] == TEST_DOCS[0]['weight'] - 1

    ur = coll.update_one({'name': 'fake'}, {'$set': {'name': 'Mngse'}})
    assert ur.matched_count == 0
    assert ur.modified_count == 0
    assert not coll.count_documents({'name': 'Mngse'})

    # corner case updating ids must be strings
    coll.update_one({'name': 'fake'}, {'$set': {'_id': 'uniqlo'}})
    coll.update_one({'name': 'fake'}, {'$set': {'_id': bson.ObjectId()}})
    with pytest.raises(errors.MongitaError):
        coll.update_one({'name': 'fake'}, {'$set': {'_id': 5}})
    with pytest.raises(errors.MongitaError):
        coll.update_one({'name': 'fake'}, {'$set': {'_id': 5}})


def test_update_many():
    client, coll, imr = setup_many()
    with pytest.raises(errors.MongitaError):
        coll.update_many({}, {}, upsert=True)
    with pytest.raises(errors.MongitaError):
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


def test_distinct():
    client, coll, imr = setup_many()
    with pytest.raises(errors.MongitaError):
        coll.distinct(5)
    dist = coll.distinct('name')
    assert isinstance(dist, list)
    assert set(dist) == set(d['name'] for d in TEST_DOCS)

    dist = coll.distinct('family')
    assert set(dist) == set(d['family'] for d in TEST_DOCS)

    dist = coll.distinct('family', {'kingdom': 'mammal'})
    assert set(dist) == set(d['family'] for d in TEST_DOCS if d['kingdom'] == 'mammal')


def test_delete_one():
    client, coll, imr = setup_many()
    with pytest.raises(TypeError):
        coll.delete_one()
    dor = coll.delete_one({})
    assert isinstance(dor, results.DeleteResult)
    assert isinstance(repr(dor), str)
    assert dor.deleted_count == 1
    assert coll.find_one({'_id': imr.inserted_ids[0]}) is None
    dor = coll.delete_one({'_id': imr.inserted_ids[-1]})
    assert dor.deleted_count == 1
    dor = coll.delete_one({'_id': imr.inserted_ids[-1]})
    assert dor.deleted_count == 0
    assert coll.count_documents({}) == LEN_TEST_DOCS - 2

    dor = coll.delete_one({'_id': 'never_existed'})
    assert dor.deleted_count == 0


def test_delete_many():
    client, coll, imr = setup_many()
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


def test_basic_validation():
    client, coll, imr = setup_many()

    with pytest.raises(errors.MongitaError):
        coll.insert_one('')
    with pytest.raises(errors.MongitaError):
        coll.insert_one('blah')
    with pytest.raises(errors.MongitaError):
        coll.insert_one([])
    with pytest.raises(errors.MongitaError):
        coll.insert_one(['sdasd'])
    with pytest.raises(errors.MongitaError):
        coll.insert_one([{'a': 'b'}])
    with pytest.raises(errors.MongitaError):
        coll.insert_one({'_id': 5, 'param': 'param'})


def test_collection():
    client, coll, imr = setup_many()
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


def test_database():
    client, coll, imr = setup_many()
    db = client.db
    assert db.name == 'db'
    assert isinstance(repr(db), str)
    assert db.list_collection_names() == ['snake_hunter']
    cc = db.list_collections()
    assert isinstance(cc, command_cursor.CommandCursor)
    with pytest.raises(errors.MongitaError):
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

    with pytest.raises(errors.MongitaError):
        db['$reserved']
    with pytest.raises(errors.MongitaError):
        db['system.']
    with pytest.raises(errors.MongitaError):
        db['']


def test_client():
    client, coll, imr = setup_many()
    db = coll.database
    assert isinstance(repr(client), str)
    assert isinstance(db, database.Database)
    assert isinstance(client.engine, engines.memory_engine.MemoryEngine)
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

    with pytest.raises(errors.MongitaError):
        client['$reserved']
    with pytest.raises(errors.MongitaError):
        db['system.']
    with pytest.raises(errors.MongitaError):
        client['']


def test_empty_client():
    client = MongitaClientMemory()
    assert client.list_database_names() == []
    assert client.db.list_collection_names() == []
    assert client.db.coll.count_documents({}) == 0


def test_bad_import():
    with pytest.raises(mongita.errors.MongitaNotImplementedError):
        mongita.InsertOne()
    with pytest.raises(AttributeError):
        mongita.mongodb_fictional_method()


def test_common():
    assert isinstance(repr(Location()), str)
    assert Location(database='a', collection='b').path == 'a/b'
    assert Location(database='a', collection='b', _id='c').path == 'a/b/c'
    assert Location(database='a', _id='c').path == 'a/c'
    assert Location(_id='c').path == 'c'



# def test_indicies():
#     client, coll, imr = setup_many()
#     with pytest.raises(mongita.errors.MongitaError):
#         coll.create_index('')
#     with pytest.raises(mongita.errors.MongitaError):
#         coll.create_index(5)
#     with pytest.raises(mongita.errors.MongitaError):
#         coll.create_index([])
#     with pytest.raises(mongita.errors.MongitaError):
#         coll.create_index({'key': 1})
#     with pytest.raises(mongita.errors.MongitaError):
#         coll.create_index([('key', ASCENDING), ('key2', ASCENDING)])
#     with pytest.raises(mongita.errors.MongitaError):
#         coll.create_index([('key', 2)])

#     idx_name = coll.create_index('kingdom')
#     assert idx_name == 'kingdom_1'
#     assert coll.count_documents({'kingdom': 'mammal'}) == \
#         sum(1 for d in TEST_DOCS if d['kingdom'] == 'mammal')
#     assert coll.count_documents({'kingdom': 'reptile'}) == \
#         sum(1 for d in TEST_DOCS if d['kingdom'] == 'reptile')
#     assert coll.count_documents({'kingdom': 'fish'}) == 0

#     coll.drop_index('kingdom_1')
