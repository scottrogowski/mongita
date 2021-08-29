![Mongita Logo](https://raw.githubusercontent.com/scottrogowski/mongita/master/assets/mongita.jpg)

![Version 1.0.1](https://img.shields.io/badge/version-1.0.1-brightgreen) ![Build passing](https://img.shields.io/badge/build-passing-brightgreen) ![Coverage 100%](https://img.shields.io/badge/coverage-100%25-brightgreen) ![License BSD](https://img.shields.io/badge/license-BSD-green])

Mongita is a lightweight embedded document database that implements a commonly-used subset of the [MongoDB/PyMongo interface](https://pymongo.readthedocs.io/en/stable/). Mongita differs from MongoDB in that instead of being a server, Mongita is a self-contained Python library.  Mongita can be configured to store its documents either on disk or in memory.

> *"Mongita is to MongoDB as SQLite is to SQL"*

Mongita is in active development. Please report any bugs. Mongita is free and open source. [You can contribute!]((#contributing))

### Applications
- **Embedded database**: Mongita is a good alternative to [SQLite](https://www.sqlite.org/index.html) for embedded applications when a document database makes more sense than a relational one.
- **Unit testing**: Mocking PyMongo/MongoDB is a pain. Worse, mocking can hide real bugs. By monkey-patching PyMongo with Mongita, unit tests can be more faithful while remaining isolated.
 
### Design goals
- **MongoDB compatibility**: Mongita implements a commonly-used subset of the PyMongo API. This allows projects to be started with Mongita and later upgraded to MongoDB once they reach an appropriate scale.
- **Embedded/self-contained**: Mongita does not require a server or start a process. It is just a Python library. To use it, just add `import mongita` to the top of your script.
- **Speed**: Mongita is within an order of magnitude of MongoDB and Sqlite in 10k document benchmarks. See the performance section below.
- **Well tested**: Mongita has 100% test coverage and more test code than library code.
- **Limited dependencies**: Mongita runs anywhere that Python runs. Currently the only dependencies are `pymongo` (for bson) and `sortedcontainers` (for faster indexes).
- **Thread-safe**: (EXPERIMENTAL) Mongita avoids race conditions by isolating certain document modification operations.

### When NOT to use Mongita
- **You need a traditional server/client relationship**: Mongita is an embedded database. It is not process-safe. When you have multiple clients, a traditional server/client database is the correct choice.
- **You run a lot of uncommon commands**: Mongita implements a commonly used subset of MongoDB. While the goal is to eventually implement most of it, it will take some time to get there.
- **You need extreme performance**: Mongita has comparable performance to MongoDB and SQLite for common operations. However, it's possible you'll find bottlenecks - especially with uncommon operations. 

### Installation

```bash
pip3 install mongita
```

###  Hello world

```python
>>> from mongita import MongitaClientDisk
>>> client = MongitaClientDisk()
>>> hello_world_db = client.hello_world_db
>>> mongoose_collection = hello_world_db.mongoose_collection
>>> mongoose_collection.insert_many([{'name': 'Meercat', 'does_not_eat': 'Snakes'},
                                     {'name': 'Yellow mongoose', 'eats': 'Termites'}])
<mongita.results.InsertManyResult object at 0x000000000>
>>> mongoose_collection.count_documents({})
2
>>> mongoose_collection.update_one({'name': 'Meercat'}, {'$set': {"weight": 2}})
<mongita.results.UpdateResult object at 00000000000>
>>> mongoose_collection.find({'weight': {'$gt': 1}})
<mongita.cursor.Cursor object at 00000000000>
>>> list(mongoose_collection.find({'weight': {'$gt': 1}}))
[{'_id': 'a1b2c3d4e5f6', 'name': 'Meercat', 'does_not_eat': 'Snakes', 'weight': 2}]
>>> mongoose_collection.delete_one({'name': 'Meercat'})
<mongita.results.DeleteResult object at 00000000000>
```

### Performance

![Inserts and access](https://raw.githubusercontent.com/scottrogowski/mongita/master/assets/performance_comparison_inserts_and_access.svg)
![Finds](https://raw.githubusercontent.com/scottrogowski/mongita/master/assets/performance_comparison_finds.svg)
![Updates and deletes](https://raw.githubusercontent.com/scottrogowski/mongita/master/assets/performance_comparison_updates_and_deletes.svg)
![Cold start](https://raw.githubusercontent.com/scottrogowski/mongita/master/assets/performance_comparison_cold_starts.svg)

### API

Refer to the [PyMongo docs](https://pymongo.readthedocs.io/en/stable/api/index.html) for detailed syntax and behavior. Most named keyword parameters are *not implemented*. When something is not implemented, efforts are made to be loud and obvious about it.

**mongita.MongitaClientMemory / mongita.MongitaClientDisk** ([PyMongo docs](https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html))

```python
mongita.MongitaClient.close()
mongita.MongitaClient.list_database_names()
mongita.MongitaClient.list_databases()
mongita.MongitaClient.drop_database(name_or_database)
```

Note: By default, MongitaClientDisk stores its data in ~/.mongita. To use a different directory, pass `host` when initializing `client = MongitaClientDisk(host=<db_path>)`.

**Database** ([PyMongo docs](https://pymongo.readthedocs.io/en/stable/api/pymongo/database.html))

```python
mongita.Database.list_collection_names()
mongita.Database.list_collections()
mongita.Database.drop_collection(name_or_collection)
```

**Collection** ([PyMongo docs](https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html))

```python
mongita.Collection.insert_one(document)
mongita.Collection.insert_many(documents, ordered=True)
mongita.Collection.find_one(filter, sort)
mongita.Collection.find(filter, sort, limit)
mongita.Collection.replace_one(filter, replacement, upsert=False)
mongita.Collection.update_one(filter, update)
mongita.Collection.update_many(filter, update)
mongita.Collection.delete_one(filter)
mongita.Collection.delete_many(filter)
mongita.Collection.count_documents(filter)
mongita.Collection.distinct(key, filter)
mongita.Collection.create_index(keys)
mongita.Collection.drop_index(index_or_name)
mongita.Collection.index_information()
```

**Cursor** ([PyMongo docs](https://pymongo.readthedocs.io/en/stable/api/pymongo/cursor.html))

```python
mongita.Cursor.sort(key_or_list, direction=None)
mongita.Cursor.next()
mongita.Cursor.limit(limit)
mongita.Cursor.clone()
mongita.Cursor.close()
```

**CommandCursor** ([PyMongo docs](https://pymongo.readthedocs.io/en/stable/api/pymongo/command_cursor.html))

```python
mongita.CommandCursor.next()
mongita.CommandCursor.close()
```

**errors** ([PyMongo docs](https://pymongo.readthedocs.io/en/stable/api/pymongo/errors.html))

```python
mongita.errors.MongitaError (parent class of all errors)
mongita.errors.PyMongoError (alias of MongitaError)
mongita.errors.InvalidOperation
mongita.errors.OperationFailure
mongita.errors.DuplicateKeyError
mongita.errors.MongitaNotImplementedError
```

**results** ([PyMongo docs](https://pymongo.readthedocs.io/en/stable/api/pymongo/results.html))

```python
mongita.results.InsertOneResult
mongita.results.InsertManyResult
mongita.results.UpdateResult
mongita.results.DeleteResult
```

**Currently implemented query operators**

    $eq
    $gt
    $gte
    $in
    $lt
    $lte
    $ne
    $nin

**Currently implemented update operators**

    $set
    $inc

### Contributing

Mongita is an *excellent* project for open source contributors. There is a lot to do and it is easy to get started. In particular, the following tasks are high in priority:
- More testing. Try Mongita on a project and report any bugs. The unit tests are extensive but in-the-wild bugs are still possible.
- SQLite engine. SQLite beats Mongita in most benchmarks. Having it as an engine would provide a fast persistent database which would be a little less than a perfect MongoDB reproduction.
- Cache eviction. In the disk engine, cache is never evicted after it is read or written. This could cause issues when dealing with large databases.
- More [update operators](https://docs.mongodb.com/manual/reference/operator/update/#id1). Currently, only $set and $inc are implemented.
- More [query operators](https://docs.mongodb.com/manual/reference/operator/query/). Currently, only the "comparison operators" are implemented.
- [find_one_and_...](https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.find_one_and_replace) methods.
- [Aggregation pipelines](https://docs.mongodb.com/manual/reference/command/aggregate/).
- More [cursor methods](https://pymongo.readthedocs.io/en/stable/api/pymongo/cursor.html). Currently only sort, next, and limit are implemented.

You are welcome to email me at scottmrogowski@gmail.com if you are interested.

### License

BSD 3-clause. Mongita is free and open source for any purpose with basic restrictions related to liability, warranty, and endorsement.

### History

Mongita was started as a component of the [fastmap server](https://github.com/fastmap-io). [Fastmap](https://fastmap.io) offloads and parallelizes arbitrary Python functions on the cloud.

### Similar projects

- [TinyMongo](https://github.com/schapman1974/tinymongo): Python library. Attempts to replicate the MongoDB interface.
- [MontyDb](https://github.com/davidlatwe/montydb): Python library. Attempts to replicate the MongoDB interface.
- [UnQLite](https://unqlite.org/): Embedded NoSQL with Python bindings. Does not attempt to replicate the MongoDB interface. Very popular.
- [NeDB](https://github.com/louischatriot/nedb): Javascript library. Attempts to replicate the MongoDB interface. Very popular.
