TODO mongita mascot

"Mongita is to MongoDB as SQLite is to SQL"

Mongita is a lightweight embedded document database that implements a commonly-used subset of the [MongoDB/PyMongo interface](https://pymongo.readthedocs.io/en/stable/). Mongita differs from MongoDB in that instead of being a server, Mongita is a self-contained Python library.  Mongita can be configured to store its documents either on the filesystem, in memory, or in [GCP Cloud Storage buckets](https://cloud.google.com/storage)). 

*Mongita is very much a project*. Please report any bugs. Anticipate breaking changes until version 1.0. Mongita is free and open source. [You can contribute!]((#contributing))

Applications:
- Embedded database: Mongita is a good alternative to [SQLite](https://www.sqlite.org/index.html) for embedded applications when a document database makes more sense than a relational one.
- Serverless storage: In many cases, maintaining a running database on the cloud is overkill. Mongita can store documents in GCP Cloud Storage buckets. This allows you to have a database on the cloud without the cost and overhead of an actual database instance.
- Unit testing: Mocking PyMongo/MongoDB is a pain. Worse, mocking can hide real bugs. By monkey-patching PyMongo with Mongita, unit tests can be more faithful while remaining isolated.
 
Design goals:
- MongoDB compatibility: Mongita implements a commonly used subset of the PyMongo API. This allows projects to be started with Mongita and later upgraded to MongoDB once they reach an appropriate scale.
- Truly serverless: Mongita does not require a server or run a process. It is just a Python library import - much like SQLite.
- Limited dependencies: Mongita runs anywhere that Python runs. Currently there are no dependencies for the core library and there will never be any compiled dependencies.

When NOT to use Mongita:
- You need extreme speed: Mongita is fast enough for most use cases. If you are dealing with hundreds of transactions per second, you probably want to use a standard MongoDB server.
- You run a lot of uncommon commands: Mongita implements a commonly used subset of MongoDB. While the goal is to eventually implement most of it, it will take some time to get there.

### Installation

In-memory and filesystem backed (MongitaClientLocal & MongitaClientMemory)1

    pip3 install mongita

In-memory, filesystem, and Cloud Storage backed (+ MongitaClientGCP)

    pip3 install mongita[gcp]


###  Hello world

    >>> from mongita import MongitaClientLocal
    >>> client = MongitaClientLocal()
    >>> db = client.hello_world_db
    >>> coll = db.common_phrases
    >>> coll.insert_many([{'phrase_id': 1, 'hello': 'world'}, {'phrase_id': 2, 'foo': 'bar'})
    InsertResult()
    >>> coll.count_documents()
    2
    >>> coll.update_one({'phrase_id': 1}, {'$set': {'hello': 'World!'}})
    UpdateResult()
    >>> coll.replace_one({'phrase_id': 1}, {'phrase_id': 1, 'HELLO': 'WORLD'}
    ReplaceResult
    >>> coll.find({'phrase_id': {'$gt': 1})
    Cursor
    >>> list(coll.find({'phrase_id': {'$gt': 1}))
    [{'_id': 'a1b2c3d4e5f6', 'phrase_id': 2, 'foo': 'bar'}]
    >>> coll.delete_one({'phrase_id': 1})
    DropResult

###  Importing / exporting from MongoDB

# MongoDB -> Mongita
$ mongodump --db my_db --uri mongodb://localhost:27017 --out /tmp/dump1
$ mongitarestore --db my_db --dir /tmp/dump1 --out ~/.mongita_storage

# Mongita -> MongoDB
$ mongitadump --db db_2 --dir ~/.mongita_storage_2 --out /tmp/dump2
$ mongorestore --db db_2 --dir /tmp/dump2 --uri mongodb://localhost:27017


### API

Refer to the [PyMongo docs](https://pymongo.readthedocs.io/en/stable/api/index.html) for detailed syntax and behavior. Most named keyword parameters are *not implemented*. When something is not implemented, efforts are made to be loud and obvious about it.

#### Currently implemented classes / methods:

MongitaClientMemory / MongitaClientLocal / MongitaClientGCP
- list_database_names
- list_databases
- drop_database

Database
- list_collection_names
- list_collections
- drop_collection

Collection
- insert_one
- insert_many
- find_one
- find
- replace_one
- update_one
- update_many
- delete_one
- delete_many
- count_documents
- distinct
- create_index
- drop_index

Cursor
- sort
- next
- limit

errors
- MongitaError
- MongitaNotImplementedError

results
- InsertOneResult
- InsertManyResult
- UpdateResult
- DeleteResult

#### Currently implemented query operators

- $eq   
- $gt   
- $gte  
- $in 
- $lt 
- $lte
- $ne   
- $nin 

#### Currently implemented update operators

- $set
- $inc

### Performance

Results from a side-by-side comparison on the same machine (MacBook Pro mid-2016)

### Contributing

Mongita is an *excellent* project for open source contributors. There is a lot to do and it is easy to get started. In particular, the following tasks are high in priority:
- More [update operators](https://docs.mongodb.com/manual/reference/operator/update/#id1). Currently, only $set and $inc are implemented.
- More [query operators](https://docs.mongodb.com/manual/reference/operator/query/). Currently, only the "comparison operators" are implemented.
- find_one_and_... methods.
- (Aggregation pipelines)[https://docs.mongodb.com/manual/reference/command/aggregate/].
- More (cursor methods)[https://pymongo.readthedocs.io/en/stable/api/pymongo/cursor.html]. Currently only sort, next, and limit are implemented.

I would also like to add an S3 MongitaClient but that would require S3 to support something like GCP Cloud Storage's ["generations"](https://cloud.google.com/storage/docs/generations-preconditions). Generations allow for conditional PUTs to avoid overwriting objects in race conditions. It might also be possible for a clever developer to implement a lockfile mechanism using of objects. Specifically, I think it might be possible to "obtain" a lock by deleting an object and "release" it by putting it back.

### License

BSD 3-clause. Mongita is free and open source for any purpose with basic restrictions related to liability, warranty, and endorsement.

### History

Mongita was started as a component of the [fastmap server](https://github.com/fastmap-io). [Fastmap](https://fastmap.io) offloads and parallelizes arbitrary Python functions on the cloud.  A design goal of the fastmap project is for clusters to scale to 0 when not in use to avoid cloud charges. Since a running database costs $30+ per month, using a "real" database was not an option. Mongita was designed to solve this challenge.

### Comparison / alternatives

Every project in this table is an embedded, document-oriented database with Python support.

| Project | 100% Python | MongoDB compatible | Distributed | 1k+ stars
---------------------------------------------------------
[Mongita](https://github.com/scottrogowski/Mongita)  | ✅ | ✅ | ✅ | 🚫
[UNQLITE](https://unqlite.org/)                      | 🚫 | 🚫 | 🚫 | ✅
[TinyDB](https://pypi.org/project/tinydb/)           | ✅ | 🚫 | 🚫 | ✅
[BlitzDB](https://blitzdb.readthedocs.io/en/latest/) | ✅ | 🚫 | 🚫 | 🚫
