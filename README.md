TODO mongita mascot

Mongita is a lightweight embedded document database that implements a commonly-used subset of the [MongoDB/PyMongo interface](https://pymongo.readthedocs.io/en/stable/). Mongita differs from MongoDB in that instead of being a server, Mongita is a self-contained Python library.  Mongita can be configured to store its documents either on the filesystem or simply in memory.

| "Mongita is to MongoDB as SQLite is to SQL"

*Mongita is very much a project*. Please report any bugs. Anticipate breaking changes until version 1.0. Mongita is free and open source. [You can contribute!]((#contributing))

Applications:
- Embedded database: Mongita is a good alternative to [SQLite](https://www.sqlite.org/index.html) for embedded applications when a document database makes more sense than a relational one.
- Unit testing: Mocking PyMongo/MongoDB is a pain. Worse, mocking can hide real bugs. By monkey-patching PyMongo with Mongita, unit tests can be more faithful while remaining isolated.
 
Design goals:
- MongoDB compatibility: Mongita implements a commonly-used subset of the PyMongo API. This allows projects to be started with Mongita and later upgraded to MongoDB once they reach an appropriate scale.
- Embedded/self-contained: Mongita does not require a server or run a process. It is just a Python library import - much like SQLite.
- Thread and process safe: Mongita avoids race conditions by isolating certain document modification operations.
- Limited dependencies: Mongita runs anywhere that Python runs. Currently the only dependency is PyMongo (for bson).

When NOT to use Mongita:
- You need extreme speed: Mongita is fast enough for most use cases. If you are dealing with hundreds of transactions per second on your local machine, you probably want to use a standard MongoDB server.
- You run a lot of uncommon commands: Mongita implements a commonly used subset of MongoDB. While the goal is to eventually implement most of it, it will take some time to get there.

### Installation

    pip3 install mongita

###  Hello world

    >>> from mongita import MongitaClientLocal
    >>> client = MongitaClientLocal()
    >>> hello_world_db = client.hello_world_db
    >>> mongoose_types = hello_world_db.mongoose_types
    >>> mongoose_types.insert_many([{'name': 'Meercat', 'favorite_food', 'Worms'})
    InsertResult()
    >>> mongoose_types.count_documents()
    2
    >>> mongoose_types.update_one({'phrase_id': 1}, {'$set': {'hello': 'World!'}})
    UpdateResult()
    >>> mongoose_types.replace_one({'phrase_id': 1}, {'phrase_id': 1, 'HELLO': 'WORLD'}
    ReplaceResult
    >>> mongoose_types.find({'phrase_id': {'$gt': 1})
    Cursor
    >>> list(coll.find({'phrase_id': {'$gt': 1}))
    [{'_id': 'a1b2c3d4e5f6', 'phrase_id': 2, 'foo': 'bar'}]
    >>> coll.delete_one({'phrase_id': 1})
    DropResult

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

TODO memory / local / pymongo+mongodb

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

Mongita was started as a component of the [fastmap server](https://github.com/fastmap-io). [Fastmap](https://fastmap.io) offloads and parallelizes arbitrary Python functions on the cloud.
