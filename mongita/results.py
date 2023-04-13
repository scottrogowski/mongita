import pymongo.results


class InsertOneResult(pymongo.results.InsertOneResult):
    ...


class InsertManyResult(pymongo.results.InsertManyResult):
    ...


class UpdateResult(pymongo.results.UpdateResult):
    ...


class DeleteResult(pymongo.results.DeleteResult):
    ...
