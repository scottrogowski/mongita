class InsertOneResult():
    def __init__(self, inserted_id):
        self.acknowledged = True
        self.inserted_id = inserted_id

    def __repr__(self):
        return "InsertOneResult(inserted_id=%r)" % self.inserted_id


class InsertManyResult():
    def __init__(self, documents):
        self.acknowledged = True
        self.inserted_ids = [d['_id'] for d in documents]

    def __repr__(self):
        return "InsertOneResult(inserted_ids=%r)" % self.inserted_ids


class UpdateResult():
    def __init__(self, matched_count, modified_count, upserted_id=None):
        self.acknowledged = True
        self.matched_count = matched_count
        self.modified_count = modified_count
        self.upserted_id = upserted_id

    def __repr__(self):
        return "UpdateResult(matched_count=%d)" % self.matched_count


class DeleteResult():
    def __init__(self, deleted_count):
        self.acknowledged = True
        self.deleted_count = deleted_count

    def __repr__(self):
        return "DeleteResult(deleted_count=%d)" % self.deleted_count
