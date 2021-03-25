import bson

from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import BadRequest

from .common import Location
from .engine_common import Engine, StorageObject


class GCPEngine(Engine):
    def __init__(self, bucket):
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(bucket)
        self._cache = {}

    def upload_doc(self, location, doc, generation=None):
        blob = self.bucket.blob(location.path)
        data = bson.dumps(doc)
        try:
            if generation:
                blob.upload_from_string(data, if_generation_match=generation)
            else:
                blob.upload_from_string(data)
        except BadRequest:
            return False
        return True

    def download_doc(self, location):
        blob = self.bucket.blob(location.path)
        blob.reload()

        doc = self._cache.get(location)
        if doc and doc.generation == blob.generation:
            return doc

        so = StorageObject(bson.loads(blob.download_as_bytes()), blob.generation)
        self._cache[location] = so
        return so

    def doc_exists(self, location):
        blob = self.bucket.blob(location.path)
        return blob.exists()

    def list_ids(self, prefix, limit=None):
        blobs = self.bucket.list_blobs(prefix=prefix.path)
        ret = []
        i = 0
        for blob in blobs:
            if i == limit:
                break
            ret.append(blob.name.rsplit('/', 1)[1])
            i += 1
        return ret

    def delete_doc(self, location):
        blob = self.bucket.blob(location.path)
        success = True
        try:
            blob.delete()
        except NotFound:
            success = False
        try:
            del self._cache[location]
        except KeyError:
            pass
        return success

    def delete_dir(self, location):
        # TODO what happens when we can't delete a blob?
        blobs = self.bucket.list_blobs(prefix=location.path)
        for blob in blobs:
            try:
                blob.delete()
            except NotFound:
                pass
            try:
                del self._cache[Location.from_path(blob.name)]
            except KeyError:
                pass


