import bson
import boto3
import botocore

from .engine_common import Engine, StorageObject


# TODO object lock requires versioning enabled?? WTF
# https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock-overview.html

class AWSEngine(Engine):
    def __init__(self, bucket):
        self.s3 = boto3.resource('s3')
        self.bucket = self.s3.Bucket(bucket)
        self._cache = {}

    def upload_doc(self, location, doc, generation=None):
        data = bson.dumps(doc)

        try:
            if generation:
                self.bucket.upload_fileobj(data, location.path) # TODO if_generation_match=generation)
            else:
                self.bucket.upload_fileobj(data, location.path)
        except botocore.exceptions.ClientError:
            return False
        return True

    def download_doc(self, location):
        CacheControl
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object
        # IfModifiedSince for avoiding unneccessary round trips to get the index or other items
        # IfNoneMatch (string) -- Return the object only if its entity tag (ETag) is different from the one specified, otherwise return a 304 (not modified).
        # There is no way to get whether the bucket has been modified so what we
        # really need to do is update the metadata on every insert operation
        # regardless of whether an index exists or not.
        # then, other clients can check the metadata first.

        # question is still how to manage everything with updating the index / etc.
        # I don't see a way to do it without a lock file. We should get a lock on
        # every insert.
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
        except botocore.exceptions.ClientError:
            success = False
        try:
            del self._cache[location]
        except KeyError:
            pass
        return success
