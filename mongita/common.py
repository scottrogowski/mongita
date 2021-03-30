import copy
import functools
import os
import re
import unicodedata

import bson

from .errors import MongitaError

ASCENDING = 1
DESCENDING = -1


_windows_device_files = ('CON', 'AUX', 'COM1', 'COM2', 'COM3', 'COM4', 'LPT1',
                         'LPT2', 'LPT3', 'PRN', 'NUL')
_filename_ascii_strip_re = re.compile(r'[^A-Za-z0-9_.-]')


def _secure_filename(filename: str) -> str:
    r"""From werkzeug source"""
    filename = unicodedata.normalize("NFKD", filename)
    filename = filename.encode("ascii", "ignore").decode("ascii")
    for sep in os.path.sep, os.path.altsep:
        if sep:
            filename = filename.replace(sep, " ")
    filename = str(_filename_ascii_strip_re.sub("", "_".join(filename.split()))).strip(
        "._"
    )
    # on nt a couple of special files are present in each folder.  We
    # have to ensure that the target file is not such a filename.  In
    # this case we prepend an underline
    if (
        os.name == "nt"
        and filename
        and filename.split(".")[0].upper() in _windows_device_files
    ):
        filename = f"_{filename}"
    return filename


def ok_name(name):
    # https://docs.mongodb.com/manual/reference/limits/#Restriction-on-Collection-Names
    if not name:
        return False
    if '$' in name:
        return False
    if name.startswith('system.'):
        return False
    return True


def support_alert(func):
    @functools.wraps(func)
    def inner(*args, **kwargs):
        for k in kwargs:
            if k not in func.__code__.co_varnames:
                raise MongitaError("The argument %r is not supported by %r in Mongita. "
                                   "This may or may not be supported in PyMongo. "
                                   "If it is, you can help implement it." %
                                   k, func)
        return func(*args, **kwargs)
    return inner


def int_to_bytes(x: int) -> bytes:
    return x.to_bytes(8, 'big')


def int_from_bytes(xbytes: bytes) -> int:
    return int.from_bytes(xbytes, 'big')


class StorageObject(dict):
    __slots__ = ['generation']

    def __init__(self, doc, generation=0):
        self.generation = generation
        super().__init__(doc)

    def to_storage(self, strict=False):
        self.generation += 1
        if strict:
            return int_to_bytes(self.generation) + bson.encode(self)
        return self

    @staticmethod
    def from_storage(obj, strict=False):
        if strict:
            generation = int_from_bytes(obj[:8])
            doc = bson.decode(obj[8:])
            return StorageObject(doc, generation)
        return obj


class MetaStorageObject(StorageObject):
    def to_storage(self, strict=False):
        self.generation += 1
        if strict:
            new_metadata = copy.deepcopy(self.__dict__)
            if 'indexes' in new_metadata:
                for idx_key in list(new_metadata['indexes'].keys()):
                    new_metadata['indexes'][idx_key]['idx'] = list(self['indexes'][idx_key]['idx'].items())
            return int_to_bytes(self.generation) + bson.encode(new_metadata)
        return self

    @staticmethod
    def from_storage(obj, strict=False):
        if strict:
            generation = int_from_bytes(obj[:8])
            doc = bson.decode(obj[8:])
            so = MetaStorageObject(doc, generation)
            so.decode_indexes()
            return so
        return obj

    def decode_indexes(self):
        if 'indexes' in self:
            for idx_key in list(self['indexes'].keys()):
                self['indexes'][idx_key]['idx'] = dict(self['indexes'][idx_key]['idx'])


class Location():
    """
    Thin generic wrapper for object locations. Utilized by different engines in
    different ways
    """

    __slots__ = ['database', 'collection', '_id', 'path']

    def __init__(self, database=None, collection=None, _id=None):
        self.database = database and _secure_filename(database)
        self.collection = collection and _secure_filename(collection)
        self._id = _id
        # TODO secure _id
        self.path = os.path.join(*tuple(filter(None, (database, collection, _id and str(_id)))))

    # @staticmethod
    # def from_path(self, path):
    #     parts = path.split('/')
    #     assert len(parts) <= 3
    #     return Location(*parts)

    # @property
    # def fake_path(self):
    #     return self.path.replace('/', '/')

    def parent_path(self):
        return os.path.join(*tuple(filter(None, (self.database, self.collection))))

    def is_collection(self):
        return self.database and self.collection and not self._id

    def is_in_collection(self, collection_location):
        return (self.is_in_collection_incl_metadata(collection_location)
                and not str(self._id).startswith('$'))

    def is_in_collection_incl_metadata(self, collection_location):
        return (self.database == collection_location.database
                and (not collection_location.collection
                     or self.collection == collection_location.collection))

    def __repr__(self):
        return f"Location(db={self.database} collection={self.collection}, _id={self._id})"

    def __hash__(self):
        return hash(self.path)

    def __eq__(self, other):
        return self.path == other.path
