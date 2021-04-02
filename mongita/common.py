import copy
import functools
import os
import re
import unicodedata

import bson
import sortedcontainers

from .errors import MongitaError

ASCENDING = 1
DESCENDING = -1


_windows_device_files = ('CON', 'AUX', 'COM1', 'COM2', 'COM3', 'COM4', 'LPT1',
                         'LPT2', 'LPT3', 'PRN', 'NUL')
_filename_ascii_strip_re = re.compile(r'[^A-Za-z0-9_.-]')


def _secure_filename(filename: str) -> str:
    """
    The idea of this is to ensure that the document_id doesn't do sketchy shit on
    the filesystem. This will probably be deleted soon.
    """
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
    # TODO we won't need this at all if we have only one file open per collection
    # and it will be faster too. Let's let this be for now.
    if (
        os.name == "nt"
        and filename
        and filename.split(".")[0].upper() in _windows_device_files
    ):
        filename = f"_{filename}"
    return filename


def ok_name(name):
    """
    In-line with MongoDB restrictions, names are not be allowed to start with
    '$' or 'system.'.
    https://docs.mongodb.com/manual/reference/limits/#Restriction-on-Collection-Names
    """
    if not name:
        return False
    if '$' in name:
        return False
    if name.startswith('system.'):
        return False
    return True


def support_alert(func):
    """
    Provide smart tips if the user tries to use un-implemented / deprecated
    known kwargs.
    """
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
    """To change the generation to a byte string for making the head of a file."""
    return x.to_bytes(8, 'big')


def int_from_bytes(xbytes: bytes) -> int:
    """To get the generation from a byte string that was at the head of a file."""
    return int.from_bytes(xbytes, 'big')


class StorageObject(dict):
    """
    Wrapper over the document dictionary that allows us to keep track of the
    document generation for concurrency reasons.
    """
    __slots__ = ['generation']

    def __init__(self, doc, generation=0):
        self.generation = generation
        super().__init__(doc)

    def to_storage(self, as_bson=False):
        """
        Return this document as something storable. In strict / disk mode,
        this should be bson. Otherwise, better performance by not encoding.
        """
        self.generation += 1
        if as_bson:
            return int_to_bytes(self.generation) + bson.encode(self)
        return self

    @staticmethod
    def from_storage(obj, from_bson=False):
        """
        Return a storage object given what we got out of storage
        """
        if from_bson:
            generation = int_from_bytes(obj[:8])
            doc = bson.decode(obj[8:])
            return StorageObject(doc, generation)
        return obj


class MetaStorageObject(StorageObject):
    """
    Subclass of the StorageObject with some extra handling for metadata.
    Specifically, indexes need extra steps to fully encode / decode.
    """

    def to_storage(self, as_bson=False):
        """
        Makes sure that the SortedDict indexes are bson-compatible
        """
        self.generation += 1
        if as_bson:
            new_metadata = copy.deepcopy(self)
            if 'indexes' in new_metadata:
                for idx_key in list(new_metadata['indexes'].keys()):
                    new_metadata['indexes'][idx_key]['idx'] = list(self['indexes'][idx_key]['idx'].items())
            return int_to_bytes(self.generation) + bson.encode(new_metadata)
        return self

    @staticmethod
    def from_storage(obj, from_bson=False):
        if from_bson:
            generation = int_from_bytes(obj[:8])
            doc = bson.decode(obj[8:])
            so = MetaStorageObject(doc, generation)
            so.decode_indexes()
            return so
        return obj

    def decode_indexes(self):
        """
        Changes the encoded indexes to SortedDicts
        """
        if 'indexes' in self:
            for idx_key in list(self['indexes'].keys()):
                self['indexes'][idx_key]['idx'] = sortedcontainers.SortedDict(self['indexes'][idx_key]['idx'])


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
        # TODO make _id safe for localfiles. Probably not an issue after one-file support
        self.path = os.path.join(*tuple(filter(None, (database, collection, _id and str(_id)))))

    def parent_path(self):
        """
        Returns the collection path of the location
        """
        return os.path.join(*tuple(filter(None, (self.database, self.collection))))

    def is_collection(self):
        return self.database and self.collection and not self._id

    def is_in_collection(self, collection_location):
        return (self.is_in_collection_incl_metadata(collection_location)
                and not str(self._id).startswith('$'))

    def is_in_collection_incl_metadata(self, collection_location):
        """
        Usually, we don't want metadata files in document counts, etc.
        Sometimes we do.
        """
        return (self.database == collection_location.database
                and (not collection_location.collection
                     or self.collection == collection_location.collection))

    def __repr__(self):
        return f"Location(db={self.database} collection={self.collection}, _id={self._id})"

    def __hash__(self):
        """ Needed to store the location as a key in the cache """
        return hash(self.path)

    def __eq__(self, other):
        """ Needed to store the location as a key in the cache """
        return self.path == other.path
