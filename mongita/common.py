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
_invalid_names = re.compile(r'[/\. "$*<>:|?]')


def secure_filename(filename: str) -> str:
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
    if filename and filename.split(".")[0].upper() in _windows_device_files:
        filename = f"_{filename}"
    return filename


def ok_name(name):
    """
    In-line with MongoDB restrictions.
    https://docs.mongodb.com/manual/reference/limits/#std-label-restrictions-on-db-names
    https://docs.mongodb.com/manual/reference/limits/#Restriction-on-Collection-Names
    The prohibition on "system." names will be covered by the prohibition on '.'
    """
    if not name:
        return False
    if _invalid_names.search(name):
        return False
    if len(name) > 64:
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
                                   (k, func))
        return func(*args, **kwargs)
    return inner


class MetaStorageObject(dict):
    """
    Subclass of the StorageObject with some extra handling for metadata.
    Specifically, indexes need extra steps to fully encode / decode.
    """

    def __init__(self, doc):
        super().__init__(doc)

    def to_storage(self, as_bson=False):
        """
        Makes sure that the SortedDict indexes are bson-compatible
        """
        if as_bson:
            if 'indexes' in self:
                swap = {}
                self_indexes = self['indexes']
                for idx_key in self_indexes.keys():
                    swap[idx_key] = self_indexes[idx_key]['idx']
                    idx = map(lambda tup: (tup[0], list(tup[1])),
                              self_indexes[idx_key]['idx'].items())
                    self_indexes[idx_key]['idx'] = list(idx)
                ret = bson.encode(self)
                for idx_key, idx in swap.items():
                    self_indexes[idx_key]['idx'] = idx
                return ret
            return bson.encode(self)
        return self

    @staticmethod
    def from_storage(obj, from_bson=False):
        if from_bson:
            doc = bson.decode(obj)
            so = MetaStorageObject(doc)
            so.decode_indexes()
            return so
        return obj

    def decode_indexes(self):
        """
        Changes the encoded indexes to SortedDicts
        """
        if 'indexes' in self:
            self_indexes = self['indexes']
            for idx_key in self_indexes.keys():
                idx = list(map(lambda tup: (tuple(tup[0]), set(tup[1])),
                               self['indexes'][idx_key]['idx']))
                self_indexes[idx_key]['idx'] = sortedcontainers.SortedDict(idx)
