import functools

from .errors import MongitaError

ASCENDING = 1
DESCENDING = -1


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
                raise MongitaError("The argument %r is not supported by %r in mongita. "
                                   "This may or may not be supported in PyMongo. "
                                   "If it is, you can help implement it." %
                                   k, func)
        return func(*args, **kwargs)
    return inner


class Location():
    """
    Thin generic wrapper for object locations. Utilized by different engines in
    different ways
    """

    __slots__ = ['database', 'collection', '_id', 'path']

    def __init__(self, database=None, collection=None, _id=None):
        self.database = database
        self.collection = collection
        self._id = _id
        self.path = '/'.join(filter(None, (database, collection, _id)))

    # @staticmethod
    # def from_path(self, path):
    #     parts = path.split('/')
    #     assert len(parts) <= 3
    #     return Location(*parts)

    # @property
    # def fake_path(self):
    #     return self.path.replace('/', '/')

    def is_in_collection(self, collection_location):
        return (self.is_in_collection_incl_metadata(collection_location)
                and not self._id.startswith('$'))

    def is_in_collection_incl_metadata(self, collection_location):
        return (self.database == collection_location.database
                and (not collection_location.collection
                     or self.collection == collection_location.collection))

    def __repr__(self):
        return f"Location(db={self.database} collection={self.collection}, _id={self._id}"

    def __hash__(self):
        return hash(self.path)

    def __eq__(self, other):
        return self.path == other.path
