from .errors import MongitaNotImplementedError, MongitaError, InvalidOperation
from .common import ASCENDING, DESCENDING


class Cursor():
    UNIMPLEMENTED = ['add_option', 'address', 'alive', 'allow_disk_use', 'batch_size', 'clone', 'close', 'collation', 'collection', 'comment', 'cursor_id', 'distinct', 'explain', 'hint', 'limit', 'max', 'max_await_time_ms', 'max_time_ms', 'min', 'remove_option', 'retrieved', 'rewind', 'session', 'skip', 'where']
    DEPRECATED = ['count', 'max_scan']

    def __init__(self, _find, filter):
        self._find = _find
        self._filter = filter
        self._sort = []
        self._limit = None
        self._cursor = None

    def __getattr__(self, attr):
        if attr in self.DEPRECATED:
            raise MongitaNotImplementedError.create_depr("Collection", attr)
        if attr in self.UNIMPLEMENTED:
            raise MongitaNotImplementedError.create("Cursor", attr)
        raise AttributeError()

    def __getitem__(self, val):
        raise MongitaNotImplementedError.create("Cursor", '__getitem__')

    def __iter__(self):
        for el in self._gen():
            yield el

    def __next__(self):
        return next(self._gen())

    def _gen(self):
        if self._cursor:
            return self._cursor
        print('precursor', self._cursor)
        self._cursor = self._find(filter=self._filter, sort=self._sort, limit=self._limit)
        print('cursor', self._cursor)
        return self._cursor

    def next(self):
        """
        https://pymongo.readthedocs.io/en/stable/api/pymongo/cursor.html
        """
        return next(self._gen())

    def sort(self, *args):
        """
        https://pymongo.readthedocs.io/en/stable/api/pymongo/cursor.html
        """
        if self._cursor:
            raise InvalidOperation("Cursor has already started and can't be sorted")

        if len(args) == 1 and isinstance(args[0], (list, tuple)) \
           and all(isinstance(tup, (list, tuple)) and len(tup) == 2 for tup in args[0]):
            self._sort = args[0]
        elif len(args) == 1 and isinstance(args[0], str):
            self._sort = [(args[0], ASCENDING)]
        elif len(args) == 2 and isinstance(args[0], str) and isinstance(args[1], int):
            self._sort = [(args[0], args[1])]
        else:
            raise MongitaError("Unsupported sort parameter format %r. See the docs." % str(args))
        for k, direction in self._sort:
            if not isinstance(k, str):
                raise MongitaError("Sort key(s) must be strings %r" % str(args))
            if direction not in (ASCENDING, DESCENDING):
                raise MongitaError("Sort direction(s) must be either ASCENDING (1) or DESCENDING (-1). Not %r" % direction)
        return self

    def limit(self, limit):
        """
        https://pymongo.readthedocs.io/en/stable/api/pymongo/cursor.html
        """
        if not isinstance(limit, int):
            raise TypeError('Limit must be an integer')

        if self._cursor:
            raise InvalidOperation("Cursor has already started and can't be sorted")

        self._limit = limit
        return self
