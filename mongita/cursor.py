from .errors import MongitaNotImplementedError, MongitaError, InvalidOperation
from .common import ASCENDING, DESCENDING, support_alert


class Cursor():
    UNIMPLEMENTED = ['add_option', 'address', 'alive', 'allow_disk_use', 'batch_size', 'clone', 'collation', 'collection', 'comment', 'cursor_id', 'distinct', 'explain', 'hint', 'limit', 'max', 'max_await_time_ms', 'max_time_ms', 'min', 'remove_option', 'retrieved', 'rewind', 'session', 'skip', 'where']
    DEPRECATED = ['count', 'max_scan']

    def __init__(self, _find, filter, sort, limit):
        self._find = _find
        self._filter = filter
        self._sort = sort or []
        self._limit = limit or None
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
        """
        This exists so that we can maintain our position in the cursor and
        to not execute until we start requesting items
        """
        if self._cursor:
            return self._cursor
        self._cursor = self._find(filter=self._filter, sort=self._sort, limit=self._limit)
        return self._cursor

    @support_alert
    def next(self):
        """
        Returns the next document in the Cursor. Raises StopIteration if there
        are no more documents.

        :rtype: dict
        """
        return next(self._gen())

    @support_alert
    def sort(self, key_or_list, direction=None):
        """
        Apply a sort to the cursor. Sorts have no impact until retrieving the
        first document from the cursor. If not sorting against indexes, sort can
        negatively impact performance.
        This returns the same cursor to allow for chaining. Only the last sort
        is applied.

        :param key_or_list str|[(key, direction)]:
        :param direction mongita.ASCENDING|mongita.DESCENDING:
        :rtype: cursor.Cursor
        """
        if self._cursor:
            raise InvalidOperation("Cursor has already started and can't be sorted")

        if direction is None and isinstance(key_or_list, (list, tuple)) \
           and all(isinstance(tup, (list, tuple)) and len(tup) == 2 for tup in key_or_list):
            self._sort = key_or_list
        elif direction is None and isinstance(key_or_list, str):
            self._sort = [(key_or_list, ASCENDING)]
        elif isinstance(key_or_list, str) and isinstance(direction, int):
            self._sort = [(key_or_list, direction)]
        else:
            raise MongitaError("Unsupported sort parameter format. See the docs.")
        for sort_key, sort_direction in self._sort:
            if not isinstance(sort_key, str):
                raise MongitaError("Sort key(s) must be strings %r" % str(key_or_list))
            if sort_direction not in (ASCENDING, DESCENDING):
                raise MongitaError("Sort direction(s) must be either ASCENDING (1) or DESCENDING (-1). Not %r" % direction)
        return self

    @support_alert
    def limit(self, limit):
        """
        Apply a limit to the number of elements returned from the cursor.
        This returns the same cursor to allow for chaining. Only the last limit
        is applied.

        :param limit int:
        :rtype: cursor.Cursor
        """
        if not isinstance(limit, int):
            raise TypeError('Limit must be an integer')

        if self._cursor:
            raise InvalidOperation("Cursor has already started and can't be sorted")

        self._limit = limit
        return self

    @support_alert
    def close(self):
        """
        Close this cursor to free the memory
        """
        def empty_cursor():
            return
            yield

        self._cursor = empty_cursor()
