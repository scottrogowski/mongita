from .errors import MongitaNotImplementedError
from .common import support_alert


class CommandCursor():
    UNIMPLEMENTED = ['address', 'alive', 'batch_size', 'cursor_id', 'session']

    def __init__(self, _generator):
        self._generator = _generator
        self._cursor = None

    def __getattr__(self, attr):
        if attr in self.UNIMPLEMENTED:
            raise MongitaNotImplementedError.create("CommandCursor", attr)
        raise AttributeError()

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
        self._cursor = self._generator()
        return self._cursor

    @support_alert
    def next(self):
        """
        Returns the next item in the CommandCursor. Raises StopIteration if there
        are no more items.
        """
        return next(self._gen())

    @support_alert
    def close(self):
        """
        Close this cursor to free the memory
        """

        self._cursor = iter(())
