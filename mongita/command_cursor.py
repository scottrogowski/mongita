from .errors import MongitaNotImplementedError


class CommandCursor():
    UNIMPLEMENTED = ['address', 'alive', 'batch_size', 'close', 'cursor_id', 'next', 'session']

    def __init__(self, cursor):
        self.cursor = cursor

    def __getattr__(self, attr):
        if attr in self.UNIMPLEMENTED:
            raise MongitaNotImplementedError.create("Cursor", attr)
        raise AttributeError()

    def __iter__(self):
        for el in self.cursor:
            yield el
