from .errors import MongitaNotImplementedError


class WriteConcern():
    def __init__(self, w=None, wtimeout=None, j=None, fsync=None):
        if any(p is not None for p in (w, wtimeout, j, fsync)):
            raise MongitaNotImplementedError("Mongita's WriteConcern is a dummy "
                                             "doesn't support parameters")

        self.w = None
        self.wtimeout = None
        self.j = None
        self.fsync = None

        self.acknowledged = True
        self.is_server_default = True

    @property
    def document(self):
        return {}
