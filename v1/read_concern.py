from .errors import MongitaNotImplementedError


class ReadConcern():
    def __init__(self, level=None):
        if level is not None:
            raise MongitaNotImplementedError("Mongita's ReadConcern is a dummy "
                                             "doesn't support parameters")
        self.level = None

    @property
    def document(self):
        return {}
