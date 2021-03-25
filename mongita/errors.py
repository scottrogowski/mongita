class MongitaError(Exception):
    pass


class MongitaNotImplementedError(MongitaError, NotImplementedError):
    @staticmethod
    def create(cls, attr):
        msg = "%s.%s is not yet implemented. You can help." % (cls, attr)
        return MongitaNotImplementedError(msg)

    @staticmethod
    def create_client(cls, attr):
        msg = "%s.%s is not yet implemented. Most MongoClient attributes/methods will never be implemented because this is the key place where Mongita differs. See the Mongita docs." % (cls, attr)
        return MongitaNotImplementedError(msg)

    @staticmethod
    def create_depr(cls, attr):
        msg = "%s.%s is deprecated and will not be implemented in Mongita." % (cls, attr)
        return MongitaNotImplementedError(msg)


class InvalidOperation(MongitaError):
    pass
