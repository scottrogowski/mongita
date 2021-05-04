import pymongo
from . import mongita_client


def _resolve_client(connection_type, uri):
    """
    :param str connection_type:
    :param str uri:
    :rtype: mongita.MongitaClientDisk|pymongo.MongoClient
    """
    assert connection_type in ('mongita', 'mongodb')
    if connection_type == 'mongita':
        if uri:
            uri = uri.replace('file://', '/')
        return mongita_client.MongitaClientDisk(uri)
    return pymongo.MongoClient(host=uri or 'localhost')


def _resolve_collections(collections):
    """
    Split a list of raw collections into a list of database/collection tuples

    :param list[str] collections:
    :rtype: list[(str, str|None)]
    """
    ret = []
    for raw_collection in collections:
        attr_chain = raw_collection.split('.', 1)
        database = attr_chain[0]
        if len(attr_chain) == 2:
            ret.append((database, attr_chain[1]))
        else:
            ret.append((database, None))
    return ret


def _batch_docs(cursor, cnt=1000):
    """
    Batch a generator of documents into lists of length cnt

    :param generator[dict] cursor:
    :param int cnt:
    :rtype: generator[list[dict]]
    """
    ret = []
    for doc in cursor:
        ret.append(doc)
        if len(ret) == cnt:
            yield ret
            ret = []
    if ret:
        yield ret


def _confirm_loop(msg, logger):
    """
    Confirm in a loop that the user wants to do the thing.
    Returns a tuple of (yes/no, yesall)

    :param str msg:
    :param Logger logger:
    :rtype: (bool, bool)
    """
    while True:
        logger.log("%s (yes/yesall/no)", msg)
        confirm = input()
        if confirm.lower() == 'yesall':
            return True, True
        if confirm.lower() in ('yes', 'y'):
            return True, False
        if confirm.lower() in ('no', 'n'):
            return False, False


def _replace_collection(source, dest, database, collection, force, logger):
    """
    Replace a single collection at destination with the source's collection.
    Returns whether we want to 'force' going forward

    :param MongoClient|MongitaClientDisk source:
    :param MongoClient|MongitaClientDisk dest:
    :param str database:
    :param str collection:
    :param bool force:
    :param Logger logger:
    :rtype: bool
    """
    source_coll = source[database][collection]
    dest_coll = dest[database][collection]
    if force:
        logger.log("Replacing %s.%s (%d documents -> %d documents)...",
                   database, collection, source_coll.count_documents({}),
                   dest_coll.count_documents({}))
    else:
        confirm, force = _confirm_loop("Replace %s.%s? (%d documents -> %d documents)" %
                                       (database, collection,
                                        source_coll.count_documents({}),
                                        dest_coll.count_documents({})),
                                       logger)
        if not confirm:
            return False
    dest[database].drop_collection(collection)
    for doc_batch in _batch_docs(source_coll.find({})):
        dest_coll.insert_many(doc_batch)
    return force


class _Logger():
    def __init__(self, quiet):
        self.quiet = quiet

    def log(self, msg, *args):
        if not self.quiet:
            msg = msg % args
            print("MONGITASYNC: %s" % (msg))


def mongitasync(source_type, destination_type, collections, force=False,
                source_uri=None, destination_uri=None, quiet=False):
    """
    Sync a list of collections from the source to the destination.
    Source/destination can be either 'mongita' or 'mongodb'
    Collections can be formatted as either 'db.coll' or plain 'db'

    :param str source_type: mongita|mongodb
    :param str destination_type: mongita|mongodb
    :param list[str]|str collections:
    :param bool force:
    :param str source_uri:
    :param str destination_uri:
    :param bool quiet:
    """
    source = _resolve_client(source_type, source_uri)
    destination = _resolve_client(destination_type, destination_uri)

    if not collections:
        raise AssertionError("No collections provided")
    if not isinstance(collections, list):
        collections = [collections]

    logger = _Logger(quiet)
    logger.log("Syncing %d databases/collections from %r (%s) to %r (%s):",
               len(collections), source_type, source_uri,
               destination_type, destination_uri)
    for collection in collections:
        logger.log('  ' + collection)

    for database, collection in _resolve_collections(collections):
        if collection:
            force = _replace_collection(source, destination, database, collection,
                                        force, logger)
            continue

        db_collections = list(source[database].list_collection_names())
        if force:
            destination.drop_database(database)
        else:
            confirm, force = _confirm_loop("Drop database %r on %r?" %
                                           (database, destination), logger)
            if confirm:
                destination.drop_database(database)

        for collection in db_collections:
            force = _replace_collection(source, destination, database, collection,
                                        force, logger)
