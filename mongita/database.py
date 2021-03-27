import bson

from .collection import Collection
from .command_cursor import CommandCursor
from .common import support_alert, Location, ok_name, MetaStorageObject
from .errors import MongitaNotImplementedError, InvalidName


class Database():
    UNIMPLEMENTED = ['aggregate', 'codec_options', 'command', 'create_collection', 'dereference', 'drop_collection', 'get_collection', 'next', 'profiling_info', 'profiling_level', 'read_concern', 'read_preference', 'set_profiling_level', 'validate_collection', 'watch', 'with_options', 'write_concern']
    DEPRECATED = ['add_son_manipulator', 'add_user', 'authenticate', 'collection_names', 'current_op', 'error', 'eval', 'incoming_copying_manipulators', 'incoming_manipulators', 'last_status', 'logout', 'outgoing_copying_manipulators', 'outgoing_manipulators', 'previous_error', 'remove_user', 'reset_error_history', 'system_js', 'SystemJS']

    def __init__(self, db_name, client):
        self.name = db_name
        self.client = client
        self._engine = client.engine
        self._existence_verified = False
        self._base_location = Location(database=db_name)
        self._metadata_location = Location(database=db_name,
                                           _id='$.metadata')
        self._cache = {}

    def __repr__(self):
        return "Database(%s, %r)" % (repr(self.client), self.name)

    def __getattr__(self, attr):
        if attr in self.UNIMPLEMENTED:
            raise MongitaNotImplementedError.create("Database", attr)
        if attr in self.DEPRECATED:
            raise MongitaNotImplementedError.create_depr("Database", attr)
        return self[attr]

    def __getitem__(self, collection_name):
        try:
            return self._cache[collection_name]
        except KeyError:
            if not ok_name(collection_name):
                raise InvalidName("Collection cannot be named %r." % collection_name)
            coll = Collection(collection_name, self)
            self._cache[collection_name] = coll
            return coll

    def _create(self, coll_name):
        if self._existence_verified:
            return
        if not self._engine.doc_exists(self._metadata_location):
            self._engine.create_path(self._base_location)
            metadata = MetaStorageObject({
                'options': {},
                'collection_names': [coll_name],
                'uuid': str(bson.ObjectId()),
            })
            self._engine.upload_metadata(self._metadata_location, metadata)
        self.client._create(self.name)
        self._existence_verified = True

    @support_alert
    def list_collection_names(self):
        metadata_tup = self._engine.download_metadata(self._metadata_location)
        if metadata_tup:
            return metadata_tup[0]['collection_names']
        return []

    @support_alert
    def list_collections(self):
        def cursor():
            for coll_name in self.list_collection_names():
                if coll_name not in self._cache:
                    self._cache[coll_name] = Database(coll_name, self)
                yield self._cache[coll_name]
        return CommandCursor(cursor())

    @support_alert
    def drop_collection(self, collection):
        if isinstance(collection, Collection):
            collection = collection.name
        location = Location(database=self.name, collection=collection)
        self._engine.delete_dir(location)
        del self._cache[collection]
