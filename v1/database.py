import bson

from .collection import Collection
from .command_cursor import CommandCursor
from .common import support_alert, ok_name, MetaStorageObject
from .errors import MongitaNotImplementedError, InvalidName


class Database():
    UNIMPLEMENTED = ['aggregate', 'codec_options', 'command', 'create_collection', 'dereference', 'get_collection', 'next', 'profiling_info', 'profiling_level', 'read_concern', 'read_preference', 'set_profiling_level', 'validate_collection', 'watch', 'with_options', 'write_concern']
    DEPRECATED = ['add_son_manipulator', 'add_user', 'authenticate', 'collection_names', 'current_op', 'error', 'eval', 'incoming_copying_manipulators', 'incoming_manipulators', 'last_status', 'logout', 'outgoing_copying_manipulators', 'outgoing_manipulators', 'previous_error', 'remove_user', 'reset_error_history', 'system_js', 'SystemJS']

    def __init__(self, db_name, client):
        self.name = db_name
        self.client = client
        self._engine = client.engine
        self._base_location = db_name
        self._cache = {}

    def __repr__(self):
        return "Database(%s, %r)" % (repr(self.client), self.name)

    def __getattr__(self, attr):
        if attr in self.UNIMPLEMENTED:
            raise MongitaNotImplementedError.create("Database", attr)
        if attr in self.DEPRECATED:
            raise MongitaNotImplementedError.create_depr("Database", attr)
        if attr == '_Collection__create':
            return self.__create
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

    def __create(self, coll_name):
        """
        Mongodb does not create anything until first insert. So when we insert
        something, this will create a small metadata file to basically just store
        our collection_names
        """
        metadata = self._engine.get_metadata(self._base_location)
        if metadata:
            if coll_name not in metadata['collection_names']:
                metadata['collection_names'].append(coll_name)
                assert self._engine.put_metadata(self._base_location, metadata)
            return
        self._engine.create_path(self._base_location)
        metadata = MetaStorageObject({
            'options': {},
            'collection_names': [coll_name],
            'uuid': str(bson.ObjectId()),
        })
        assert self._engine.put_metadata(self._base_location, metadata)
        self.client.__create(self.name)

    @support_alert
    def list_collection_names(self):
        """
        List every collection name.

        :rtype: list[str]
        """
        metadata = self._engine.get_metadata(self._base_location)
        if metadata:
            return metadata['collection_names']
        return []

    @support_alert
    def list_collections(self):
        """
        Returns a cursor to iterate over all collections.

        :rtype: CommandCursor
        """
        def cursor():
            for coll_name in self.list_collection_names():
                if coll_name not in self._cache:
                    self._cache[coll_name] = Collection(coll_name, self)
                yield self._cache[coll_name]
        return CommandCursor(cursor)

    @support_alert
    def drop_collection(self, name_or_collection):
        """
        Drop a collection.

        :param name_or_collection str|collection.Collection:
        :rtype: None
        """
        if isinstance(name_or_collection, Collection):
            collection = name_or_collection.name
        else:
            collection = name_or_collection
        self._engine.delete_dir(f'{self.name}.{collection}')
        metadata = self._engine.get_metadata(self._base_location)
        if metadata and collection in metadata['collection_names']:
            metadata['collection_names'].remove(collection)
            assert self._engine.put_metadata(self._base_location, metadata)
        try:
            self._cache[collection]._existence_verified = False
            del self._cache[collection]
        except KeyError:
            pass
