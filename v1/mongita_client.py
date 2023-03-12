import abc
import os
import pathlib

import bson

from .common import support_alert, ok_name, MetaStorageObject
from .command_cursor import CommandCursor
from .database import Database
from .errors import MongitaNotImplementedError, InvalidName
from .engines import disk_engine, memory_engine
from .read_concern import ReadConcern
from .write_concern import WriteConcern


DEFAULT_STORAGE_DIR = os.path.join(pathlib.Path.home(), '.mongita')


class MongitaClient(abc.ABC):
    UNIMPLEMENTED = ['HOST', 'PORT', 'address', 'arbiters', 'close', 'close_cursor',
                     'codec_options', 'database_names', 'event_listeners', 'fsync',
                     'get_database', 'get_default_database', 'is_locked',
                     'is_mongos', 'is_primary', 'kill_cursors',
                     'local_threshold_ms', 'max_bson_size', 'max_idle_time_ms',
                     'max_message_size', 'max_pool_size', 'max_write_batch_size',
                     'min_pool_size', 'next', 'nodes', 'primary',
                     'read_preference', 'retry_reads', 'retry_writes',
                     'secondaries', 'server_info', 'server_selection_timeout',
                     'set_cursor_manager', 'start_session', 'unlock', 'watch']
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self._base_location = ''
        self._cache = {}
        self._read_concern = ReadConcern()
        self._write_concern = WriteConcern()

    def __getattr__(self, attr):
        if attr in self.UNIMPLEMENTED:
            raise MongitaNotImplementedError.create_client("MongitaClient", attr)
        if attr == '_Database__create':
            return self.__create
        return self[attr]

    def __getitem__(self, db_name):
        try:
            return self._cache[db_name]
        except KeyError:
            if not ok_name(db_name):
                raise InvalidName("Database cannot be named %r." % db_name)
            db = Database(db_name, self)
            self._cache[db_name] = db
            return db

    def __create(self, db_name):
        """
        Mongodb does not create anything until first insert. So when we insert
        something, this will create a small metadata file to basically just store
        our database_names
        """
        metadata = self.engine.get_metadata(self._base_location)
        if metadata:
            if db_name not in metadata['database_names']:
                metadata['database_names'].append(db_name)
                assert self.engine.put_metadata(self._base_location, metadata)
            return
        metadata = MetaStorageObject({
            'options': {},
            'database_names': [db_name],
            'uuid': str(bson.ObjectId()),
        })
        assert self.engine.put_metadata(self._base_location, metadata)

    @property
    def read_concern(self):
        return self._read_concern

    @property
    def write_concern(self):
        return self._write_concern

    @support_alert
    def close(self):
        """
        Close the connection to the database and remove all cache.

        :rtype: None
        """
        self.engine.close()

    @support_alert
    def list_database_names(self):
        """
        List every database name.

        :rtype: list[str]
        """
        metadata = self.engine.get_metadata(self._base_location)
        if metadata:
            return metadata['database_names']
        return []

    @support_alert
    def list_databases(self):
        """
        Returns a cursor to iterate over all databases.

        :rtype: CommandCursor
        """
        def cursor():
            for db_name in self.list_database_names():
                if db_name not in self._cache:
                    self._cache[db_name] = Database(db_name, self)
                yield self._cache[db_name]
        return CommandCursor(cursor)

    @support_alert
    def drop_database(self, name_or_database):
        """
        Drop a database.

        :param name_or_database str|database.Database:
        :rtype: None
        """
        if isinstance(name_or_database, Database):
            db_name = name_or_database.name
        else:
            db_name = name_or_database

        db = self[db_name]
        for coll in list(db.list_collection_names()):
            db.drop_collection(coll)
        metadata = self.engine.get_metadata(self._base_location)
        if metadata and db_name in metadata['database_names']:
            metadata['database_names'].remove(db_name)
            assert self.engine.put_metadata(self._base_location, metadata)
        self.engine.delete_dir(db_name)
        self._cache.pop(db_name, None)


class MongitaClientDisk(MongitaClient):
    """
    The MongoClientDisk persists its state on the disk. It is meant to be
    compatible in most ways with pymongo's MongoClient.
    """
    def __init__(self, host=DEFAULT_STORAGE_DIR, **kwargs):
        host = host or DEFAULT_STORAGE_DIR
        if isinstance(host, list):
            # fix for mongoengine passing a list to us
            host = host[0]
        self.engine = disk_engine.DiskEngine.create(host)
        self.is_primary = True
        super().__init__()

    def __repr__(self):
        path = self.engine.base_storage_path
        return "MongitaClientDisk(path=%s)" % path


class MongitaClientMemory(MongitaClient):
    """
    The MongoClientMemory only holds its state in memory. Nonetheless, it is
    still thread-safe. It is meant to be compatible in most ways with
    pymongo's MongoClient.
    """
    def __init__(self, host=':memory:', strict=False, **kwargs):
        self.engine = memory_engine.MemoryEngine.create(strict)
        self.is_primary = True
        super().__init__()

    def __repr__(self):
        return "MongitaClientMemory()"
