import abc
import multiprocessing
import os
import pathlib

import bson

from .common import support_alert, Location, ok_name, MetaStorageObject
from .command_cursor import CommandCursor
from .database import Database
from .errors import MongitaNotImplementedError, InvalidName
from .engines import disk_engine, memory_engine

DEFAULT_STORAGE_DIR = os.path.join(pathlib.Path.home(), '.mongita_storage')


class MongitaClient(abc.ABC):
    UNIMPLEMENTED = ['HOST', 'PORT', 'address', 'arbiters', 'close', 'close_cursor', 'codec_options', 'database_names', 'event_listeners', 'fsync', 'get_database', 'get_default_database', 'is_locked', 'is_mongos', 'is_primary', 'kill_cursors', 'local_threshold_ms', 'max_bson_size', 'max_idle_time_ms', 'max_message_size', 'max_pool_size', 'max_write_batch_size', 'min_pool_size', 'next', 'nodes', 'primary', 'read_concern', 'read_preference', 'retry_reads', 'retry_writes', 'secondaries', 'server_info', 'server_selection_timeout', 'set_cursor_manager', 'start_session', 'unlock', 'watch', 'write_concern']
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self._metadata_location = Location(_id='$.metadata')
        self._cache = {}

    def __getattr__(self, attr):
        if attr in self.UNIMPLEMENTED:
            raise MongitaNotImplementedError.create_client("MongitaClient", attr)
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

    def _create(self, db_name):
        metadata = self.engine.download_metadata(self._metadata_location)
        if metadata:
            if db_name not in metadata['database_names']:
                metadata['database_names'].append(db_name)
                assert self.engine.upload_metadata(self._metadata_location, metadata)
            return
        metadata = MetaStorageObject({
            'options': {},
            'database_names': [db_name],
            'uuid': str(bson.ObjectId()),
        })
        assert self.engine.upload_metadata(self._metadata_location, metadata)

    @support_alert
    def close(self):
        self.engine.close()

    @support_alert
    def list_database_names(self):
        metadata = self.engine.download_metadata(self._metadata_location)
        if metadata:
            return metadata['database_names']
        return []

    @support_alert
    def list_databases(self):
        def cursor():
            for db_name in self.list_database_names():
                if db_name not in self._cache:
                    self._cache[db_name] = Database(db_name, self)
                yield self._cache[db_name]
        return CommandCursor(cursor())

    @support_alert
    def drop_database(self, name_or_database):
        if isinstance(name_or_database, Database):
            db_name = name_or_database.name
        else:
            db_name = name_or_database

        metadata = self.engine.download_metadata(self._metadata_location)
        if metadata and name_or_database in metadata['database_names']:
            metadata['database_names'].remove(name_or_database)
            assert self.engine.upload_metadata(self._metadata_location, metadata)
        location = Location(database=db_name)
        self.engine.delete_dir(location)
        try:
            del self._cache[db_name]
        except KeyError:
            pass


class MongitaClientDisk(MongitaClient):
    def __init__(self, bucket=DEFAULT_STORAGE_DIR):
        self.engine = disk_engine.DiskEngine(bucket)
        super().__init__()

    def __repr__(self):
        path = self.engine.base_storage_path
        return "MongitaClientDisk(path=%s)" % path


class MongitaClientMemory(MongitaClient):
    def __init__(self, strict=False):
        # TODO test strict
        self.engine = memory_engine.MemoryEngine(strict)
        super().__init__()

    def __repr__(self):
        pid = multiprocessing.current_process().pid
        return "MongitaClientMemory(pid=%s)" % pid
