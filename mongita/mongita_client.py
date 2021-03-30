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


class MongitaClient(abc.ABC):
    UNIMPLEMENTED = ['HOST', 'PORT', 'address', 'arbiters', 'close', 'close_cursor', 'codec_options', 'database_names', 'event_listeners', 'fsync', 'get_database', 'get_default_database', 'is_locked', 'is_mongos', 'is_primary', 'kill_cursors', 'local_threshold_ms', 'max_bson_size', 'max_idle_time_ms', 'max_message_size', 'max_pool_size', 'max_write_batch_size', 'min_pool_size', 'next', 'nodes', 'primary', 'read_concern', 'read_preference', 'retry_reads', 'retry_writes', 'secondaries', 'server_info', 'server_selection_timeout', 'set_cursor_manager', 'start_session', 'unlock', 'watch', 'write_concern']
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self._existence_verified = False
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
        if self._existence_verified:
            return
        if not self.engine.doc_exists(self._metadata_location):
            metadata = MetaStorageObject({
                'options': {},
                'database_names': [db_name],
                'uuid': str(bson.ObjectId()),
            })
            self.engine.upload_metadata(self._metadata_location, metadata)
        self._existence_verified = True

    @support_alert
    def close(self):
        self.engine.close()

    @support_alert
    def list_database_names(self):
        metadata_tup = self.engine.download_metadata(self._metadata_location)
        if metadata_tup:
            return metadata_tup[0]['database_names']
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
    def drop_database(self, db):
        if isinstance(db, Database):
            db = db.name
        location = Location(database=db)
        self.engine.delete_dir(location)
        try:
            del self._cache[db]
        except KeyError:
            pass


class MongitaClientDisk(MongitaClient):
    def __init__(self, bucket=os.path.join(pathlib.Path.home(),
                                           '.mongita_storage')):
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
