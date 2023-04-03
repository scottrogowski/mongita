from . import collection
from . import cursor
from . import database
from . import errors
from . import mongita_client
from . import results
from . import read_concern
from . import write_concern
from . import mongitasync

mongita_client = mongita_client
MongitaClientMemory = mongita_client.MongitaClientMemory
MongitaClientDisk = mongita_client.MongitaClientDisk

database = database
collection = collection
cursor = cursor
errors = errors
results = results
read_concern = read_concern
write_concern = write_concern
mongitasync = mongitasync.mongitasync

ASCENDING = collection.ASCENDING
DESCENDING = collection.DESCENDING

UNIMPLEMENTED = ['ALL', 'CursorType', 'DeleteMany', 'DeleteOne', 'GEO2D', 'GEOHAYSTACK',
                 'GEOSPHERE', 'HASHED', 'IndexModel', 'InsertOne', 'MAX_SUPPORTED_WIRE_VERSION',
                 'MIN_SUPPORTED_WIRE_VERSION', 'MongoClient', 'MongoReplicaSetClient',
                 'OFF', 'ReadPreference', 'ReplaceOne', 'ReturnDocument', 'SLOW_ONLY',
                 'TEXT', 'UpdateMany', 'UpdateOne', 'WriteConcern', 'aggregation',
                 'auth', 'auth_aws', 'bulk', 'change_stream', 'client_options',
                 'client_session', 'collation', 'collection', 'command_cursor',
                 'common', 'compression_support', 'cursor', 'cursor_manager', 'database',
                 'driver_info', 'encryption_options', 'errors', 'get_version_string',
                 'has_c', 'helpers', 'ismaster', 'max_staleness_selectors', 'message',
                 'mongo_client', 'mongo_replica_set_client', 'monitor', 'monitoring',
                 'monotonic', 'network', 'operations', 'periodic_executor', 'pool',
                 'read_concern', 'read_preferences', 'response', 'results', 'saslprep',
                 'server', 'server_description', 'server_selectors', 'server_type',
                 'settings', 'socket_checker', 'son_manipulator', 'srv_resolver',
                 'ssl_context', 'ssl_match_hostname', 'ssl_support', 'thread_util',
                 'topology', 'topology_description', 'uri_parser', 'version',
                 'version_tuple', 'write_concern']

VERSION = "1.2.0"


def __getattr__(attr):
    if attr in UNIMPLEMENTED:
        msg = "%s is not yet implemented. Most PyMongo module-level " \
              "attributes/methods will never be implemented. See the Mongita " \
              "docs." % attr
        raise errors.MongitaNotImplementedError(msg)
    raise AttributeError()
