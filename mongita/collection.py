import collections

import bson
import sortedcontainers
import functools
import operator

from .cursor import Cursor
from .common import support_alert, Location, ASCENDING, DESCENDING, StorageObject, MetaStorageObject
from .errors import MongitaError, MongitaNotImplementedError, DuplicateKeyError, OperationFailure
from .results import InsertOneResult, InsertManyResult, DeleteResult, UpdateResult


_SUPPORTED_FILTER_OPERATORS = ('$in', '$eq', '$gt', '$gte', '$lt', '$lte', '$ne', '$nin')
_SUPPORTED_UPDATE_OPERATORS = ('$set', '$inc')


def _validate_filter(filter):
    if not isinstance(filter, dict):
        raise MongitaError("The filter parameter must be a dict, not %r" % type(filter))
    for k in filter.keys():
        if not isinstance(k, str):
            raise MongitaError("Filter keys must be strings, not %r" % type(filter))
    _id = filter.get('_id')
    if _id:
        if not isinstance(_id, (bson.ObjectId, str, dict)):
            raise MongitaError("If present, the '_id' filter must be a bson ObjectId, string, or a dict")
    for filter_v in filter.values():
        if isinstance(filter_v, dict):
            for op in filter_v.keys():
                if op not in _SUPPORTED_FILTER_OPERATORS:
                    raise MongitaError(
                        "Mongita does not support %r. These filter operators are "
                        "supported: %r" % (k, _SUPPORTED_FILTER_OPERATORS))


def _validate_update(update):
    if not isinstance(update, dict):
        raise MongitaError("The update parameter must be a dict, not %r" % type(update))
    for k in update.keys():
        if k in _SUPPORTED_UPDATE_OPERATORS:
            continue
        raise MongitaError(
            "Mongita does not support %r. These update operators are "
            "supported: %r" % (k, _SUPPORTED_UPDATE_OPERATORS))
    for update_dict in update.values():
        if not isinstance(update_dict, dict):
            raise MongitaError("If present, the update operator must be a dict, not %r" % type(update_dict))
        _id = update_dict.get('_id')
        if _id:
            if not isinstance(_id, (str, bson.ObjectId)):
                raise MongitaError("The update _id must be a bson ObjectId or a string")


def _validate_doc(doc):
    if not isinstance(doc, dict):
        raise MongitaError("The document must be a dict, not %r" % type(doc))
    _id = doc.get('_id')
    if _id:
        if not isinstance(_id, (bson.ObjectId, str)):
            raise MongitaError("The document _id must be a bson ObjectId, a string, or not present")


def _doc_matches_agg(doc_v, agg):
    for agg_k, agg_v in agg.items():
        if agg_k == '$in':
            if not isinstance(agg_v, (list, tuple, set)):
                raise MongitaError("'$in' requires an iterable")
            if doc_v not in agg_v:
                return False
        elif agg_k == '$nin':
            if not isinstance(agg_v, (list, tuple, set)):
                raise MongitaError("'$nin' requires an iterable")
            if doc_v in agg_v:
                return False
        elif agg_k == '$eq':
            if doc_v != agg_v:
                return False
        elif agg_k == '$ne':
            if doc_v == agg_v:
                return False
        elif agg_k == '$lt':
            if doc_v >= agg_v:
                return False
        elif agg_k == '$lte':
            if doc_v > agg_v:
                return False
        elif agg_k == '$gt':
            if doc_v <= agg_v:
                return False
        elif agg_k == '$gte':
            if doc_v < agg_v:
                return False
        # agg_k check is in _validate_filter
    return True


def _doc_matches_filter(doc, filter):
    for filter_k, filter_v in filter.items():
        if isinstance(filter_v, dict):
            doc_v = _get_item_from_doc(doc, filter_k)
            if _doc_matches_agg(doc_v, filter_v):
                continue
            return False

        if _get_item_from_doc(doc, filter_k) == filter_v:
            continue
        return False
    return True


def idx_fltr_keys(matched_keys, idx, **kwargs):
    if matched_keys is not None:
        return matched_keys.intersection(set(idx.irange(**kwargs)))
    return set(idx.irange(**kwargs))


def idx_filter_sort(tup):
    k, _ = tup
    return (k == '$eq',
            k in ('$lt', '$lte', '$gt', '$gte'))


def _doc_ids_match_filter_in_idx(doc_idx, filter_v):
    """
    :param doc_idx {key: [ObjectId, ...]}:
    :param filter_v str|dict:
    :rtype: set
    """
    idx = doc_idx['idx']
    matched_keys = None
    if isinstance(filter_v, dict):
        for agg_k, agg_v in sorted(filter_v.items(),
                                   key=idx_filter_sort, reverse=True):
            if agg_k == '$eq':
                matched_keys = set((agg_v,)) if agg_v in (matched_keys or idx.keys()) else set()
            elif agg_k == '$ne':
                matched_keys = set(k for k in matched_keys or idx.keys() if k != agg_v)
            elif agg_k == '$lt':
                matched_keys = idx_fltr_keys(matched_keys, idx,
                                             maximum=agg_v, inclusive=(False, False))
            elif agg_k == '$lte':
                matched_keys = idx_fltr_keys(matched_keys, idx,
                                             maximum=agg_v, inclusive=(True, True))
            elif agg_k == '$gt':
                matched_keys = idx_fltr_keys(matched_keys, idx,
                                             minimum=agg_v, inclusive=(False, False))
            elif agg_k == '$gte':
                matched_keys = idx_fltr_keys(matched_keys, idx,
                                             minimum=agg_v, inclusive=(True, True))
            elif agg_k == '$in':
                if not isinstance(agg_v, (list, tuple, set)):
                    raise MongitaError("'$in' requires an iterable")
                matched_keys = set(k for k in matched_keys or idx.keys() if k in agg_v)
            elif agg_k == '$nin':
                if not isinstance(agg_v, (list, tuple, set)):
                    raise MongitaError("'$nin' requires an iterable")
                matched_keys = set(k for k in matched_keys or idx.keys() if k not in agg_v)
            # validation of options is done earlier
        ret = set()
        for k in matched_keys:
            ret.update(idx[k])
        return ret
    return set(idx.get(filter_v, set()))


def _apply_update(doc, update):
    for update_k, update_v in update.items():
        if update_k == '$set':
            for k, v in update['$set'].items():
                doc[k] = v
        elif update_k == '$inc':
            for k, v in update['$inc'].items():
                doc[k] += v
        # Should never get an update key we don't recognize b/c _validate_update

def _get_item_from_doc(doc, key):
    if '.' in key:
        item = doc
        for level in key.split('.'):
            item = item.get(level, {})
        return item or None
    return doc.get(key)


def _update_idx_doc_with_new_documents(documents, idx_doc):
    key_str = idx_doc['key_str']
    new_idx = sortedcontainers.SortedDict(idx_doc['idx'])

    for doc in documents:
        key = _get_item_from_doc(doc, key_str)
        new_idx.setdefault(key, []).append(doc['_id'])

    reverse = idx_doc['direction'] == DESCENDING
    idx_doc['idx'] = sortedcontainers.SortedDict(sorted(new_idx.items(), reverse=reverse))


def _remove_docs_from_idx_doc(doc_ids, idx_doc):
    for k in idx_doc['idx'].keys():
        idx_doc['idx'][k] = [d for d in idx_doc['idx'][k] if d not in doc_ids]
    return idx_doc


def _sorter(ret, sort_list):
    # from https://docs.python.org/3/howto/sorting.html
    for k, direction in reversed(sort_list):
        if direction == ASCENDING:
            ret.sort(key=lambda el: el.get(k))
        elif direction == DESCENDING:
            ret.sort(key=lambda el: el.get(k), reverse=True)
        # validation on direction happens in cursor


class Collection():
    UNIMPLEMENTED = ['aggregate', 'aggregate_raw_batches', 'bulk_write', 'codec_options', 'create_indexes', 'drop', 'drop_indexes', 'ensure_index', 'estimated_document_count', 'find_one_and_delete', 'find_one_and_replace', 'find_one_and_update', 'find_raw_batches', 'inline_map_reduce', 'list_indexes', 'map_reduce', 'next', 'options', 'read_concern', 'read_preference', 'rename', 'watch', 'with_options', 'write_concern']
    DEPRECATED = ['reindex', 'parallel_scan', 'initialize_unordered_bulk_op', 'initialize_ordered_bulk_op', 'group', 'count', 'insert', 'save', 'update', 'remove', 'find_and_modify', 'ensure_index']

    def __init__(self, collection_name, database):
        self.name = collection_name
        self.database = database
        self._engine = database._engine
        self._existence_verified = False
        self._base_location = Location(database=database.name,
                                       collection=collection_name)
        self._metadata_location = Location(database=database.name,
                                           collection=collection_name,
                                           _id='$.metadata')

    def __repr__(self):
        return "Collection(%s, %r)" % (repr(self.database), self.name)

    def __getattr__(self, attr):
        if attr in self.DEPRECATED:
            raise MongitaNotImplementedError.create_depr("Collection", attr)
        if attr in self.UNIMPLEMENTED:
            raise MongitaNotImplementedError.create("Collection", attr)
        return Collection(collection_name=self.name + '.' + attr,
                          database=self.database)

    @property
    def full_name(self):
        return self.database.name + '.' + self.name

    def _get_location(self, object_id):
        return Location(self.database.name, self.name, object_id)

    def _create(self):
        if self._existence_verified:
            return
        with self._engine.lock:
            if not self._engine.doc_exists(self._metadata_location):
                self._engine.create_path(self._base_location)
                metadata = MetaStorageObject({
                    'options': {},
                    'indexes': {},
                    '_id': str(bson.ObjectId()),
                })
                assert self._engine.upload_metadata(self._metadata_location, metadata)
            self.database._create(self.name)
        self._existence_verified = True

    def _insert_one(self, document):
        document_location = self._get_location(document['_id'])
        success = self._engine.upload_doc(document_location, document,
                                          if_gen_match=True)
        if not success:
            # TODO is this the only reason it would fail? I don't think so.
            print("duplicate key or something")
            raise DuplicateKeyError("Document %r already exists" % document['_id'])

    @support_alert
    def insert_one(self, document):
        _validate_doc(document)
        document = StorageObject(document)
        document['_id'] = document.get('_id') or bson.ObjectId()
        self._create()
        with self._engine.lock:
            metadata = self._get_metadata()
            self._insert_one(document)
            self._update_indicies([document], metadata)
        return InsertOneResult(document['_id'])

    @support_alert
    def insert_many(self, documents, ordered=True):
        """
        :param list documents:
        :param bool ordered:

        Insert documents. If ordered, stop inserting if there is an error.
        If not ordered, all operations are attempted
        """
        if not isinstance(documents, list):
            raise MongitaError("Documents must be a list")
        ready_docs = []
        for doc in documents:
            _validate_doc(doc)
            doc = StorageObject(doc)
            doc['_id'] = doc.get('_id') or bson.ObjectId()
            ready_docs.append(doc)
        self._create()
        success_docs = []
        exception = None
        with self._engine.lock:
            metadata = self._get_metadata()
            for doc in ready_docs:
                try:
                    self._insert_one(doc)
                    success_docs.append(doc)
                except Exception as ex:
                    if ordered:
                        self._update_indicies(success_docs, metadata)
                        raise MongitaError("Ending insert_many because of error") from ex
                    exception = ex
                    continue
            self._update_indicies(success_docs, metadata)
        if exception:
            raise MongitaError("Not all documents inserted") from exception
        return InsertManyResult(success_docs)

    @support_alert
    def replace_one(self, filter, replacement, upsert=False):
        filter = filter or {}
        _validate_filter(filter)
        _validate_doc(replacement)
        self._create()

        replacement = StorageObject(replacement)

        doc_id = self._find_one_id(filter)
        if not doc_id:
            if upsert:
                with self._engine.lock:
                    metadata = self._get_metadata()
                    replacement['_id'] = replacement.get('_id') or bson.ObjectId()
                    self._insert_one(replacement)
                    self._update_indicies([replacement], metadata)
                return UpdateResult(0, 1, replacement['_id'])
            return UpdateResult(0, 0)
        replacement['_id'] = doc_id
        with self._engine.lock:
            metadata = self._get_metadata()
            assert self._engine.upload_doc(self._get_location(doc_id), replacement)
            self._update_indicies([replacement], metadata)
            return UpdateResult(1, 1)

    def _find_one_id(self, filter):
        """
        :param filter dict:
        :rtype: str|None

        Given the filter, return a single object_id or None.
        """

        if not filter:
            return self._engine.find_one_id(self._base_location)

        if '_id' in filter:
            location = self._get_location(filter['_id'])
            if self._engine.doc_exists(location):
                return location._id
            return None

        try:
            return next(self._find_ids(filter))
        except StopIteration:
            return None

    def _find_one(self, filter):
        doc_id = self._find_one_id(filter)
        if doc_id:
            doc = self._engine.download_doc(self._get_location(doc_id))
            if doc:
                return dict(doc)

    def _find_ids(self, filter, sort=None, limit=None, metadata=None):
        """
        :param filter dict:
        :param sort bool:

        Given a filter, find all the document ids that match this filter.
        This will, in some cases, download documents. These are cached in the
        storage layer so performance hits are minimal.
        """
        filter = filter or {}
        sort = sort or []

        if limit == 0:
            return

        reg_filters = {}
        idx_filters = {}
        metadata = metadata or self._get_metadata()
        indexes = metadata.get('indexes', {})
        for filter_k, filter_v in filter.items():
            if filter_k + '_1' in indexes:
                idx_filters[filter_k + '_1'] = filter_v
            elif filter_k + '_-1' in indexes:
                idx_filters[filter_k + '_-1'] = filter_v
            else:
                reg_filters[filter_k] = filter_v
        if idx_filters:
            doc_ids_from_all_idx_filters = []
            for idx_k, filter_v in idx_filters.items():
                doc_idx = indexes[idx_k]
                doc_ids = _doc_ids_match_filter_in_idx(doc_idx, filter_v)
                if not doc_ids:
                    return
                doc_ids_from_all_idx_filters.append(doc_ids)
            doc_ids = set.intersection(*doc_ids_from_all_idx_filters)
        else:
            doc_ids = self._engine.list_ids(self._base_location)

        if sort:
            ret = []
            for doc_id in doc_ids:
                doc = self._engine.download_doc(self._get_location(doc_id))
                if _doc_matches_filter(doc, reg_filters):
                    ret.append(doc)
            _sorter(ret, sort)
            if limit is None:
                for doc in ret:
                    yield doc['_id']
            else:
                i = 0
                for doc in ret:
                    yield doc['_id']
                    i += 1
                    if i == limit:
                        return
            return

        if limit is None:
            for doc_id in doc_ids:
                doc = self._engine.download_doc(self._get_location(doc_id))
                if _doc_matches_filter(doc, reg_filters):
                    yield doc['_id']
            return

        i = 0
        for doc_id in doc_ids:
            doc = self._engine.download_doc(self._get_location(doc_id))
            if _doc_matches_filter(doc, reg_filters):
                yield doc['_id']
                i += 1
                if i == limit:
                    return

    def _find(self, filter, sort=None, limit=None, metadata=None):
        for doc_id in self._find_ids(filter, sort, limit, metadata=metadata):
            doc = self._engine.download_doc(self._get_location(doc_id))
            yield dict(doc)

    @support_alert
    def find_one(self, filter=None):
        filter = filter or {}
        _validate_filter(filter)
        return self._find_one(filter)

    @support_alert
    def find(self, filter=None):
        filter = filter or {}
        _validate_filter(filter)
        return Cursor(self._find, filter)

    def _update_doc(self, doc_id, update):
        """
        Given a doc_id and an update dict, find the document and safely update it
        """
        loc = self._get_location(doc_id)
        doc = self._engine.download_doc(loc)
        _apply_update(doc, update)
        assert self._engine.upload_doc(loc, doc, if_gen_match=True)
        return dict(doc)

    @support_alert
    def update_one(self, filter, update, upsert=False):
        _validate_filter(filter)
        _validate_update(update)
        self._create()
        if upsert:
            raise MongitaNotImplementedError("Mongita does not support 'upsert' on update operations. Use `replace_one`.")

        doc_ids = list(self._find_ids(filter))
        matched_count = len(doc_ids)
        if not matched_count:
            return UpdateResult(matched_count, 0)
        with self._engine.lock:
            metadata = self._get_metadata()
            doc = self._update_doc(doc_ids[0], update)
            self._update_indicies([doc], metadata)
        return UpdateResult(matched_count, 1)

    @support_alert
    def update_many(self, filter, update, upsert=False):
        _validate_filter(filter)
        _validate_update(update)
        self._create()
        if upsert:
            raise MongitaNotImplementedError("Mongita does not support 'upsert' on update operations. Use `replace_one`.")

        success_docs = []
        matched_cnt = 0
        doc_ids = list(self._find_ids(filter))
        with self._engine.lock:
            metadata = self._get_metadata()
            for doc_id in doc_ids:
                doc = self._update_doc(doc_id, update)
                success_docs.append(doc)
                matched_cnt += 1
            self._update_indicies(success_docs, metadata)
        return UpdateResult(matched_cnt, len(success_docs))

    @support_alert
    def delete_one(self, filter):
        _validate_filter(filter)
        self._create()

        doc_id = self._find_one_id(filter)
        if not doc_id:
            return DeleteResult(0)

        loc = self._get_location(doc_id)
        with self._engine.lock:
            metadata = self._get_metadata()
            self._engine.delete_doc(loc)
            self._update_indicies_deletes([doc_id], metadata)
        return DeleteResult(1)

    @support_alert
    def delete_many(self, filter):
        _validate_filter(filter)
        self._create()

        doc_ids = list(self._find_ids(filter))
        success_deletes = []
        with self._engine.lock:
            metadata = self._get_metadata()
            for doc_id in doc_ids:
                if self._engine.delete_doc(self._get_location(doc_id)):
                    success_deletes.append(doc_id)
            self._update_indicies_deletes(success_deletes, metadata)
        return DeleteResult(len(success_deletes))

    @support_alert
    def count_documents(self, filter):
        _validate_filter(filter)
        return len(list(self._find_ids(filter)))

    @support_alert
    def distinct(self, key, filter=None):
        if not isinstance(key, str):
            raise MongitaError("The 'key' parameter must be a string")
        filter = filter or {}
        _validate_filter(filter)
        uniq = set()
        for doc in self.find(filter):
            uniq.add(_get_item_from_doc(doc, key))
        uniq.discard(None)
        return list(uniq)

    def _get_metadata(self):
        """
        Get metadata
        """
        return self._engine.download_metadata(self._metadata_location) or {}

    def _update_indicies_deletes(self, doc_ids, metadata):
        if not doc_ids:
            return metadata
        for idx_doc in metadata.get('indexes', {}).values():
            _remove_docs_from_idx_doc(doc_ids, idx_doc)
        assert self._engine.upload_metadata(self._metadata_location, metadata)
        return metadata

    def _update_indicies(self, documents, metadata):
        for idx_doc in metadata.get('indexes', {}).values():
            _update_idx_doc_with_new_documents(documents, idx_doc)
        assert self._engine.upload_metadata(self._metadata_location, metadata)
        return metadata

    @support_alert
    def create_index(self, keys):
        if isinstance(keys, str):
            keys = [(keys, ASCENDING)]
        if not isinstance(keys, list) or keys == []:
            raise MongitaError("Unsupported keys parameter format %r. See the docs." % str(keys))
        if len(keys) > 1:
            raise MongitaNotImplementedError("Multi-key indexes are not supported")
        for k, direction in keys:
            if not k or not isinstance(k, str):
                raise MongitaError("Index keys must be strings %r" % str(k))
            if direction not in (ASCENDING, DESCENDING):
                raise MongitaError("Index key direction must be either ASCENDING (1) or DESCENDING (-1). Not %r" % direction)

        key_str, direction = keys[0]

        idx_name = f'{key_str}_{direction}'
        idx_doc = {
            '_id': idx_name,
            'key_str': key_str,
            'direction': direction,
            'idx': {},
        }

        with self._engine.lock:
            metadata = self._get_metadata()
            _update_idx_doc_with_new_documents(self._find({}, metadata=metadata), idx_doc)
            metadata['indexes'][idx_name] = idx_doc
            assert self._engine.upload_metadata(self._metadata_location, metadata)
        return idx_name

    @support_alert
    def drop_index(self, index_or_name):
        if isinstance(index_or_name, (list, tuple)) \
           and len(index_or_name) == 1 \
           and len(index_or_name[0]) == 2 \
           and isinstance(index_or_name[0][0], str) \
           and isinstance(index_or_name[0][1], int):
            index_or_name = f'{index_or_name[0][0]}_{index_or_name[0][1]}'
        if not isinstance(index_or_name, str):
            raise MongitaError("Unsupported index_or_name parameter format. See the docs.")
        try:
            key, direction = index_or_name.split('_')
            direction = int(direction)
        except ValueError:
            raise MongitaError("Unsupported index_or_name parameter format. See the docs.")

        with self._engine.lock:
            metadata = self._get_metadata()
            del metadata['indexes'][index_or_name]
            assert self._engine.upload_metadata(self._metadata_location, metadata)

    @support_alert
    def index_information(self):
        ret = [{'_id_': {'key': [('_id', 1)]}}]
        metadata = self._get_metadata()
        for idx in metadata.get('indexes', {}).values():
            ret.append({idx['_id']: {'key': [(idx['key_str'], idx['direction'])]}})
        return ret
