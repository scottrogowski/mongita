import collections
import time

import bson

from .cursor import Cursor
from .common import support_alert, Location, ASCENDING, DESCENDING, StorageObject
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
        if filter_k not in doc:
            return False

        if isinstance(filter_v, dict):
            doc_v = doc[filter_k]
            if _doc_matches_agg(doc_v, filter_v):
                continue
            return False

        if doc[filter_k] == filter_v:
            continue
        return False
    return True


def _doc_ids_match_filter_in_idx(doc_idx, filter_v):
    """
    :param doc_idx {key: [ObjectId, ...]}:
    :param filter_v str|dict:
    :rtype: set
    """
    # TODO sortedcontainers.SortedDict
    # functools.reduce(operator.iconcat, (weights[el] for el in weights.irange(None,8)))
    if not doc_idx['idx']:
        return set()

    key_type = type(doc_idx['example_type'])
    example_key = next(doc_idx['idx'].keys().__iter__())
    if not isinstance(example_key, key_type):
        doc_idx['idx'] = {key_type(k): v for k, v in doc_idx['idx'].items()}
    idx = doc_idx['idx']

    if isinstance(filter_v, dict):
        idx_keys = list(idx.keys())
        for agg_k, agg_v in filter_v.items():
            if not idx_keys:
                break
            if agg_k == '$in':
                if not isinstance(agg_v, (list, tuple, set)):
                    raise MongitaError("'$in' requires an iterable")
                idx_keys = [k for k in idx_keys if k in agg_v]
            elif agg_k == '$nin':
                if not isinstance(agg_v, (list, tuple, set)):
                    raise MongitaError("'$nin' requires an iterable")
                idx_keys = [k for k in idx_keys if k not in agg_v]
            elif agg_k == '$eq':
                idx_keys = [k for k in idx_keys if k == agg_v]
            elif agg_k == '$ne':
                idx_keys = [k for k in idx_keys if k != agg_v]
            elif agg_k == '$lt':
                idx_keys = [k for k in idx_keys if k < agg_v]
            elif agg_k == '$lte':
                idx_keys = [k for k in idx_keys if k <= agg_v]
            elif agg_k == '$gt':
                idx_keys = [k for k in idx_keys if k > agg_v]
            elif agg_k == '$gte':
                idx_keys = [k for k in idx_keys if k >= agg_v]
            else:
                raise MongitaError("Unexpected match exception %r" % agg_k)
        ret = set()
        for k in idx_keys:
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
    new_idx = collections.defaultdict(list, idx_doc['idx'])
    example_type = key_type = None
    if new_idx:
        example_type = next(new_idx.keys().__iter__())
        key_type = type(example_type)

    if not example_type:
        try:
            doc = next(documents)
        except StopIteration:
            return
        key = _get_item_from_doc(doc, key_str)
        new_idx[key].append(str(doc['_id']))
        example_type = key
        key_type = type(example_type)

    for doc in documents:
        key = _get_item_from_doc(doc, key_str)
        if not isinstance(key, key_type):
            # TODO Nonetype keys
            raise MongitaError("All index keys must be the same type. "
                               f"For index '{key_str}', the type is '{key_type}'. "
                               f"However, when building, a key arrived of type {type(key)}")
        new_idx[key].append(str(doc['_id']))
    idx_doc['example_type'] = example_type
    reverse = idx_doc['direction'] == DESCENDING
    idx_doc['idx'] = dict(sorted(new_idx.items(), reverse=reverse))


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
    UNIMPLEMENTED = ['aggregate', 'aggregate_raw_batches', 'bulk_write', 'codec_options', 'create_indexes', 'drop', 'drop_indexes', 'ensure_index', 'estimated_document_count', 'find_one_and_delete', 'find_one_and_replace', 'find_one_and_update', 'find_raw_batches', 'index_information', 'inline_map_reduce', 'list_indexes', 'map_reduce', 'next', 'options', 'read_concern', 'read_preference', 'rename', 'watch', 'with_options', 'write_concern']
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
        if not self._engine.doc_exists(self._metadata_location):
            self._engine.create_path(self._base_location)
            metadata = StorageObject({
                'options': {},
                'indexes': {},
                '_id': str(bson.ObjectId()),
            })
            if not self._engine.upload_metadata(self._metadata_location, metadata):
                raise OperationFailure("expected match")
        self.database._create(self.name)
        self._existence_verified = True

    def _insert_one(self, document):
        document_location = self._get_location(document['_id'])
        success = self._engine.upload_doc(document_location, document,
                                          if_gen_match=True)
        if not success:
            raise DuplicateKeyError("Document %r already exists" % document['_id'])

    @support_alert
    def insert_one(self, document):
        _validate_doc(document)
        document = StorageObject(document)
        document['_id'] = document.get('_id') or bson.ObjectId()
        self._create()
        metadata = self._checkout_metadata([document['_id']], 'add')
        self._insert_one(document)
        self._update_indicies([document], metadata)
        return InsertOneResult(document['_id'])

    @support_alert
    def insert_many(self, documents, ordered=True):
        """
        :param list documents:
        :param bool ordered:

        Insert documents. If ordered, stop inserting if there is an error
        """
        if not isinstance(documents, list):
            raise MongitaError("Documents must be a list")
        ready_docs = []
        for doc in documents:
            try:
                _validate_doc(doc)
            except MongitaError:
                if ordered:
                    break
                continue
            doc = StorageObject(doc)
            doc['_id'] = doc.get('_id') or bson.ObjectId()
            ready_docs.append(doc)
        self._create()
        success_docs = []
        metadata = self._checkout_metadata([d['_id'] for d in ready_docs], 'add')
        start = time.time()
        touch_idx = 1
        for doc in ready_docs:
            try:
                self._insert_one(doc)
                success_docs.append(doc)
            except MongitaError:
                if ordered:
                    self._update_indicies(success_docs, metadata)
                    return InsertManyResult(success_docs)
                continue
            if time.time() - start > touch_idx:
                self._engine.touch_metadata(self._metadata_location)
                touch_idx += 1
        self._update_indicies(success_docs, metadata)
        return InsertManyResult(success_docs)

    @support_alert
    def replace_one(self, filter, replacement, upsert=False):
        filter = filter or {}
        _validate_filter(filter)
        _validate_doc(replacement)
        replacement = StorageObject(replacement)

        doc_id = self._find_one_id(filter)
        if not doc_id:
            if upsert:
                replacement['_id'] = replacement.get('_id') or bson.ObjectId()
                self._insert_one(replacement)
                return UpdateResult(0, 1, replacement['_id'])
            return UpdateResult(0, 0)
        replacement['_id'] = doc_id
        metadata = self._checkout_metadata([doc_id], 'add')
        success = self._engine.upload_doc(self._get_location(doc_id), replacement)
        self._update_indicies([replacement], metadata)
        if success:
            return UpdateResult(1, 1)
        return UpdateResult(1, 0)

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
            # TODO do we need to check if these exist? Hypothetically no but things may get corrupted.
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
        for attempt in range(1, 6):
            doc = self._engine.download_doc(loc)
            _apply_update(doc, update)
            success = self._engine.upload_doc(loc, doc, if_gen_match=True)
            if success:
                break
            time.sleep(attempt ** 4 / 1000)
        else:
            raise OperationFailure("Could not update document after 5 tries")
        return dict(doc)

    @support_alert
    def update_one(self, filter, update, upsert=False):
        _validate_filter(filter)
        _validate_update(update)
        if upsert:
            raise MongitaNotImplementedError("Mongita does not support 'upsert' on update operations. Use `replace_one`.")

        doc_ids = list(self._find_ids(filter))
        matched_count = len(doc_ids)
        if not matched_count:
            return UpdateResult(matched_count, 0)
        metadata = self._checkout_metadata(doc_ids[:1], 'add')
        doc = self._update_doc(doc_ids[0], update)
        self._update_indicies([doc], metadata)
        return UpdateResult(matched_count, 1)

    @support_alert
    def update_many(self, filter, update, upsert=False):
        _validate_filter(filter)
        _validate_update(update)
        if upsert:
            raise MongitaNotImplementedError("Mongita does not support 'upsert' on update operations. Use `replace_one`.")

        success_docs = []
        matched_cnt = 0
        doc_ids = list(self._find_ids(filter))
        metadata = self._checkout_metadata(doc_ids, 'add')
        start = time.time()
        touch_idx = 1
        for doc_id in doc_ids:
            doc = self._update_doc(doc_id, update)
            success_docs.append(doc)
            matched_cnt += 1
            if time.time() - start > touch_idx:
                self._engine.touch_metadata(self._metadata_location)
                touch_idx += 1
        self._update_indicies(success_docs, metadata)
        return UpdateResult(matched_cnt, len(success_docs))

    @support_alert
    def delete_one(self, filter):
        _validate_filter(filter)

        doc_id = self._find_one_id(filter)
        if not doc_id:
            return DeleteResult(0)
        loc = self._get_location(doc_id)

        metadata = self._checkout_metadata([doc_id], 'delete')
        if self._engine.delete_doc(loc):
            self._update_indicies_deletes([doc_id], metadata)
            return DeleteResult(1)
        self._update_indicies_deletes([], metadata)
        return DeleteResult(0)

    @support_alert
    def delete_many(self, filter):
        _validate_filter(filter)

        doc_ids = list(self._find_ids(filter))
        metadata = self._checkout_metadata(doc_ids, 'delete')
        success_deletes = []
        start = time.time()
        touch_idx = 1
        for doc_id in doc_ids:
            if self._engine.delete_doc(self._get_location(doc_id)):
                success_deletes.append(doc_id)
            if time.time() - start > touch_idx:
                self._engine.touch_metadata(self._metadata_location)
                touch_idx += 1
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
        Get metadata. If we encounter a corrupted state, fix it
        """
        for attempt in range(1, 8):
            metadata_tup = self._engine.download_metadata(self._metadata_location)
            if not metadata_tup:
                return {}
            metadata, staleness = metadata_tup
            if 'updating_set' not in metadata:
                return metadata
            if staleness < 2:
                time.sleep(attempt ** 4 / 1000)
                continue
            self._engine.touch_metadata(self._metadata_location)
            if metadata['updating_set']['op'] in ('add', 'mod_idx'):
                ids_in = metadata['updating_set']['doc_ids']
                docs = list(self._find({'_id': {'$in': ids_in}}, metadata=metadata))
                try:
                    ret = self._update_indicies(docs, metadata)
                    return ret
                except OperationFailure:
                    pass
            else:
                try:
                    ret = self._update_indicies_deletes(metadata['updating_set']['doc_ids'], metadata)
                    return ret
                except OperationFailure:
                    pass
            time.sleep(attempt ** 4 / 1000)
        raise OperationFailure("Couldn't get metadata. Timed out")

    def _checkout_metadata(self, doc_ids, op):
        assert op in ('add', 'mod_idx', 'delete')
        for attempt in range(1, 6):
            metadata = self._get_metadata()
            metadata['updating_set'] = {'doc_ids': list(map(str, doc_ids)), 'op': op}
            success = self._engine.upload_metadata(self._metadata_location, metadata)
            if success:
                return metadata
        raise OperationFailure("Could not checkout metadata after 5 tries")

    def _update_indicies_deletes(self, doc_ids, metadata):
        if doc_ids:
            for idx_doc in metadata.get('indexes', {}).values():
                _remove_docs_from_idx_doc(doc_ids, idx_doc)
        del metadata['updating_set']
        success = self._engine.upload_metadata(self._metadata_location, metadata)
        if success:
            return metadata
        raise OperationFailure("Could not update indicies")

    def _update_indicies(self, documents, metadata):
        for idx_doc in metadata.get('indexes', {}).values():
            _update_idx_doc_with_new_documents(documents, idx_doc)
        del metadata['updating_set']
        success = self._engine.upload_metadata(self._metadata_location, metadata)
        if success:
            return metadata
        raise OperationFailure("Could not update indicies")

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
            'example_type': None
        }

        metadata = self._checkout_metadata([], 'mod_idx')
        _update_idx_doc_with_new_documents(self._find({}, metadata=metadata), idx_doc)
        metadata['indexes'][idx_name] = idx_doc
        del metadata['updating_set']
        success = self._engine.upload_metadata(self._metadata_location, metadata)
        if not success:
            raise OperationFailure("Could not create index")
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

        metadata = self._checkout_metadata([], 'mod_idx')
        try:
            del metadata['indexes'][index_or_name]
        except KeyError:
            pass
        del metadata['updating_set']
        success = self._engine.upload_metadata(self._metadata_location, metadata)
        if not success:
            raise OperationFailure("Could not drop index")
