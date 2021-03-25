import collections

import bson

from .cursor import Cursor
from .common import support_alert, Location, ASCENDING, DESCENDING
from .errors import MongitaError, MongitaNotImplementedError
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
        if isinstance(_id, bson.ObjectId):
            _id = str(_id)
            filter['_id'] = _id
        if not isinstance(_id, (str, dict)):
            raise MongitaError("If present, the filter _id must be a string or a dict")
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
            if isinstance(_id, bson.ObjectId):
                _id = str(_id)
                update_dict['_id'] = _id
            if not isinstance(_id, str):
                raise MongitaError("The update _id must be a string")


def _validate_doc(doc):
    if not isinstance(doc, dict):
        raise MongitaError("The document must be a dict, not %r" % type(doc))
    _id = doc.get('_id')
    if _id:
        if isinstance(_id, bson.ObjectId):
            _id = str(_id)
            doc['_id'] = _id
        if not isinstance(_id, str):
            raise MongitaError("The document _id must be a string or not present")


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


def _doc_ids_match_filter_in_idx(idx, filter_v):
    """
    :param idx
    """
    # sortedcontainers.SortedDict
    # functools.reduce(operator.iconcat, (weights[el] for el in weights.irange(None,8)))

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
            ret.add(idx[k])
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
    item = doc
    for level in key.split('.'):
        item = item.get(level, {})
    return item or None


def _update_idx_doc_from_new_documents(documents, inx_doc):
    key_str = inx_doc['key_str']
    new_idx = collections.defaultdict(list, inx_doc['idx'])
    for doc in documents:
        new_idx[_get_item_from_doc(doc, key_str)].append(doc['_id'])
    reverse = inx_doc['direction'] == DESCENDING
    inx_doc['idx'] = dict(sorted(new_idx.items(), reverse=reverse))
    return inx_doc


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
            metadata = {
                'options': {},
                'indexes': {},
                '_id': str(bson.ObjectId()),
            }
            self._engine.upload_doc(self._metadata_location, metadata)
        self.database._create(self.name)
        self._existence_verified = True

    def _insert_one(self, document):
        document = document.copy()
        obj_id = str(document.get('_id', '') or bson.ObjectId())
        document_location = self._get_location(obj_id)
        if self._engine.doc_exists(document_location):
            raise MongitaError("Document %r already exists" % obj_id)
        document['_id'] = obj_id
        self._engine.upload_doc(document_location, document)
        return document

    @support_alert
    def insert_one(self, document):
        _validate_doc(document)
        self._create()
        document = self._insert_one(document)
        self._update_indicies([document])
        return InsertOneResult(document['_id'])

    @support_alert
    def insert_many(self, documents, ordered=True):
        if not isinstance(documents, list):
            raise MongitaError("Documents must be a list")
        self._create()
        new_docs = []
        for doc in documents:
            try:
                _validate_doc(doc)
                new_docs.append(self._insert_one(doc))
            except MongitaError:
                if ordered:
                    self._update_indicies(new_docs)
                    return InsertManyResult(new_docs)
                continue
        self._update_indicies(new_docs)
        return InsertManyResult(new_docs)

    @support_alert
    def replace_one(self, filter, replacement, upsert=False):
        filter = filter or {}
        _validate_filter(filter)
        _validate_doc(replacement)
        replacement = replacement.copy()

        doc_id = self._find_one_id(filter)
        if not doc_id:
            if upsert:
                new_doc = self._insert_one(replacement)
                return UpdateResult(0, 1, new_doc['_id'])
            return UpdateResult(0, 0)
        replacement['_id'] = doc_id

        self._engine.upload_doc(self._get_location(doc_id), replacement)
        self._update_indicies([replacement])
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

    def _find_ids(self, filter, sort=None, limit=None):
        """
        :param filter dict:
        :param sort bool:

        Given a filter, find all the document ids that match this filter.
        This will, in some cases, download documents. These are cached in the
        storage layer so performance hits are minimal.
        """
        print('a')
        filter = filter or {}
        sort = sort or []

        if limit == 0:
            return

        print('b')
        reg_filters = {}
        idx_filters = {}
        indexes = self._get_metadata().get('indexes', {})
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

        print('c')

        if sort:
            print('c1')
            ret = []
            for doc_id in doc_ids:
                doc = self._engine.download_doc(self._get_location(doc_id))
                if _doc_matches_filter(doc, reg_filters):
                    ret.append(doc)
            _sorter(ret, sort)
            print('c2')
            if limit is None:
                print('c3')
                for doc in ret:
                    yield doc['_id']
            else:
                print('c4', ret)
                i = 0
                for doc in ret:
                    yield doc['_id']
                    i += 1
                    if i == limit:
                        return
            return
        print('d')

        if limit is None:
            for doc_id in doc_ids:
                doc = self._engine.download_doc(self._get_location(doc_id))
                if _doc_matches_filter(doc, reg_filters):
                    yield doc['_id']
            return

        print('e')
        i = 0
        for doc_id in doc_ids:
            doc = self._engine.download_doc(self._get_location(doc_id))
            if _doc_matches_filter(doc, reg_filters):
                yield doc['_id']
                i += 1
                if i == limit:
                    return

    def _find(self, filter, sort=None, limit=None):
        for doc_id in self._find_ids(filter, sort, limit):
            doc = self._engine.download_doc(self._get_location(doc_id))
            yield dict(doc)
        print('done with _find')

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
        self._engine.upload_doc(loc, doc, generation=doc.generation)
        return dict(doc)
        # TODO generation fail handling

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
        doc = self._update_doc(doc_ids[0], update)
        self._update_indicies([doc])
        return UpdateResult(matched_count, 1)

    @support_alert
    def update_many(self, filter, update, upsert=False):
        _validate_filter(filter)
        _validate_update(update)
        if upsert:
            raise MongitaNotImplementedError("Mongita does not support 'upsert' on update operations. Use `replace_one`.")

        success_docs = []
        matched_cnt = 0
        for doc_id in self._find_ids(filter):
            doc = self._update_doc(doc_id, update)
            success_docs.append(doc)
            matched_cnt += 1
        return UpdateResult(matched_cnt, len(success_docs))

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
        return self._engine.download_doc(self._metadata_location) or {}

    def _update_indicies(self, documents):
        metadata = self._get_metadata()
        for idx_doc in metadata.get('indexes', {}).values():
            _update_idx_doc_from_new_documents(documents, idx_doc)
        self._engine.upload_doc(self._metadata_location, metadata)

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
        inx_doc = {
            '_id': idx_name,
            'key_str': key_str,
            'direction': direction,
            'idx': {},
        }
        idx_doc = _update_idx_doc_from_new_documents(list(self._find({})), inx_doc)

        # TODO collection-level write lock
        metadata = self._get_metadata()
        metadata['indexes'][idx_name] = idx_doc
        self._engine.upload_doc(self._metadata_location, metadata)
        return idx_name

    @support_alert
    def drop_index(self, index_or_name):
        if isinstance(index_or_name, (list, tuple)) \
           and len(index_or_name) == 2 \
           and isinstance(index_or_name[0], str) \
           and isinstance(index_or_name[1], int):
            index_or_name = f'{index_or_name[0]}_{index_or_name[1]}'
        if not isinstance(index_or_name, str):
            raise MongitaError("Unsupported index_or_name parameter format. See the docs.")
        try:
            key, direction = index_or_name.split('_')
            direction = int(direction)
        except ValueError:
            raise MongitaError("Unsupported index_or_name parameter format. See the docs.")

        metadata = self._get_metadata()
        try:
            del metadata['indexes'][index_or_name]
        except KeyError:
            pass
        self._engine.upload_doc(self._metadata_location, metadata)


    @support_alert
    def delete_one(self, filter):
        _validate_filter(filter)

        doc_id = self._find_one_id(filter)
        if not doc_id:
            return DeleteResult(0)

        self._engine.delete_doc(self._get_location(doc_id))
        return DeleteResult(1)
        # TODO delete error handling

    @support_alert
    def delete_many(self, filter):
        _validate_filter(filter)

        doc_ids_gen = self._find_ids(filter)

        deleted_count = 0
        for doc_id in doc_ids_gen:
            if self._engine.delete_doc(self._get_location(doc_id)):
                deleted_count += 1
        return DeleteResult(deleted_count)
