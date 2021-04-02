import bson
import sortedcontainers

from .cursor import Cursor
from .common import support_alert, Location, ASCENDING, DESCENDING, StorageObject, MetaStorageObject
from .errors import MongitaError, MongitaNotImplementedError, DuplicateKeyError
from .results import InsertOneResult, InsertManyResult, DeleteResult, UpdateResult


_SUPPORTED_FILTER_OPERATORS = ('$in', '$eq', '$gt', '$gte', '$lt', '$lte', '$ne', '$nin')
_SUPPORTED_UPDATE_OPERATORS = ('$set', '$inc')


def _validate_filter(filter):
    """
    Validate the 'filter' parameter.
    This is near the top of most public methods.

    :param filter dict:
    :rtype: None
    """
    if not isinstance(filter, dict):
        raise MongitaError("The filter parameter must be a dict, not %r" % type(filter))
    for k in filter.keys():
        if not isinstance(k, str):
            raise MongitaError("Filter keys must be strings, not %r" % type(filter))
    _id = filter.get('_id')
    if _id:
        if not isinstance(_id, (bson.ObjectId, str, dict)):
            raise MongitaError("If present, the '_id' filter must be a bson ObjectId, string, or a dict")
    for query_ops in filter.values():
        if isinstance(query_ops, dict):
            for op in query_ops.keys():
                if op not in _SUPPORTED_FILTER_OPERATORS:
                    raise MongitaError(
                        "Mongita does not support %r. These filter operators are "
                        "supported: %r" % (k, _SUPPORTED_FILTER_OPERATORS))


def _validate_update(update):
    """
    Validate the 'update' parameter.
    This is near the top of the public update methods.

    :param update dict:
    :rtype: None
    """
    if not isinstance(update, dict):
        raise MongitaError("The update parameter must be a dict, not %r" % type(update))
    for k in update.keys():
        if k in _SUPPORTED_UPDATE_OPERATORS:
            continue
        if k.startswith('$'):
            raise MongitaNotImplementedError(
                "Mongita does not support %r. These update operators are " \
                "supported: %r." % (k, _SUPPORTED_UPDATE_OPERATORS))
        raise MongitaError(
            "In update operations, you must use one of the supported " \
            "update operators %r." % (_SUPPORTED_UPDATE_OPERATORS,))
    for update_dict in update.values():
        if not isinstance(update_dict, dict):
            raise MongitaError("If present, the update operator must be a dict, not %r" % type(update_dict))
        _id = update_dict.get('_id')
        if _id:
            if not isinstance(_id, (str, bson.ObjectId)):
                raise MongitaError("The update _id must be a bson ObjectId or a string")


def _validate_doc(doc):
    """
    Validate the 'doc' parameter.
    This is near the top of the public insert / replace methods.

    :param doc dict:
    :rtype: None
    """

    if not isinstance(doc, dict):
        raise MongitaError("The document must be a dict, not %r" % type(doc))
    _id = doc.get('_id')
    if _id:
        if not isinstance(_id, (bson.ObjectId, str)):
            raise MongitaError("The document _id must be a bson ObjectId, a string, or not present")


def _doc_matches_agg(doc_v, query_ops):
    """
    Return whether an individual document value matches a dict of
    query operations. Usually there will be one query_op but sometimes there
    are many.

    e.g. collection.find({'path.to.doc_v': {'$query_op': query_val}})

    :param doc_v: The value in the doc to compare against
    :param query_ops {$query_op: query_val}:
    :returns: Whether the document value matches all query operators
    :rtype: bool
    """

    for query_op, query_val in query_ops.items():
        if query_op == '$in':
            if not isinstance(query_val, (list, tuple, set)):
                raise MongitaError("'$in' requires an iterable")
            if doc_v not in query_val:
                return False
        elif query_op == '$nin':
            if not isinstance(query_val, (list, tuple, set)):
                raise MongitaError("'$nin' requires an iterable")
            if doc_v in query_val:
                return False
        elif query_op == '$eq':
            if doc_v != query_val:
                return False
        elif query_op == '$ne':
            if doc_v == query_val:
                return False
        elif query_op == '$lt':
            if doc_v >= query_val:
                return False
        elif query_op == '$lte':
            if doc_v > query_val:
                return False
        elif query_op == '$gt':
            if doc_v <= query_val:
                return False
        elif query_op == '$gte':
            if doc_v < query_val:
                return False
        # agg_k check is in _validate_filter
    return True


def _doc_matches_slow_filters(doc, slow_filters):
    """
    Given an entire doc, return whether that doc matches every filter item in the
    slow_filters dict. A slow_filter is just the set of filters that we didn't
    have an index for.

    :param doc dict:
    :param slow_filters dict:
    :rtype: bool
    """
    for doc_key, query_ops in slow_filters.items():
        if isinstance(query_ops, dict):
            doc_v = _get_item_from_doc(doc, doc_key)
            if _doc_matches_agg(doc_v, query_ops):
                continue
            return False

        if _get_item_from_doc(doc, doc_key) == query_ops:
            continue
        return False
    return True


def _ids_given_irange_filters(matched_ids, idx, **kwargs):
    """
    Given an existing set of matched_ids, a SortedDict (idx), and
    a set of kwargs to apply to SortedDict.irange,
    return all keys that match both the irange and the existing matched_ids

    :param matched_ids set:
    :param idx sortedcontainers.SortedDict:
    :param kwargs dict: irange filters
    :rtype set:
    """
    if matched_ids is not None:
        return matched_ids.intersection(set(idx.irange(**kwargs)))
    return set(idx.irange(**kwargs))


def _idx_filter_sort(query_op_tup):
    """
    For performance, the order of filtering matters. It's best to do
    equality first before comparsion. Not-equal should be last becase a lot
    of values are liable to be returned.
    In and nin vary in how much they matter so go with not-equal

    :param query_op_tup (query_op, query_val):
    :rtype: bool
    """
    query_op, _ = query_op_tup
    return (query_op == '$eq',
            query_op in ('$lt', '$lte', '$gt', '$gte'))


def _get_ids_from_idx(idx, query_ops):
    """
    Returns the ids that match a set of query_ops in an index.

    :param idx SortedDict:
    :param query_ops str|dict:
    :rtype: set
    """
    matched_ids = None
    if isinstance(query_ops, dict):
        for query_op, query_val in sorted(query_ops.items(),
                                          key=_idx_filter_sort, reverse=True):
            if query_op == '$eq':
                matched_ids = set((query_val,)) if query_val in (matched_ids or idx.keys()) else set()
            elif query_op == '$ne':
                matched_ids = set(k for k in matched_ids or idx.keys() if k != query_val)
            elif query_op == '$lt':
                matched_ids = _ids_given_irange_filters(matched_ids, idx,
                                                        maximum=query_val,
                                                        inclusive=(False, False))
            elif query_op == '$lte':
                matched_ids = _ids_given_irange_filters(matched_ids, idx,
                                                        maximum=query_val,
                                                        inclusive=(True, True))
            elif query_op == '$gt':
                matched_ids = _ids_given_irange_filters(matched_ids, idx,
                                                        minimum=query_val,
                                                        inclusive=(False, False))
            elif query_op == '$gte':
                matched_ids = _ids_given_irange_filters(matched_ids, idx,
                                                        minimum=query_val,
                                                        inclusive=(True, True))
            elif query_op == '$in':
                if not isinstance(query_val, (list, tuple, set)):
                    raise MongitaError("'$in' requires an iterable")
                matched_ids = set(k for k in matched_ids or idx.keys() if k in query_val)
            elif query_op == '$nin':
                if not isinstance(query_val, (list, tuple, set)):
                    raise MongitaError("'$nin' requires an iterable")
                matched_ids = set(k for k in matched_ids or idx.keys() if k not in query_val)
            # validation of options is done earlier
            if not matched_ids:
                return set()
        ret = set()
        for k in matched_ids:
            ret.update(idx[k])
        return ret
    return set(idx.get(query_ops, set()))


def _set_item_in_doc(update_op, update_op_dict, doc):
    """
    Given an $update_op, a {doc_key: value} update_op_dict, and a doc,
    Update the doc in-place at doc_key with the update operation.

    e.g.
    doc = {'hi': 'ma'}
    update_op = '$set'
    update_op_dict {'ma': 'pa'}
    -> {'hi': 'pa'}

    :param update_op str:
    :param update_op_dict {str: value}:
    :param doc dict:
    :rtype: None
    """

    for doc_key, value in update_op_dict.items():
        ds, last_key = _get_datastructure_from_doc(doc, doc_key)
        if ds is None:
            raise MongitaError("Cannot apply operation %r to %r" %
                               ({update_op: update_op_dict}, doc))
        if update_op == '$set':
            ds[last_key] = value
        elif update_op == '$inc':
            ds[last_key] += value
        # Should never get an update key we don't recognize b/c _validate_update


def _rightpad(item, desired_length):
    """
    Given a list, pad to the desired_length with Nones
    This might be slow but it modifies the list in place

    :param item list:
    :param desired_length int:
    :rtype: None
    """
    pad_len = desired_length - len(item)
    for _ in range(pad_len):
        item.append(None)


def _get_datastructure_from_doc(doc, key):
    """
    Get a pass-by-reference data structure from the document so that we can
    update it in-place. This dives deep into the document with the key
    parameter which uses dot notation.

    e.g.
    doc = {'deep': {'nested': {'list': [1, 2, 3]}}}
    key = 'deep.nested.list.5'
    -> a reference to [1, 2, 3, None, None] and 5

    :param doc dict:
    :param key str:
    :returns: the datastructure and the final accessor
    :rtype: list|dict|None, value
    """

    if '.' not in key:
        return doc, key

    item = doc
    levels = key.split('.')
    levels, last_level = levels[:-1], levels[-1]
    for level in levels:
        if isinstance(item, list):
            try:
                level_int = int(level)
            except ValueError:
                return None, None
            try:
                item = item[level_int]
            except IndexError:
                if level_int > 0:
                    _rightpad(item, level_int)
                    item = item[level_int]
                else:
                    return None, None
        elif isinstance(item, dict):
            if level not in item:
                item[level] = {}
            item = item[level]
        else:
            return None, None
    return item, last_level


def _get_item_from_doc(doc, key):
    """
    Get an item from the document given a key which might use dot notation.

    e.g.
    doc = {'deep': {'nested': {'list': ['a', 'b', 'c']}}}
    key = 'deep.nested.list.1'
    -> 'b'

    :param doc dict:
    :param key str:
    :rtype: value
    """
    if '.' in key:
        item = doc
        for level in key.split('.'):
            if isinstance(item, list):
                try:
                    level_int = int(level)
                except ValueError:
                    return None
                try:
                    item = item[level_int]
                except IndexError:
                    return None
            elif isinstance(item, dict):
                item = item.get(level, {})
            else:
                return None
        return item or None
    return doc.get(key)


def _update_idx_doc_with_new_documents(documents, idx_doc):
    """
    Update an idx_doc given documents which were just inserted / modified / etc

    :param documents list[dict]:
    :param idx_doc {key_str: str, direction: int idx: SortedDict, ...}:
    :rtype: None
    """

    key_str = idx_doc['key_str']
    new_idx = sortedcontainers.SortedDict(idx_doc['idx'])

    for doc in documents:
        key = _get_item_from_doc(doc, key_str)
        new_idx.setdefault(key, []).append(doc['_id'])

    reverse = idx_doc['direction'] == DESCENDING
    idx_doc['idx'] = sortedcontainers.SortedDict(sorted(new_idx.items(), reverse=reverse))


def _remove_docs_from_idx_doc(doc_ids, idx_doc):
    """
    Update an idx_doc given documents which were just removed

    :param doc_ids list[str]:
    :param idx_doc {key_str: str, direction: int idx: SortedDict, ...}:
    :rtype: None
    """

    for k in idx_doc['idx'].keys():
        idx_doc['idx'][k] = [d for d in idx_doc['idx'][k] if d not in doc_ids]
    return idx_doc


def _sort_docs(docs, sort_list):
    """
    Given the sort list provided in the .sort() method,
    sort the documents in place.

    from https://docs.python.org/3/howto/sorting.html

    :param docs list[dict]:
    :param sort_list list[(key, direction)]
    :rtype: None
    """
    for k, direction in reversed(sort_list):
        if direction == ASCENDING:
            docs.sort(key=lambda d: _get_item_from_doc(d, k))
        elif direction == DESCENDING:
            docs.sort(key=lambda d: _get_item_from_doc(d, k), reverse=True)
        # validation on direction happens in cursor


def _split_filter(filter, metadata):
    """
    Split the filter into indx_ops and slow_filters which are later used
    differently

    :param filter {doc_key: query_ops}:
    :param metadata dict:
    :rtype: {doc_key: query_ops}, [(SortedDict idx, dict query_ops), ...]
    """
    slow_filters = {}
    indx_ops = []
    indexes = metadata.get('indexes', {})
    for doc_key, query_ops in filter.items():
        if doc_key + '_1' in indexes:
            indx_ops.append((indexes[doc_key + '_1']['idx'], query_ops))
        elif doc_key + '_-1' in indexes:
            indx_ops.append((indexes[doc_key + '_-1']['idx'], query_ops))
        else:
            slow_filters[doc_key] = query_ops
    return slow_filters, indx_ops


def _apply_indx_ops(indx_ops):
    """
    Return all doc_ids that can be found through the index filters

    :param indx_ops {idx_key: query_ops}:
    :param indexes dict:
    :rtype: set
    """
    doc_ids_from_all_indx_ops = []
    for idx, query_ops in indx_ops:
        doc_ids = _get_ids_from_idx(idx, query_ops)
        if not doc_ids:
            return set()
        doc_ids_from_all_indx_ops.append(doc_ids)
    return set.intersection(*doc_ids_from_all_indx_ops)


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
        """
        First check for deprecated / unimplemented.
        Then, MongoDB has this weird thing where there can be dots in a collection
        name.
        """
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
        """Given an object_id, return the Location object"""
        return Location(self.database.name, self.name, object_id)

    def _create(self):
        """
        MongoDB doesn't require you to explicitly create collections. They
        are created when first accessed. This creates the collection and is
        called early in modifier methods.
        """
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
        """
        Insert a single document.

        :param document dict:
        :rtype: None
        """
        document_location = self._get_location(document['_id'])
        success = self._engine.upload_doc(document_location, document,
                                          if_gen_match=True)
        if not success:
            assert self._engine.doc_exists(document_location)
            raise DuplicateKeyError("Document %r already exists" % document['_id'])

    @support_alert
    def insert_one(self, document):
        """
        Insert a single document.

        :param document dict:
        :rtype: results.InsertOneResult
        """
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
        Insert documents. If ordered, stop inserting if there is an error.
        If not ordered, all operations are attempted

        :param list documents:
        :param bool ordered:
        :rtype: results.InsertManyResult
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
        """
        Replace one document. If no document was found with the filter,
        and upsert is True, insert the replacement.

        :param filter dict:
        :param replacement dict:
        :param bool upsert:
        :rtype: results.UpdateResult
        """
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

    def _find_one_id(self, filter, sort=None):
        """
        Given the filter, return a single object_id or None.

        :param filter dict:
        :param sort list[(key, direction)]|None
        :rtype: str|None
        """

        if not filter and not sort:
            return self._engine.find_one_id(self._base_location)

        if '_id' in filter:
            location = self._get_location(filter['_id'])
            if self._engine.doc_exists(location):
                return location._id
            return None

        try:
            return next(self._find_ids(filter, sort))
        except StopIteration:
            return None

    def _find_one(self, filter, sort):
        """
        Given the filter, return a single doc or None.

        :param filter dict:
        :param sort list[(key, direction)]|None
        :rtype: dict|None
        """
        doc_id = self._find_one_id(filter, sort)
        if doc_id:
            doc = self._engine.download_doc(self._get_location(doc_id))
            if doc:
                return dict(doc)

    def _find_ids(self, filter, sort=None, limit=None, metadata=None):
        """
        Given a filter, find all doc_ids that match this filter.
        Be sure to also sort and limit them.
        This method will download documents for non-indexed filters (slow_filters).
        Downloaded docs are cached in the engine layer so performance cost is minimal.
        This method returns a generator

        :param filter dict:
        :param sort list[(key, direction)]|None:
        :param limit int|None:
        :param metadata dict|None:
        :rtype: Generator(list[str])
        """
        filter = filter or {}
        sort = sort or []

        if limit == 0:
            return

        metadata = metadata or self._get_metadata()
        slow_filters, indx_ops = _split_filter(filter, metadata)

        # If we have index ops, we can use those ids as a starting point.
        # otherwise, we need to get all_ids and filter one-by-one
        if indx_ops:
            doc_ids = _apply_indx_ops(indx_ops)
        else:
            doc_ids = self._engine.list_ids(self._base_location)
        if not doc_ids:
            return

        if sort:
            docs_to_return = []
            for doc_id in doc_ids:
                doc = self._engine.download_doc(self._get_location(doc_id))
                if _doc_matches_slow_filters(doc, slow_filters):
                    docs_to_return.append(doc)
            _sort_docs(docs_to_return, sort)
            if limit is None:
                for doc in docs_to_return:
                    yield doc['_id']
            else:
                i = 0
                for doc in docs_to_return:
                    yield doc['_id']
                    i += 1
                    if i == limit:
                        return
            return

        if limit is None:
            for doc_id in doc_ids:
                doc = self._engine.download_doc(self._get_location(doc_id))
                if _doc_matches_slow_filters(doc, slow_filters):
                    yield doc['_id']
            return

        i = 0
        for doc_id in doc_ids:
            doc = self._engine.download_doc(self._get_location(doc_id))
            if _doc_matches_slow_filters(doc, slow_filters):
                yield doc['_id']
                i += 1
                if i == limit:
                    return

    def _find(self, filter, sort=None, limit=None, metadata=None):
        """
        Given a filter, find all docs that match this filter.
        This method returns a generator.

        :param filter dict:
        :param sort list[(key, direction)]|None:
        :param limit int|None:
        :param metadata dict|None:
        :rtype: Generator(list[dict])
        """

        for doc_id in self._find_ids(filter, sort, limit, metadata=metadata):
            doc = self._engine.download_doc(self._get_location(doc_id))
            yield dict(doc)

    @support_alert
    def find_one(self, filter=None, sort=None):
        """
        Return the first matching document.

        :param filter dict:
        :param sort list[(key, direction)]|None:
        :rtype: dict|None
        """

        # TODO test sort on find_one
        filter = filter or {}
        _validate_filter(filter)
        return self._find_one(filter, sort)

    @support_alert
    def find(self, filter=None, sort=None, limit=None):
        """
        Return a cursor of all matching documents.

        :param filter dict:
        :param sort list[(key, direction)]|None:
        :param limit int|None:
        :rtype: cursor.Cursor
        """

        filter = filter or {}
        _validate_filter(filter)
        return Cursor(self._find, filter, sort, limit)  # TODO test inline finds

    def _update_doc(self, doc_id, update):
        """
        Given a doc_id and an update dict, find the document and safely update it.
        Returns the updated document

        :param doc_id str:
        :param update dict:
        :rtype: dict
        """
        loc = self._get_location(doc_id)
        doc = self._engine.download_doc(loc)
        for update_op, update_op_dict in update.items():
            _set_item_in_doc(update_op, update_op_dict, doc)
        assert self._engine.upload_doc(loc, doc, if_gen_match=True)
        return dict(doc)

    @support_alert
    def update_one(self, filter, update, upsert=False):
        """
        Find one document matching the filter and update it.
        The 'upsert' parameter is not supported.

        :param filter dict:
        :param update dict:
        :param upsert bool:
        :rtype: results.UpdateResult
        """

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
        """
        Update every document matched by the filter.
        The 'upsert' parameter is not supported.

        :param filter dict:
        :param update dict:
        :param upsert bool:
        :rtype: results.UpdateResult
        """
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
        """
        Delete one document matching the filter.

        :param filter dict:
        :rtype: results.DeleteResult
        """
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
        """
        Delete all documents matching the filter.

        :param filter dict:
        :rtype: results.DeleteResult
        """
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
        """
        Returns a count of all documents matching the filter.
        This can be much faster than taking the length of a find query.

        :param filter dict:
        :rtype: int
        """
        _validate_filter(filter)
        return len(list(self._find_ids(filter)))

    @support_alert
    def distinct(self, key, filter=None):
        """
        Given a key, return all distinct documents matching the key

        :param key str:
        :param filter dict|None:
        :rtype: list[str]
        """

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
        Thin wrapper to get metadata.
        Always be sure to lock the engine when modifying metadata

        :rtype: dict
        """
        return self._engine.download_metadata(self._metadata_location) or {}

    def _update_indicies_deletes(self, doc_ids, metadata):
        """
        Given a list of deleted document ids, remove those documents from all indexes.
        Returns the new metadata dictionary.

        :param doc_ids list[str]:
        :param metadata dict:
        :rtype: dict
        """
        if not doc_ids:
            return metadata
        for idx_doc in metadata.get('indexes', {}).values():
            _remove_docs_from_idx_doc(doc_ids, idx_doc)
        assert self._engine.upload_metadata(self._metadata_location, metadata)
        return metadata

    def _update_indicies(self, documents, metadata):
        """
        Given a list of deleted document ids, add those documents to all indexes.
        Returns the new metadata dictionary.

        :param documents list[dict]:
        :param metadata dict:
        :rtype: dict
        """
        for idx_doc in metadata.get('indexes', {}).values():
            _update_idx_doc_with_new_documents(documents, idx_doc)
        assert self._engine.upload_metadata(self._metadata_location, metadata)
        return metadata

    @support_alert
    def create_index(self, keys):
        """
        Create a new index for the collection.
        Indexes can dramatically speed up queries that use its fields.
        Currently, only single key indicies are supported.
        Returns the name of the new index.

        :param keys str|[(key, direction)]:
        :rtype: str
        """

        if isinstance(keys, str):
            keys = [(keys, ASCENDING)]
        if not isinstance(keys, list) or keys == []:
            raise MongitaError("Unsupported keys parameter format %r. See the docs." % str(keys))
        if len(keys) > 1:
            raise MongitaNotImplementedError("Mongita does not support multi-key indexes yet")
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
        """
        Drops the index given by the index_or_name parameter. Passing index
        objects is not supported

        :param index_or_name str|(str, int)
        :rtype: None
        """
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
        """
        Returns a list of indexes in the collection

        :rtype: {idx_id: {'key': [(key_str, direction_int)]}}
        """

        ret = [{'_id_': {'key': [('_id', 1)]}}]
        metadata = self._get_metadata()
        for idx in metadata.get('indexes', {}).values():
            ret.append({idx['_id']: {'key': [(idx['key_str'], idx['direction'])]}})
        return ret
