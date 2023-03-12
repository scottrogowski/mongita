import collections
import copy
import datetime
import functools
import re

import bson
import sortedcontainers

from .cursor import Cursor, _validate_sort
from .common import support_alert, ASCENDING, DESCENDING, MetaStorageObject
from .errors import (MongitaError, MongitaNotImplementedError, DuplicateKeyError,
                     InvalidName, OperationFailure)
from .read_concern import ReadConcern
from .results import InsertOneResult, InsertManyResult, DeleteResult, UpdateResult
from .write_concern import WriteConcern


_SUPPORTED_FILTER_OPERATORS = ('$in', '$eq', '$gt', '$gte', '$lt', '$lte', '$ne', '$nin')
_SUPPORTED_UPDATE_OPERATORS = ('$set', '$inc', '$push')
_DEFAULT_METADATA = {
    'options': {},
    'indexes': {},
    '_id': str(bson.ObjectId()),
}


# FROM docs.mongodb.com/manual/reference/bson-type-comparison-order/#comparison-sort-order
SORT_ORDER = {
    int: b'\x02',
    float: b'\x02',
    str: b'\x03',
    object: b'\x04',
    list: b'\x05',
    bytes: b'\x06',
    bson.ObjectId: b'\x07',
    bool: b'\x08',
    datetime.datetime: b'\t',
    re.Pattern: b'\n',
}


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
                if op.startswith('$') and op not in _SUPPORTED_FILTER_OPERATORS:
                    raise MongitaError(
                        "Mongita does not support %r. These filter operators are "
                        "supported: %r" % (op, _SUPPORTED_FILTER_OPERATORS))


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
            raise MongitaError("If present, the update operator must be a dict, "
                               "not %r" % type(update_dict))
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
    for k in doc.keys():
        if not k or k.startswith('$'):
            raise InvalidName("All document keys must be truthy and cannot start with '$'.")


def _overlap(iter_a, iter_b):
    """
    Return if there is any overlap between iter_a and iter_b
    from https://stackoverflow.com/questions/3170055

    :param iter_a list:
    :param iter_b list:
    :rtype: bool
    """
    return not set(iter_a).isdisjoint(iter_b)


def _doc_matches_agg(doc_v, query_ops):
    """
    Return whether an individual document value matches a dict of
    query operations. Usually there will be one query_op but sometimes there
    are many.

    e.g. collection.find({'path.to.doc_v': {'$query_op': query_val}})

    The loop returns False whenever we know for sure that the document is
    not part of the query. At the end return True

    :param doc_v: The value in the doc to compare against
    :param query_ops {$query_op: query_val}:
    :returns: Whether the document value matches all query operators
    :rtype: bool
    """
    if any(k.startswith('$') for k in query_ops.keys()):
        for query_op, query_val in query_ops.items():
            if query_op == '$eq':
                if doc_v != query_val:
                    return False
            elif query_op == '$ne':
                if doc_v == query_val:
                    return False
            elif query_op == '$in':
                if not isinstance(query_val, (list, tuple, set)):
                    raise MongitaError("'$in' requires an iterable")
                if not ((isinstance(doc_v, list) and _overlap(doc_v, query_val))
                        or (doc_v in query_val)):
                    return False
            elif query_op == '$nin':
                if not isinstance(query_val, (list, tuple, set)):
                    raise MongitaError("'$nin' requires an iterable")
                if (isinstance(doc_v, list) and _overlap(doc_v, query_val)) \
                   or (doc_v in query_val):
                    return False
            elif query_op == '$lt':
                try:
                    if doc_v >= query_val:
                        return False
                except TypeError:
                    return False
            elif query_op == '$lte':
                try:
                    if doc_v > query_val:
                        return False
                except TypeError:
                    return False
            elif query_op == '$gt':
                try:
                    if doc_v <= query_val:
                        return False
                except TypeError:
                    return False
            elif query_op == '$gte':
                try:
                    if doc_v < query_val:
                        return False
                except TypeError:
                    return False
            # agg_k check is in _validate_filter
        return True
    else:
        return doc_v == query_ops


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

        item_from_doc = _get_item_from_doc(doc, doc_key)
        if isinstance(item_from_doc, list) and query_ops in item_from_doc:
            continue
        if item_from_doc == query_ops:
            continue
        return False
    return True


def _ids_given_irange_filters(matched_keys, idx, **kwargs):
    """
    Given an existing set of matched_keys, a SortedDict (idx), and
    a set of kwargs to apply to SortedDict.irange,
    return all keys that match both the irange and the existing matched_keys

    :param matched_keys set:
    :param idx sortedcontainers.SortedDict:
    :param kwargs dict: irange filters
    :rtype set:
    """
    clean_idx_key = kwargs.get('minimum') or kwargs.get('maximum')
    # if 'minimum' in kwargs:
    #     kwargs['maximum'] = (bytes([ord(kwargs['minimum'][0]) + 1]), None)
    # if 'maximum' in kwargs:
    #     kwargs['minimum'] = (bytes([ord(kwargs['maximum'][0]) - 1]), None)
    ret = set(idx.irange(**kwargs))
    ret = set(key for key in ret if key[0] == clean_idx_key[0])

    return set.intersection(matched_keys, ret)


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
    if not isinstance(query_ops, dict):
        return set(idx.get(_make_idx_key(query_ops), set()))

    if not set(query_ops.keys()).intersection(_SUPPORTED_FILTER_OPERATORS):
        if _make_idx_key(query_ops) in idx.keys():
            return idx[_make_idx_key(query_ops)]
        return set()

    keys_remain = set(idx.keys())
    keys_not_cursed = keys_remain.copy()
    keys_cursed = set()

    for query_op, query_val in sorted(query_ops.items(),
                                      key=_idx_filter_sort, reverse=True):
        clean_idx_key = _make_idx_key(query_val)
        if query_op == '$eq':
            keys_remain = {clean_idx_key} if clean_idx_key in keys_remain else set()

        elif query_op == '$ne':
            _keys_cursed = set(k for k in keys_not_cursed if k == clean_idx_key)
            keys_remain -= _keys_cursed
            keys_not_cursed -= _keys_cursed
            keys_cursed.update(_keys_cursed)
        elif query_op == '$lt':
            keys_remain = _ids_given_irange_filters(keys_remain, idx,
                                                    maximum=clean_idx_key,
                                                    inclusive=(False, False))
        elif query_op == '$lte':
            keys_remain = _ids_given_irange_filters(keys_remain, idx,
                                                    maximum=clean_idx_key,
                                                    inclusive=(False, True))
        elif query_op == '$gt':
            keys_remain = _ids_given_irange_filters(keys_remain, idx,
                                                    minimum=clean_idx_key,
                                                    inclusive=(False, False))
        elif query_op == '$gte':
            keys_remain = _ids_given_irange_filters(keys_remain, idx,
                                                    minimum=clean_idx_key,
                                                    inclusive=(True, False))
        elif query_op == '$in':
            if not isinstance(query_val, (list, tuple, set)):
                raise MongitaError("'$in' requires an iterable")
            clean_q_val = [_make_idx_key(e) for e in query_val]
            keys_remain = set(k for k in keys_remain
                              if k in clean_q_val)
        elif query_op == '$nin':
            if not isinstance(query_val, (list, tuple, set)):
                raise MongitaError("'$nin' requires an iterable")
            clean_q_val = [_make_idx_key(e) for e in query_val]
            _keys_cursed = set(k for k in keys_not_cursed
                               if k in clean_q_val)
            keys_remain -= _keys_cursed
            keys_not_cursed -= _keys_cursed
            keys_cursed.update(_keys_cursed)
        # validation of options is done earlier
    ids_cursed = set()
    for k in keys_cursed:
        ids_cursed.update(idx[k])

    ret = set()
    for k in keys_remain:
        ret.update(idx[k])
    ret -= ids_cursed
    return ret


def _failed_update_error(update_op, update_op_dict, doc, msg):
    """Helper for raising errors on update"""
    return MongitaError("Cannot apply operation %r to %r (%s)" %
                        ({update_op: update_op_dict}, doc, msg))


def _update_item_in_doc(update_op, update_op_dict, doc):
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
        if isinstance(ds, list):
            _rightpad(ds, last_key)
        if ds is None:
            raise _failed_update_error(update_op, update_op_dict, doc,
                                       "Could not find item")
        if update_op == '$set':
            ds[last_key] = value
        elif update_op == '$inc':
            if not isinstance(value, (int, float)):
                raise _failed_update_error(update_op, update_op_dict, doc,
                                           "Increment was not numeric")
            elif not isinstance(ds.get(last_key), (int, float)):
                raise _failed_update_error(update_op, update_op_dict, doc,
                                           "Document value was not numeric")
            ds[last_key] += value
        elif update_op == '$push':
            if isinstance(ds.get(last_key), list):
                ds[last_key].append(value)
            elif last_key not in ds:
                ds[last_key] = [value]
            else:
                raise _failed_update_error(update_op, update_op_dict, doc,
                                           "Document value was not a list")
        # Should never get an update key we don't recognize b/c _validate_update


def _rightpad(item, desired_length):
    """
    Given a list, pad to the desired_length with Nones
    This might be slow but it modifies the list in place

    :param item list:
    :param desired_length int:
    :rtype: None
    """
    pad_len = desired_length - len(item) + 1
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
            if level_int < 0:
                return None, None
            try:
                item = item[level_int]
            except IndexError:
                _rightpad(item, level_int)
                item = item[level_int] or {}
        elif isinstance(item, dict):
            if level not in item or not isinstance(item[level], (list, dict)):
                item[level] = {}
            item = item[level]
        else:
            return None, None
    if isinstance(item, list):
        try:
            last_level = int(last_level)
        except ValueError:
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


def _make_idx_key(idx_key):
    """
    MongoDB is very liberal when it comes to what keys it can compare on.
    When we get something weird, it makes sense to just store it as a
    hashable key

    :param idx_key value:
    :rtype: hashable value
    """
    if isinstance(idx_key, collections.abc.Hashable):
        return _sort_tup(idx_key)
    try:
        return _sort_tup(str(bson.encode(idx_key)))
    except TypeError:
        return _sort_tup(str(bson.encode({'idx_key': idx_key})))


def _update_idx_doc_with_new_documents(documents, idx_doc):
    """
    Update an idx_doc given documents which were just inserted / modified / etc

    :param documents list[dict]:
    :param idx_doc {key_str: str, direction: int idx: SortedDict, ...}:
    :rtype: None
    """
    documents = list(documents)
    _remove_docs_from_idx_doc(set(d['_id'] for d in documents), idx_doc)

    key_str = idx_doc['key_str']
    new_idx = sortedcontainers.SortedDict(idx_doc['idx'])

    for doc in documents:
        item_from_doc = _get_item_from_doc(doc, key_str)
        if isinstance(item_from_doc, list):
            for item in item_from_doc:
                key = _make_idx_key(item)
                new_idx.setdefault(key, set()).add(doc['_id'])
        key = _make_idx_key(item_from_doc)
        new_idx.setdefault(key, set()).add(doc['_id'])

    reverse = idx_doc['direction'] == DESCENDING
    idx_doc['idx'] = sortedcontainers.SortedDict(sorted(new_idx.items(), reverse=reverse))


def _remove_docs_from_idx_doc(doc_ids, idx_doc):
    """
    Update an idx_doc given documents which were just removed

    :param doc_ids set[str]:
    :param idx_doc {key_str: str, direction: int idx: SortedDict, ...}:
    :rtype: None
    """

    idx_doc_idx = idx_doc['idx']
    for k in idx_doc_idx.keys():
        idx_doc_idx[k] -= doc_ids


def _sort_tup(item):
    """
    Get sort tuple of item type according to mongodb rules

    :param item Value:
    :rtype: (int, Value)
    """
    try:
        return (SORT_ORDER[type(item)], item)
    except KeyError:
        pass
    # this assumes the item is None but could catch other
    # types if we are not careful. Sorting bugs are minor though
    return (b'\x01', item)


def _sort_func(doc, sort_key):
    """
    Sorter to sort different types according to MongoDB rules

    :param doc dict:
    :param sort_key str:
    :rtype: tuple
    """
    item = _get_item_from_doc(doc, sort_key)
    return _sort_tup(item)


def _sort_docs(docs, sort_list):
    """
    Given the sort list provided in the .sort() method,
    sort the documents in place.

    from https://docs.python.org/3/howto/sorting.html

    :param docs list[dict]:
    :param sort_list list[(key, direction)]
    :rtype: None
    """
    for sort_key, direction in reversed(sort_list):
        _sort_func_partial = functools.partial(_sort_func, sort_key=sort_key)
        if direction == ASCENDING:
            docs.sort(key=_sort_func_partial)
        elif direction == DESCENDING:
            docs.sort(key=_sort_func_partial, reverse=True)
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
    doc_ids_so_far = set()
    for idx, query_ops in indx_ops:
        doc_ids = _get_ids_from_idx(idx, query_ops)
        if not doc_ids:
            return set()
        if doc_ids_so_far:
            doc_ids_so_far = doc_ids_so_far.intersection(doc_ids)
            if not doc_ids_so_far:
                return set()
        else:
            doc_ids_so_far = doc_ids
    return doc_ids_so_far


class Collection():
    UNIMPLEMENTED = ['aggregate', 'aggregate_raw_batches', 'bulk_write', 'codec_options',
                     'create_indexes', 'drop', 'drop_indexes', 'ensure_index',
                     'estimated_document_count', 'find_one_and_delete',
                     'find_one_and_replace', 'find_one_and_update', 'find_raw_batches',
                     'inline_map_reduce', 'list_indexes', 'map_reduce', 'next',
                     'options', 'read_concern', 'read_preference', 'rename', 'watch', ]
    DEPRECATED = ['reindex', 'parallel_scan', 'initialize_unordered_bulk_op',
                  'initialize_ordered_bulk_op', 'group', 'count', 'insert', 'save',
                  'update', 'remove', 'find_and_modify', 'ensure_index']

    def __init__(self, collection_name, database, write_concern=None, read_concern=None):
        self.name = collection_name
        self.database = database
        self._write_concern = write_concern or WriteConcern()
        self._read_concern = read_concern or ReadConcern()
        self._engine = database._engine
        self._existence_verified = False
        self._base_location = f'{database.name}.{collection_name}'

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
        return self._base_location

    @property
    def write_concern(self):
        return self._write_concern

    @property
    def read_concern(self):
        return self._read_concern

    def with_options(self, **kwargs):
        write_concern = kwargs.pop('write_concern', None)
        read_concern = kwargs.pop('read_concern', None)

        if kwargs:
            raise MongitaNotImplementedError("The method 'with_options' doesn't yet "
                                             "accept %r" % kwargs)
        return Collection(self.name, self.database,
                          write_concern=write_concern,
                          read_concern=read_concern)

    def __create(self):
        """
        MongoDB doesn't require you to explicitly create collections. They
        are created when first accessed. This creates the collection and is
        called early in modifier methods.
        """
        if self._existence_verified:
            return
        with self._engine.lock:
            if not self._engine.get_metadata(self._base_location):
                self._engine.create_path(self._base_location)
                metadata = MetaStorageObject(copy.deepcopy(_DEFAULT_METADATA))
                assert self._engine.put_metadata(self._base_location, metadata)
            self.database.__create(self.name)
        self._existence_verified = True

    def __insert_one(self, document):
        """
        Insert a single document.

        :param document dict:
        :rtype: None
        """
        success = self._engine.put_doc(self.full_name, document,
                                       no_overwrite=True)
        if not success:
            assert self._engine.doc_exists(self.full_name, document['_id'])
            raise DuplicateKeyError("Document %r already exists" % document['_id'])

    @support_alert
    def insert_one(self, document):
        """
        Insert a single document.

        :param document dict:
        :rtype: results.InsertOneResult
        """
        _validate_doc(document)
        document = copy.deepcopy(document)
        document['_id'] = document.get('_id') or bson.ObjectId()
        self.__create()
        with self._engine.lock:
            metadata = self.__get_metadata()
            self.__insert_one(document)
            self.__update_indicies([document], metadata)
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
            doc = copy.deepcopy(doc)
            doc['_id'] = doc.get('_id') or bson.ObjectId()
            ready_docs.append(doc)
        self.__create()
        success_docs = []
        exception = None
        with self._engine.lock:
            metadata = self.__get_metadata()
            for doc in ready_docs:
                try:
                    self.__insert_one(doc)
                    success_docs.append(doc)
                except Exception as ex:
                    if ordered:
                        self.__update_indicies(success_docs, metadata)
                        raise MongitaError("Ending insert_many because of error") from ex
                    exception = ex
                    continue
            self.__update_indicies(success_docs, metadata)
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
        self.__create()

        replacement = copy.deepcopy(replacement)

        with self._engine.lock:
            doc_id = self.__find_one_id(filter, upsert=upsert)
            if not doc_id:
                if upsert:
                    metadata = self.__get_metadata()
                    replacement['_id'] = replacement.get('_id') or bson.ObjectId()
                    self.__insert_one(replacement)
                    self.__update_indicies([replacement], metadata)
                    return UpdateResult(0, 1, replacement['_id'])
                return UpdateResult(0, 0)
            replacement['_id'] = doc_id
            metadata = self.__get_metadata()
            assert self._engine.put_doc(self.full_name, replacement)
            self.__update_indicies([replacement], metadata)
            return UpdateResult(1, 1)

    def __find_one_id(self, filter, sort=None, skip=None, upsert=False):
        """
        Given the filter, return a single object_id or None.

        :param filter dict:
        :param sort list[(key, direction)]|None
        :param skip int|None
        :rtype: str|None
        """

        if not filter and not sort:
            return self._engine.find_one_id(self._base_location)

        if '_id' in filter:
            if upsert or self._engine.doc_exists(self.full_name, filter['_id']):
                return filter['_id']
            return None

        try:
            return next(self.__find_ids(filter, sort, skip=skip))
        except StopIteration:
            return None

    def __find_one(self, filter, sort, skip):
        """
        Given the filter, return a single doc or None.

        :param filter dict:
        :param sort list[(key, direction)]|None
        :param skip int|None
        :rtype: dict|None
        """
        doc_id = self.__find_one_id(filter, sort, skip)
        if doc_id:
            doc = self._engine.get_doc(self.full_name, doc_id)
            if doc:
                return copy.deepcopy(doc)

    def __find_ids(self, filter, sort=None, limit=None, skip=None, metadata=None):
        """
        Given a filter, find all doc_ids that match this filter.
        Be sure to also sort and limit them.
        This method will download documents for non-indexed filters (slow_filters).
        Downloaded docs are cached in the engine layer so performance cost is minimal.
        This method returns a generator

        :param filter dict:
        :param sort list[(key, direction)]|None:
        :param limit int|None:
        :param skip int|None:
        :param metadata dict|None:
        :rtype: Generator(list[str])
        """
        filter = filter or {}
        sort = sort or []

        if limit == 0:
            return

        metadata = metadata or self.__get_metadata()
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
                doc = self._engine.get_doc(self.full_name, doc_id)
                if _doc_matches_slow_filters(doc, slow_filters):
                    docs_to_return.append(doc)
            _sort_docs(docs_to_return, sort)

            if skip:
                docs_to_return = docs_to_return[skip:]

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

        if skip:
            doc_ids = doc_ids[skip:]

        if limit is None:
            for doc_id in doc_ids:
                doc = self._engine.get_doc(self.full_name, doc_id)
                if doc and _doc_matches_slow_filters(doc, slow_filters):
                    yield doc['_id']
            return

        i = 0
        for doc_id in doc_ids:
            doc = self._engine.get_doc(self.full_name, doc_id)
            if _doc_matches_slow_filters(doc, slow_filters):
                yield doc['_id']
                i += 1
                if i == limit:
                    return

    def __find(self, filter, sort=None, limit=None, skip=None, metadata=None, shallow=False):
        """
        Given a filter, find all docs that match this filter.
        This method returns a generator.

        :param filter dict:
        :param sort list[(key, direction)]|None:
        :param limit int|None:
        :param skip int|None:
        :param metadata dict|None:
        :rtype: Generator(list[dict])
        """
        gen = self.__find_ids(filter, sort, limit, skip, metadata=metadata)

        if shallow:
            for doc_id in gen:
                doc = self._engine.get_doc(self.full_name, doc_id)
                yield doc
        else:
            for doc_id in gen:
                doc = self._engine.get_doc(self.full_name, doc_id)
                yield copy.deepcopy(doc)

    @support_alert
    def find_one(self, filter=None, sort=None, skip=None):
        """
        Return the first matching document.

        :param filter dict:
        :param sort list[(key, direction)]|None:
        :param skip int|None:
        :rtype: dict|None
        """

        filter = filter or {}
        _validate_filter(filter)

        if sort is not None:
            sort = _validate_sort(sort)
        return self.__find_one(filter, sort, skip)

    @support_alert
    def find(self, filter=None, sort=None, limit=None, skip=None):
        """
        Return a cursor of all matching documents.

        :param filter dict:
        :param sort list[(key, direction)]|None:
        :param limit int|None:
        :param skip int|None:
        :rtype: cursor.Cursor
        """

        filter = filter or {}
        _validate_filter(filter)

        if sort is not None:
            sort = _validate_sort(sort)

        if limit is not None and not isinstance(limit, int):
            raise TypeError('Limit must be an integer')

        if skip is not None:
            if not isinstance(skip, int):
                raise TypeError('Skip must be an integer')
            if skip < 0:
                raise ValueError('Skip must be >=0')

        return Cursor(self.__find, filter, sort, limit, skip)

    def __update_doc(self, doc_id, update):
        """
        Given a doc_id and an update dict, find the document and safely update it.
        Returns the updated document

        :param doc_id str:
        :param update dict:
        :rtype: dict
        """
        doc = self._engine.get_doc(self.full_name, doc_id)
        for update_op, update_op_dict in update.items():
            _update_item_in_doc(update_op, update_op_dict, doc)
        assert self._engine.put_doc(self.full_name, doc)
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
        self.__create()
        if upsert:
            raise MongitaNotImplementedError("Mongita does not support 'upsert' on "
                                             "update operations. Use `replace_one`.")

        with self._engine.lock:
            doc_ids = list(self.__find_ids(filter))
            matched_count = len(doc_ids)
            if not matched_count:
                return UpdateResult(matched_count, 0)
            metadata = self.__get_metadata()
            doc = self.__update_doc(doc_ids[0], update)
            self.__update_indicies([doc], metadata)
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
        self.__create()
        if upsert:
            raise MongitaNotImplementedError("Mongita does not support 'upsert' "
                                             "on update operations. Use `replace_one`.")

        success_docs = []
        matched_cnt = 0
        with self._engine.lock:
            doc_ids = list(self.__find_ids(filter))
            metadata = self.__get_metadata()
            for doc_id in doc_ids:
                doc = self.__update_doc(doc_id, update)
                success_docs.append(doc)
                matched_cnt += 1
            self.__update_indicies(success_docs, metadata)
        return UpdateResult(matched_cnt, len(success_docs))

    @support_alert
    def delete_one(self, filter):
        """
        Delete one document matching the filter.

        :param filter dict:
        :rtype: results.DeleteResult
        """
        _validate_filter(filter)
        self.__create()

        with self._engine.lock:
            doc_id = self.__find_one_id(filter)
            if not doc_id:
                return DeleteResult(0)
            metadata = self.__get_metadata()
            self._engine.delete_doc(self.full_name, doc_id)
            self.__update_indicies_deletes({doc_id}, metadata)
        return DeleteResult(1)

    @support_alert
    def delete_many(self, filter):
        """
        Delete all documents matching the filter.

        :param filter dict:
        :rtype: results.DeleteResult
        """
        _validate_filter(filter)
        self.__create()

        success_deletes = set()
        with self._engine.lock:
            doc_ids = list(self.__find_ids(filter))
            metadata = self.__get_metadata()
            for doc_id in doc_ids:
                if self._engine.delete_doc(self.full_name, doc_id):
                    success_deletes.add(doc_id)
            self.__update_indicies_deletes(success_deletes, metadata)
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
        return len(list(self.__find_ids(filter)))

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

    def __get_metadata(self):
        """
        Thin wrapper to get metadata.
        Always be sure to lock the engine when modifying metadata

        :rtype: dict
        """
        return self._engine.get_metadata(self._base_location) or copy.deepcopy(_DEFAULT_METADATA)

    def __update_indicies_deletes(self, doc_ids, metadata):
        """
        Given a list of deleted document ids, remove those documents from all indexes.
        Returns the new metadata dictionary.

        :param doc_ids set[str]:
        :param metadata dict:
        :rtype: dict
        """
        if not doc_ids:
            return metadata
        for idx_doc in metadata.get('indexes', {}).values():
            _remove_docs_from_idx_doc(doc_ids, idx_doc)
        assert self._engine.put_metadata(self._base_location, metadata)
        return metadata

    def __update_indicies(self, documents, metadata):
        """
        Given a list of deleted document ids, add those documents to all indexes.
        Returns the new metadata dictionary.

        :param documents list[dict]:
        :param metadata dict:
        :rtype: dict
        """
        for idx_doc in metadata.get('indexes', {}).values():
            _update_idx_doc_with_new_documents(documents, idx_doc)
        assert self._engine.put_metadata(self._base_location, metadata)
        return metadata

    @support_alert
    def create_index(self, keys, background=False):
        """
        Create a new index for the collection.
        Indexes can dramatically speed up queries that use its fields.
        Currently, only single key indicies are supported.
        Returns the name of the new index.

        :param keys str|[(key, direction)]:
        :param background bool:
        :rtype: str
        """
        if background is not False:
            raise MongitaNotImplementedError("background index creation is not supported")

        self.__create()

        if isinstance(keys, str):
            keys = [(keys, ASCENDING)]
        if not isinstance(keys, list) or keys == []:
            raise MongitaError("Unsupported keys parameter format %r. "
                               "See the docs." % str(keys))
        if len(keys) > 1:
            raise MongitaNotImplementedError("Mongita does not support multi-key indexes yet")
        for k, direction in keys:
            if not k or not isinstance(k, str):
                raise MongitaError("Index keys must be strings %r" % str(k))
            if direction not in (ASCENDING, DESCENDING):
                raise MongitaError("Index key direction must be either ASCENDING (1) "
                                   "or DESCENDING (-1). Not %r" % direction)

        key_str, direction = keys[0]

        idx_name = f'{key_str}_{direction}'
        new_idx_doc = {
            '_id': idx_name,
            'key_str': key_str,
            'direction': direction,
            'idx': {},
        }

        with self._engine.lock:
            metadata = self.__get_metadata()
            _update_idx_doc_with_new_documents(self.__find({}, metadata=metadata, shallow=True),
                                               new_idx_doc)
            metadata['indexes'][idx_name] = new_idx_doc
            assert self._engine.put_metadata(self._base_location, metadata)
        return idx_name

    @support_alert
    def drop_index(self, index_or_name):
        """
        Drops the index given by the index_or_name parameter. Passing index
        objects is not supported

        :param index_or_name str|(str, int)
        :rtype: None
        """
        self.__create()

        if isinstance(index_or_name, (list, tuple)) \
           and len(index_or_name) == 1 \
           and len(index_or_name[0]) == 2 \
           and isinstance(index_or_name[0][0], str) \
           and isinstance(index_or_name[0][1], int):
            index_or_name = f'{index_or_name[0][0]}_{index_or_name[0][1]}'
        if not isinstance(index_or_name, str):
            raise MongitaError("Unsupported index_or_name parameter format. See the docs.")
        if re.match(r'^.*?_\-?1$', index_or_name):
            key, direction = index_or_name.rsplit('_', 1)
            direction = int(direction)
        else:
            index_or_name = index_or_name + '_1'

        with self._engine.lock:
            metadata = self.__get_metadata()
            if index_or_name not in metadata['indexes']:
                raise OperationFailure('Index not found with name %r' % index_or_name)
            del metadata['indexes'][index_or_name]
            assert self._engine.put_metadata(self._base_location, metadata)

    @support_alert
    def index_information(self):
        """
        Returns a list of indexes in the collection

        :rtype: {idx_id: {'key': [(key_str, direction_int)]}}
        """

        ret = [{'_id_': {'key': [('_id', 1)]}}]
        metadata = self.__get_metadata()
        for idx in metadata.get('indexes', {}).values():
            ret.append({idx['_id']: {'key': [(idx['key_str'], idx['direction'])]}})
        return ret
