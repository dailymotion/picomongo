from copy import copy, deepcopy

import pymongo
from pymongo import ReadPreference
from pymongo.errors import InvalidOperation, OperationFailure
from pymongo.cursor import Cursor as PymongoCursor

from exceptions import ValidationError
from utils import CMProxy, CollectionDescriptor

class DocumentCursor(PymongoCursor):

    def __init__(self, cursor, document_class, **kwargs):
        self.__dict__ = cursor.__dict__
        self._document = document_class
        self._kwargs = kwargs

    def __getitem__(self, index):
        return self._document(PymongoCursor.__getitem__(self, index),
                              **self._kwargs)

    def __getattr__(self, attr_name):
        return PymongoCursor.__getattribute__(self, attr_name)

    def next(self):
        return self._document(PymongoCursor.next(self), **self._kwargs)

class Document(dict):
    '''Base class for all documents.
    '''
    con = CMProxy('con')
    db = CMProxy('db')
    col = CollectionDescriptor()

    required_fields = []
    default_values = {}

    config_name = None
    collection_name = None

    def __init__(self, initial_values=None, use_defaults=True):
        init = {}
        if use_defaults:
            for key, value in self.default_values.items():
                init[key] = value() if hasattr(value, '__call__') else \
                    deepcopy(value)
        if initial_values is None:
            initial_values = {}

        if any(key.startswith('_') and key != "_id" for key in initial_values):
            raise ValueError("Initial values should not contains private fields")

        init.update(initial_values)

        super(Document, self).__init__(init)

    def save(self, validate=False, reload=False, **kwargs):
        '''Save document in db. Does not save attribute starting with '_'.
        '''
        if validate:
            local_copy = copy(self)
            self.__class__.validate(local_copy)
            if local_copy != self:
                err_msg = 'Changes and deletion are forbiden in validate method'
                raise ValidationError(err_msg)

        # TODO: Should picomongo manage db error
        self.col.save(self, **kwargs)

        if reload:
            self.reload()

    @classmethod
    def find_one(cls, *args, **kwargs):
        '''Get a single document from the database and return it as a Document.

        Any additionnal arguments will be passed to Collection.find_one
        '''
        the_one = cls.col.find_one(*args, **kwargs)
        if the_one:
            return cls(the_one, use_defaults=False)
        return the_one

    @classmethod
    def find(cls, *args, **kwargs):
        '''Query the database and returns results as Documents.

        Any additionnal arguments will be passed to Collection.find
        '''
        return DocumentCursor(cls.col.find(*args, **kwargs), cls,
                              use_defaults=False)

    @classmethod
    def generate_index(cls):
        '''Generate index in DB using Document.indexes

        Generate index use attribute named 'indexes' and accept theses format:
        * {'fields': ('something',)}: A single key index on the key 'something'
        * {'fields': ('-something',)}: A single descending key index on the key
            'something'
        * {'fields': ('something', '-something_else')}: A compound index on
            'something' ascending and 'something_else' descending

        Any additionnal items in 'indexes' will be passed as keyword arguments
            to ensure_index. For example:

        {'fields': ('something',), 'unique': True, 'ttl': 3600 * 24}
        '''

        ascending = lambda field: (field, pymongo.ASCENDING)
        # field[1:] remove '-' from field name
        descending = lambda field: (field[1:], pymongo.DESCENDING)

        results = []
        for index in cls.indexes:
            index = index.copy()

            fields = index.pop('fields')

            if len(fields) > 1:
                fields = [descending(field) if field.startswith('-')
                          else ascending(field) for field in fields]
            elif fields[0].startswith('-'):
                fields = [descending(fields[0])]
            else:
                fields = fields[0]

            results.append(cls.col.ensure_index(fields, **index))
        return results

    def reload(self):
        '''Reload current document from DB.

        Any additionnal arguments will be passed to Document.find_one
        Raise an InvalidOperation if current document is unsaved.
        '''
        if not self.get('_id'):
            raise InvalidOperation('You cannot reload an unsaved document.')

        doc = self.col.find_one({'_id': self._id}, read_preference=ReadPreference.PRIMARY)

        if not doc:
            raise OperationFailure('Document is no more present in DB.')

        self.clear()
        self.update(doc)

    def delete(self, *args, **kwargs):
        '''Remove current Document from database.

        Any additionnal arguments will be passed to Collection.remove
        Raise an InvalidOperation if current document is unsaved.
        '''
        if not '_id' in self:
            raise InvalidOperation('You cannot remove an unsaved document.')
        return self.col.remove({'_id': self._id}, *args, **kwargs)

    def validate(self):
        '''Override this method to add document validation.

        It takes no arguments.
        If it raises an exception, validation will be considered as failed.
        Otherwise validation will be considered as a success.
        '''
        msg = 'Validate method is not implemented for this document model.'
        raise NotImplementedError(msg)

    def __str__(self):
        return '%s(%s)' % (self.__class__.__name__, dict(self))

    def __repr__(self):
        return self.__str__()

    __getattr__ = dict.__getitem__

    def __setattr__(self, attr_name, value):
        white_list = set(('_id',)) # Set
        # Hack for properties setter
        if hasattr(getattr(self.__class__, attr_name, None), '__set__'):
            return object.__setattr__(self, attr_name, value)
        else:
            if attr_name.startswith('_') and attr_name not in white_list:
                return object.__setattr__(self, attr_name, value)
            else:
                return self.__setitem__(attr_name, value)

    def __delattr__(self, attr_name):
        if attr_name.startswith('_'):
            return object.__delattr__(self, attr_name)
        else:
            return self.__delitem__(attr_name)
