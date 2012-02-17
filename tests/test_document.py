import unittest

import pymongo
import pymongo.objectid
from mock import patch, Mock, sentinel

from pymongo import Connection
from pymongo.objectid import ObjectId
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.errors import InvalidOperation, DuplicateKeyError, OperationFailure

from picomongo import Document, ConnectionManager
from picomongo.exceptions import ValidationError
from utils import Call

#Examples document class
class UserDocument(Document):
    def full_name(self):
        return '%s %s' % (self.first_name, self.name)

class SimpleIndex(Document):
    indexes = []

class DefaultDocument(Document):
    default_values = {'a': 1, 'b': 2}

class ValidationDocument(Document):
    pass

class DocumentBaseTestCase(unittest.TestCase):

    def test_init_with_values(self):
        data = {'attr1': 1, 'attr2': 2}
        document = Document(data)

        self.assertEquals(document, data)

    def test_init_with_private_names(self):
        data = {'_private': 42}
        self.assertRaises(ValueError, Document, data)

    def test_access_config(self):
        ConnectionManager.configure()

        self.assertTrue(isinstance(UserDocument.col, Collection))
        self.assertTrue(isinstance(UserDocument.db, Database))
        self.assertTrue(isinstance(UserDocument.con, Connection))

    def test_reconfiguration(self):
        ConnectionManager.configure()

        self.assertEqual(UserDocument.db.name, 'test')

        ConnectionManager.configure({'userdocument': {'db': 'test2'}})
        self.assertEqual(UserDocument.db.name, 'test2')

class DocumentPersistenceBaseTestCase(unittest.TestCase):

    def setUp(self):
        self.document = Document()

        self.connection = pymongo.Connection()
        self.db = self.connection.test
        self.collection = self.db.test_collection

    def tearDown(self):
        self.collection.remove()

    def test_insert(self):
        """Test that a document can be directly insert into mongodb.
        """
        self.document.attribute = 'value'

        inserted_id = self.collection.insert(self.document)

        self.assertTrue(isinstance(self.document._id, pymongo.objectid.ObjectId))

        self.assertEqual(inserted_id, self.document._id)
        self.assertEqual(self.collection.find_one(), {'attribute': 'value', '_id': self.document._id})

    def test_document_rehydration(self):
        """Test that mongodb document can be rehydrated into Document classes.
        """
        user_data = {'first_name': 'Boris', 'name': 'FELD'}

        self.collection.insert(user_data)

        user = self.collection.find_one(as_class=UserDocument)

        self.assertEqual(user.full_name(), 'Boris FELD')

class DocumentPersistenceTestCase(unittest.TestCase):

    def setUp(self):
        self.data = {'first_name': 'Boris', 'name': 'FELD'}
        self.document = UserDocument()
        self.document.first_name = 'Boris'
        self.document.name = 'FELD'

    def tearDown(self):
        self.document.col.remove()

    def test_save(self):
        self.document.save()

        self.assertEqual(UserDocument.col.find_one(), self.document)

    def test_rehydrate_from_class(self):
        self.document.save()

        document = UserDocument.find_one()

        self.assertEqual(document.full_name(), 'Boris FELD')

class DocumentDBMethodsTestCase(unittest.TestCase):

    def tearDown(self):
        UserDocument.col.remove()

    def test_find_one(self):
        args = ['arg1', 'arg2', 'arg3']
        kwargs = {'kwarg1': '', 'kwarg2': '', 'kwarg3': ''}
        with patch.object(UserDocument, 'col') as mock_col:
            mock_find_one = mock_col.find_one
            mock_find_one.return_value = {}
            UserDocument.find_one(*args, **kwargs)

        self.assertEqual(mock_find_one.call_args_list, [Call(*args, **kwargs)])

    def test_fine_one_no_document(self):
        self.assertEqual(UserDocument.find_one(), None)

    def test_find(self):
        args = ['arg1', 'arg2', 'arg3']
        kwargs = {'kwarg1': '', 'kwarg2': '', 'kwarg3': ''}
        with patch.object(UserDocument, 'col') as mock_col:
            mock_find = mock_col.find
            UserDocument.find(*args, **kwargs)

        self.assertEqual(mock_find.call_args_list, [Call(*args, **kwargs)])

    def test_find_no_document(self):
        self.assertEqual(list(UserDocument.find()), [])

    def test_delete(self):
        user = UserDocument()
        user.save()

        args = ['arg1', 'arg2', 'arg3']
        kwargs = {'kwarg1': '', 'kwarg2': '', 'kwarg3': ''}

        with patch.object(UserDocument, 'col') as mock_col:
            mock_remove = mock_col.remove
            user.delete(*args, **kwargs)

        args.insert(0, {'_id': user._id})
        self.assertEqual(mock_remove.call_args_list, [Call(*args, **kwargs)])

    def test_remove_unsaved(self):
        user = UserDocument()

        self.assertRaises(InvalidOperation, user.delete)

    def test_save(self):
        user = UserDocument()

        kwargs = {'kwarg1': '', 'kwarg2': '', 'kwarg3': ''}

        with patch.object(UserDocument, 'col') as mock_col:
            mock_save = mock_col.save
            user.save(reload=False, **kwargs)

        args = [user]
        self.assertEqual(mock_save.call_args_list, [Call(*args, **kwargs)])

    def test_save_private_attr(self):
        document = UserDocument()
        document.first_name = 'Boris'
        document.name = 'FELD'
        document.save()

        self.assertEqual(UserDocument.col.find_one(), {'first_name': 'Boris',
            'name': 'FELD', '_id': document._id})

    def test_reload(self):
        document = UserDocument()
        document.first_name = 'Boris'
        document.name = 'FELD'
        document.save()

        UserDocument.col.update({'_id': document._id},
                                {'$set': {'name': 'SMITH'}})

        document.reload()

        self.assertEqual(document,
            {'_id': document._id, 'first_name': 'Boris', 'name': 'SMITH'})

    def test_reload_error(self):
        user = UserDocument()

        self.assertRaises(InvalidOperation, user.reload)

    def test_reload_invalid_id(self):
        user = UserDocument()
        user['_id'] = pymongo.objectid.ObjectId('1'*24)

        self.assertRaises(OperationFailure, user.reload)

    def test_reload_remove_fields(self):
        document = UserDocument()
        document.first_name = 'Boris'
        document.name = 'FELD'
        document.age = 21
        document.save()

        UserDocument.col.update({'_id': document._id},
                                {'$unset': {'age': 1}})
        document.reload()

        self.assertEqual(document,
            {'_id': document._id, 'first_name': 'Boris', 'name': 'FELD'})

    def test_reload_remove_fiels_and_private_fiels(self):
        document = UserDocument()
        document.first_name = 'Boris'
        document.name = 'FELD'
        document.age = 21
        document._private = 42
        document.save()

        self.assertEqual(UserDocument.col.find_one(), {'first_name': 'Boris',
            'name': 'FELD', 'age': 21, '_id': document._id})
        UserDocument.col.update({'_id': document._id},
                                {'$unset': {'age': 1}})
        document.reload()

        self.assertEqual(document,
            {'_id': document._id, 'first_name': 'Boris', 'name': 'FELD'})
        self.assertEqual(document._private, 42)

    def test_advanced_find(self):
        UserDocument.col.insert({'first_name': 'Boris', 'name': 'FELD'}, safe=True)
        UserDocument.col.insert({'first_name': 'John', 'name': 'SMITH'}, safe=True)
        UserDocument.col.insert({'first_name': 'Mickael', 'name': 'JOHNSON'}, safe=True)

        expected_values = ['John SMITH', 'Mickael JOHNSON']
        values = [doc.full_name() for doc in UserDocument.find().sort('_id').limit(2).skip(1)]

        self.assertEqual(values, expected_values)


class DocumentConfigurationTestCase(unittest.TestCase):

    def setUp(self):
        ConnectionManager.configure()

    def tearDown(self):
        UserDocument.col.remove()
        for attr in ('collection_name', 'config_name'):
            setattr(UserDocument, attr, None)

    def test_default_configuration(self):
        self.assertEqual(UserDocument.con.HOST, 'localhost')
        self.assertEqual(UserDocument.con.PORT, 27017)
        self.assertEqual(UserDocument.db.name, 'test')
        self.assertEqual(UserDocument.col.name, 'userdocument')

    def test_configuration_use_collection_attr(self):
        collection_name = 'my_collection'
        UserDocument.collection_name = collection_name

        self.assertEqual(UserDocument.col.name, collection_name)

    def test_configuration_use_config_name(self):
        config_name = 'my_custom_config_name'
        UserDocument.config_name = config_name

        db_name = 'my_custom_db'

        ConnectionManager.configure({config_name: {'db': db_name}})

        self.assertEqual(UserDocument.db.name, db_name)
        self.assertEqual(UserDocument.col.database.name, db_name)

    def test_configuration_use_col_from_config(self):
        config_name = 'my_custom_config_name'
        UserDocument.config_name = config_name

        col_name = 'my_custom_col'
        ConnectionManager.configure({config_name: {'col': col_name}})

        self.assertEqual(UserDocument.col.name, col_name)

class DocumentIndexesTestCase(unittest.TestCase):

    def setUp(self):
        ConnectionManager.configure()

    def tearDown(self):
        SimpleIndex.col.drop_indexes()
        SimpleIndex.col.remove()

    def test_simple_index(self):
        index = {'fields': ('something',)}

        SimpleIndex.indexes = [index]

        with patch.object(SimpleIndex, 'col') as mock_col:
            mock_ensure_index = mock_col.ensure_index
            SimpleIndex.generate_index()

        self.assertEqual(mock_ensure_index.call_args_list, [Call('something')])

    def test_simple_descending_index(self):
        index = {'fields': ('-something',)}

        SimpleIndex.indexes = [index]

        with patch.object(SimpleIndex, 'col') as mock_col:
            mock_ensure_index = mock_col.ensure_index
            SimpleIndex.generate_index()

        call_indexes = [('something', pymongo.DESCENDING)]
        self.assertEqual(mock_ensure_index.call_args_list, [Call(call_indexes)])

    def test_multiple_index(self):
        index = {'fields': ('something', 'something_else')}

        SimpleIndex.indexes = [index]

        with patch.object(SimpleIndex, 'col') as mock_col:
            mock_ensure_index = mock_col.ensure_index
            SimpleIndex.generate_index()

        call_indexes = [('something', pymongo.ASCENDING),
                        ('something_else', pymongo.ASCENDING)]
        self.assertEqual(mock_ensure_index.call_args_list, [Call(call_indexes)])

    def test_mutliple_combined(self):
        index = {'fields': ('something', '-something_else')}

        SimpleIndex.indexes = [index]

        with patch.object(SimpleIndex, 'col') as mock_col:
            mock_ensure_index = mock_col.ensure_index
            SimpleIndex.generate_index()

        call_indexes = [('something', pymongo.ASCENDING),
                        ('something_else', pymongo.DESCENDING)]
        self.assertEqual(mock_ensure_index.call_args_list, [Call(call_indexes)])

    def test_additionnals_args(self):
        index = {'fields': ('something',), 'unique': True, 'ttl': 3600 * 24}

        SimpleIndex.indexes = [index]

        with patch.object(SimpleIndex, 'col') as mock_col:
            mock_ensure_index = mock_col.ensure_index
            SimpleIndex.generate_index()

        index.pop('fields')
        self.assertEqual(
            mock_ensure_index.call_args_list,
            [Call('something', **index)])

    def test_generate_index_return(self):
        index = {'fields': ('something',)}

        SimpleIndex.indexes = [index]

        with patch.object(SimpleIndex, 'col') as mock_col:
            mock_ensure_index = mock_col.ensure_index
            mock_ensure_index.side_effect = lambda x: x
            results = SimpleIndex.generate_index()

        self.assertEqual(results, ['something'])

    def test_functionnal_index(self):
        index = {'fields': ('something',), 'unique': True, 'ttl': 3600 * 24}

        SimpleIndex.indexes = [index]
        SimpleIndex.generate_index()

        s = SimpleIndex()
        s.something = 'something'
        s.save(safe=True)

        s2 = SimpleIndex()
        s2.something = 'something'
        self.assertRaises(DuplicateKeyError, s2.save, safe=True)


    def test_multiple_generate(self):
        SimpleIndex.generate_index()
        SimpleIndex.generate_index()

class DocumentPropertiesTestCase(unittest.TestCase):

    def test_properties_setter(self):
        class CustomException(Exception):
            pass

        class TestSetter(Document):

            @property
            def test(self):
                pass

            @test.setter
            def test(self, value):
                raise CustomException()

        t = TestSetter()
        def setter():
            t.test = 'something'
        self.assertRaises(CustomException, setter)

class DocumentDefaultsTestCase(unittest.TestCase):

    def setUp(self):
        ConnectionManager.configure()

    def tearDown(self):
        DefaultDocument.col.remove()

    def test_simple(self):

        defaults = {'a': 1, 'b': 2, 'c': 3}

        DefaultDocument.default_values = defaults

        default_doc = DefaultDocument()

        self.assertEqual(default_doc, defaults)

    def test_callable(self):
        callable_value = 'sample'

        def sample_callable():
            return callable_value

        defaults = {'a': 1, 'b': sample_callable, 'c': 3}

        DefaultDocument.default_values = defaults

        default_doc = DefaultDocument()

        self.assertEqual(default_doc, {'a': 1, 'b': callable_value, 'c': 3})

    def test_no_defaults(self):

        defaults = {'a': 1, 'b': 2, 'c': 3}

        DefaultDocument.default_values = defaults

        default_doc = DefaultDocument(use_defaults=False)

        self.assertEqual(default_doc, {})

    def test_find_one_no_defaults(self):

        defaults = {'a': 1, 'b': 2, 'c': 3}

        DefaultDocument.default_values = defaults

        doc_id = DefaultDocument.col.insert({})

        self.assertEqual(DefaultDocument.find_one(), {'_id': doc_id})

    def test_find_no_defaults(self):
        defaults = {'a': 1, 'b': 2, 'c': 3}

        DefaultDocument.default_values = defaults

        doc_id_1 = DefaultDocument.col.insert({})
        doc_id_2 = DefaultDocument.col.insert({})
        doc_id_3 = DefaultDocument.col.insert({})

        self.assertEquals(list(DefaultDocument.find().sort('_id')),
                          [{'_id': doc_id_1}, {'_id': doc_id_2},
                           {'_id': doc_id_3}])

    def test_mutable_default(self):
        defaults = {'a': []}
        DefaultDocument.default_values = defaults

        doc1 = DefaultDocument()
        self.assertEqual(doc1.a, [])
        doc1.a.append('b')
        self.assertEqual(doc1.a, ['b'])

        doc2 = DefaultDocument()
        self.assertEqual(doc2.a, [])


class NestedDocumentTestCase(unittest.TestCase):

    def tearDown(self):
        DefaultDocument.col.remove()

    def test_nested_document_find_one(self):
        doc = DefaultDocument()
        doc.nested = {'nested': 'nested'}
        doc.save()

        db_doc = DefaultDocument.find_one()
        self.assertEqual(doc, db_doc)
        self.assertTrue(isinstance(db_doc, DefaultDocument))

    def test_nested_document_find(self):
        doc = DefaultDocument()
        doc.nested = {'nested': 'nested'}
        doc.save()

        db_doc = DefaultDocument.find()[0]
        self.assertEqual(doc, db_doc)
        self.assertTrue(isinstance(db_doc, DefaultDocument))

class DocumentValidationTestCase(unittest.TestCase):

    def setUp(self):
        self.old_validate = ValidationDocument.validate

    def tearDown(self):
        ValidationDocument.validate = self.old_validate
        ValidationDocument.col.remove()

    def test_validation(self):
        def validate(self):
            pass

        ValidationDocument.validate = validate
        doc = ValidationDocument()
        doc.save(validate=True)

    def test_validation_default_fail(self):
        doc = ValidationDocument()

        self.assertRaises(NotImplementedError, doc.save, validate=True)

    def test_validation_fail(self):
        exception = Exception('My custom exception')

        def validate(self):
            raise exception

        ValidationDocument.validate = validate
        doc = ValidationDocument()
        self.assertRaises(Exception, doc.save, validate=True)

        try:
            doc.validate()
        except Exception as e:
            self.assertEqual(e, exception)

    def test_validation_change_forbidden(self):

        def validate(self):
            self.x = 1

        ValidationDocument.validate = validate
        doc = ValidationDocument({'x': 0})

        self.assertRaises(ValidationError, doc.save, validate=True)


    def test_validation_change_forbidden(self):

        def validate(self):
            del self.x

        ValidationDocument.validate = validate
        doc = ValidationDocument({'x': 0})

        self.assertRaises(ValidationError, doc.save, validate=True)

class DocumentPrivateProtected(unittest.TestCase):

    def setUp(self):
        self.doc = Document()

    def tearDown(self):
        Document.col.remove()

    def test_private_attr_id(self):
        """We must be able to set the _id of a document
        """
        oid = ObjectId()
        self.doc._id = oid
        self.doc.save()

        print "Self doc", self.doc

        self.assertEqual(Document.col.find_one()['_id'], oid)

    def test_private_attr_during_save(self):
        self.doc.a = 'a'
        self.doc._b = 'b'

        self.doc.save()

        self.assertEqual(Document.find_one(), {'_id': self.doc._id,
            'a': self.doc.a})

class DocumentCursorTestCase(unittest.TestCase):

    def tearDown(self):
        Document.col.remove()

    def test_count(self):
        Document.col.insert({})
        Document.col.insert({})

        self.assertEqual(Document.find().count(), 2)

    def test_advanced_cursor_methods(self):
        for i in range(10):
            Document.col.insert({'i': i})

        objects = Document.find().skip(2).limit(5)

        self.assertEqual(range(2, 7), [o['i'] for o in objects])
