import copy
import unittest

from pymongo import Connection
from pymongo.database import Database
from pymongo.collection import Collection

from picomongo import ConnectionManager
from picomongo.connection_manager import _ConnectionManager
from picomongo.exceptions import NotConfiguredYet

class ConnectionManagerTestCase(unittest.TestCase):

    def setUp(self):
        self.connection_manager = _ConnectionManager()

    def test_simple(self):
        config = {
            '_default_': {
                'uri': 'mongodb://127.0.0.1:27017',
                'db': 'test'}}

        self.connection_manager.configure(config)

        default_config = self.connection_manager.get_config('_default_')

        self.assertTrue(isinstance(default_config.con, Connection))
        self.assertEqual(default_config.con.HOST, 'localhost')
        self.assertEqual(default_config.con.PORT, 27017)

        self.assertTrue(isinstance(default_config.db, Database))
        self.assertEqual(default_config.db.name, 'test')

    def test_works_on_copy(self):
        config = {
            '_default_': {
                'uri': 'mongodb://127.0.0.1:27017',
                'db': 'test'},
            'document': {
                'db': 'document'}}

        copy = config.copy()

        self.connection_manager.configure(config)

        self.assertEquals(config, copy)

    def test_not_configured_yet(self):
        self.assertRaises(NotConfiguredYet, self.connection_manager.get_config, '_default_')

    def test_default_config(self):
        """Test that connection manager use default host/port and db name if not specified.
        """
        config = {}

        self.connection_manager.configure(config)

        default_config = self.connection_manager.get_config('_default_')

        self.assertTrue(isinstance(default_config.con, Connection))
        self.assertEqual(default_config.con.HOST, 'localhost')
        self.assertEqual(default_config.con.PORT, 27017)

        self.assertTrue(isinstance(default_config.db, Database))
        self.assertEqual(default_config.db.name, 'test')

    def test_document_use_default_config(self):
        """Test that document use default config for connection and db if not specified.
        """
        config = {
            'document': {}
        }

        self.connection_manager.configure(config)

        document_config = self.connection_manager.get_config('document')

        self.assertEqual(document_config.con.HOST, 'localhost')
        self.assertEqual(document_config.con.PORT, 27017)
        self.assertEqual(document_config.db.name, 'test')

    def test_document_overload_default_config(self):
        """Test that document can overload host/port and/or db.
        """
        config = {
            'document': {'db': 'test2'}
        }

        self.connection_manager.configure(config)

        document_config = self.connection_manager.get_config('document')

        self.assertEqual(document_config.con.HOST, 'localhost')
        self.assertEqual(document_config.con.PORT, 27017)

        self.assertTrue(isinstance(document_config.db, Database))
        self.assertEqual(document_config.db.name, 'test2')

    def test_document_default(self):
        """Test that connection manager is able to generate a configuration for
        a document not given during configuration.
        """
        config = {'_default_': {'db': 'test2'}}

        self.connection_manager.configure(config)

        document_config = self.connection_manager.get_config('document')
        self.assertEqual(document_config.con.HOST, 'localhost')
        self.assertEqual(document_config.con.PORT, 27017)
        self.assertEqual(document_config.db.name, 'test2')

    def test_use_col(self):
        document_name = 'document_name'
        col_name = 'my_custom_col_name'
        config = {document_name: {'col': col_name}}

        self.connection_manager.configure(config)

        document_config = self.connection_manager.get_config(document_name)
        self.assertEqual(document_config.col.name, col_name)

    def test_no_col(self):
        self.connection_manager.configure()

        document_config = self.connection_manager.get_config('document')
        self.assertEqual(document_config.col, None)

    def test_reconfigure(self):
        self.connection_manager.configure()

        document_config = self.connection_manager.get_config('document')
        self.assertEqual(document_config.db.name, 'test')

        self.connection_manager.configure({'_default_': {'db': 'test2'}})

        document_config = self.connection_manager.get_config('document')
        self.assertEqual(document_config.db.name, 'test2')
