'''Connection manager module, use connection_manager.ConnectionManager to
access a shared state ConnectionManager.
'''

from collections import namedtuple

from pymongo import Connection
try:
    from pymongo import ReplicaSetConnection
except ImportError:
    ReplicaSetConnection = Connection

from exceptions import NotConfiguredYet

CONFIG = namedtuple('Config', ['con', 'db', 'col'])

class _ConnectionManager(object):
    '''Manage connection configuration (connection uri, database and collection for document classes).

    It use default host/port (localhost on 27017) if no uri is specified.
    It use default database (test) if no database is specified.
    '''

    def __init__(self):
        #Existing configurations
        self._configurations = {}

        #Default config
        self._default_con_uri = 'mongodb://localhost'
        self._default_db_name = 'test'

    def configure(self, config = None):
        '''Configure the connection manager.

        Take configuration as:
        {'_default_': {'uri': 'default_uri', 'db': 'default_db'},
         'document_name': {'uri': 'specific_uri', 'db': 'specific_db'},
         'document_name2': *
        }

        Uri must be a valid mongodb connection uri as described in this doc
        page: http://www.mongodb.org/display/DOCS/Connections

        Rules:
        * In default configuration:
          * If uri is not present, use 'mongodb://localhost'
          * If db is not present, use 'test'
        * In document configuration:
          * If uri is not present, use default uri
          * If db is not present, use default db
          * If col is not preset, use document class name
        '''
        if config == None:
            config = {}

        self._configurations = {}
        default = config.copy().pop('_default_', {})

        #Default
        self._default_con_uri = default.get('uri', self._default_con_uri)
        con = self._get_connection(self._default_con_uri)

        self._default_db_name = default.get('db', self._default_db_name)
        db = con[self._default_db_name]

        self._configurations['_default_'] = CONFIG(con, db, None)

        #Gen others
        for name, document_config in config.iteritems():
            self._gen_config(name, document_config)

    @staticmethod
    def _get_connection(connection_uri):
        if 'replicaSet=' in connection_uri:
            con = ReplicaSetConnection(connection_uri)
        else:
            con = Connection(connection_uri)
        return con

    def _gen_config(self, name, config=None):
        """Gen a config for a specified document name, use default values if
        necessary.
        """
        if config == None:
            config = {}

        connection = self._get_connection(config.get('uri', self._default_con_uri))
        db = connection[config.get('db', self._default_db_name)]
        col = db[config.get('col')] if config.get('col') else None
        self._configurations[name] = CONFIG(connection, db, col)

    def get_config(self, document_name):
        if not self._configurations:
            exc_msg = 'The connection manager has not yet been configured.'
            raise NotConfiguredYet(exc_msg)
        if not document_name in self._configurations:
            self._gen_config(document_name)
        return self._configurations[document_name]

ConnectionManager = _ConnectionManager()
