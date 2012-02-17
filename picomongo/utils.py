from connection_manager import ConnectionManager

#Proxy

def _class_name(cls):
    return cls.__name__.lower()

class CMProxy(object):

    def __init__(self, attr_name):
        self.attr_name = attr_name

    def __get__(self, instance, owner):
        config_name = owner.config_name

        config_name = config_name if config_name else _class_name(owner)
        return getattr(ConnectionManager.get_config(config_name),
                       self.attr_name)

class CollectionDescriptor(object):

    def __get__(self, instance, owner):
        class_name = _class_name(owner)
        config_name = owner.config_name
        config_name = config_name if config_name else class_name

        config_col = ConnectionManager.get_config(config_name).col
        if not config_col:
            collection_name = owner.collection_name
            document_name = collection_name if collection_name else class_name
            return ConnectionManager.get_config(config_name).db[document_name]

        return config_col
