from mqe.dao import daoregistry


class Context(object):
    """The context that stores runtime resources, like database connections and DAO classes'
    instances.

    The instance of the :class:`Context` class is available as :attr:`mqe.c`
    (the class is meant to be used as a singleton).
    """

    def __init__(self):
        self.dao = daoregistry.DAOInstances()


