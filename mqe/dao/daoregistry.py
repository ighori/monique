import importlib
import inspect
import types
from collections import defaultdict

from mqe import mqeconfig
from mqe.dao.daobase import BaseDAO
from mqe.signals import dao_module_loaded, fire_signal


DAO_CLASS_BY_DATABASE_TYPE_BASE_CLASS = defaultdict(dict)
DAO_MODULE_PATHS_BY_DATABASE_TYPE = defaultdict(list)


def register_dao_class(database_type, cls):
    if not issubclass(cls, BaseDAO):
        raise ValueError('Class %r isn\'t a subclass of BaseDAO')
    base_classes = inspect.getmro(cls)
    for base_cls in inspect.getmro(cls):
        if base_classes[0] != base_cls and base_cls not in (BaseDAO, types.ObjectType):
            DAO_CLASS_BY_DATABASE_TYPE_BASE_CLASS[database_type][base_cls.__name__] = cls


def register_dao_module(database_type, module_path):
    DAO_MODULE_PATHS_BY_DATABASE_TYPE[database_type].append(module_path)


def register_dao_modules_from_config(config_module):
    if not hasattr(config_module, 'DAO_MODULES'):
        return
    for database_type, module_path in config_module.DAO_MODULES:
        register_dao_module(database_type, module_path)


register_dao_modules_from_config(mqeconfig)


def init_dao_modules(database_type):
    for module_path in DAO_MODULE_PATHS_BY_DATABASE_TYPE[mqeconfig.DATABASE_TYPE]:
        module = importlib.import_module(module_path)
        fire_signal(dao_module_loaded, database_type=database_type,
                    module_path=module_path, module=module)
        if hasattr(module, 'initialize'):
            module.initialize()
        for v in vars(module).values():
            if inspect.isclass(v) and issubclass(v, BaseDAO):
                register_dao_class(database_type, v)
    DAO_MODULE_PATHS_BY_DATABASE_TYPE[mqeconfig.DATABASE_TYPE] = []


class DAOInstances(object):
    """The class creates singleton instances of DAO implementations on first usage, created
    for the :attr:`mqeconfig.DATABASE_TYPE` specified in the config.

    The instance of a DAO class can be accessed by using a base class name as the attribute
    to get, for example::

        dao_inst = DAOInstances()
        report_dao = dao_inst.ReportDAO
        report = report_dao.select(id)
    """

    def __init__(self):
        self.instances = {}

    def __getattr__(self, dao_class):
        if dao_class not in self.instances:
            cls = DAO_CLASS_BY_DATABASE_TYPE_BASE_CLASS[mqeconfig.DATABASE_TYPE].get(dao_class)
            if not cls:
                init_dao_modules(mqeconfig.DATABASE_TYPE)
                cls = DAO_CLASS_BY_DATABASE_TYPE_BASE_CLASS[mqeconfig.DATABASE_TYPE].get(dao_class)
                if not cls:
                    raise ValueError('DAO class %r isn\'t registered for DATABASE_TYPE %r' % (
                        dao_class, mqeconfig.DATABASE_TYPE))
            self.instances[dao_class] = cls()
        return self.instances.get(dao_class)