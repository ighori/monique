Other library features
======================

Configuration module
--------------------

The configuration module `mqeconfig.py <https://github.com/monique-dashboards/monique/mqe/mqeconfig.py>`_ specifies various options, including connection parameters to a database. The options can be overridden by putting the ``mqeconfig_override.py`` file in the ``$PYTHONPATH``. The config variables defined in the file will overwrite the default values present in ``mqeconfig.py``. The ``mqeconfig.py`` file could be also copied in whole into the ``mqeconfig_override.py`` file to enable editing all options.

An alternative way is to set the needed options in code, before using the library's code::

    def init_monique():
        from mqe import mqeconfig
        mqeconfig.DATABASE_TYPE = 'cassandra'


.. _guide_context:

Context object
--------------

A context that stores runtime resources, like database connections, is available as :data:`mqe.c`. Applications can use it to store custom resources, for example::

    from mqe import c

    c.db_connection = connect()


Signals
-------

`Blinker <https://pythonhosted.org/blinker/>`_ library is used to provide signals issued when a specific event takes place. The following signals are available:

* :data:`.new_dashboard` - issued when a new dashboard is created
* :data:`.new_report` - issued when a new report is created
* :data:`.layout_modified` - issued when a layout is modified by SSC or TPCreator

Note that the signals are available only for events which cannot be normally detected. For example, there is no signal issued when a new report instance is created, because the creation is triggered by calling the :meth:`.process_input` method explicitly.

The signals receive the :ref:`context object <guide_context>` as the ``sender`` argument. Subscribing to a signal can be done in the following way::

    from mqe.signals import new_dashboard

    @new_dashboard.connect
    def on_new_dashboard(c, dashboard, **kwargs):
        print('New dashboard with id %s created' % dashboard.dashboard_id)


.. _guide_serialization:

JSON Serialization
------------------

The library provides support for serializing (:func:`.json_dumps`, :func:`.mjson`, :class:`.MqeJSONEncoder`) and de-serializing (:func:`.json_loads`, :class:`.MqeJSONDecoder`) JSON documents. The following types are supported:

* :class:`~uuid.UUID`
* :class:`~datetime.datetime`, :class:`~datetime.date`
* :class:`.SeriesSpec`
* :class:`.Tile`
* :data:`tile_config` and :data:`tile_options` (the objects are dictionaries, but contain series specs, datetimes and UUIDs)

The custom classes are serialized to a JSON object containing the ``__type__`` key identifying a type, and other keys hold an object's attributes. The following keys are used:

* :class:`~uuid.UUID` - ``__type__ == 'UUID'``, ``arg`` key holds the hex representation of bytes.
* :class:`~datetime.datetime`, :class:`~datetime.date` - ``__type__ == 'date'``, ``arg`` key holds the number of milliseconds since Unix epoch.
* :class:`.SeriesSpec` - ``__type__ == 'SeriesSpec'``, other keys: ``data_colno``, ``filtering_colno``, ``filtering_expr``, ``data_column_header``, ``data_column_header_for_name``, ``filtering_column_header``, ``static_name``
* :class:`.Tile` - ``__type__ == 'Tile'``, other keys: ``tile_id``, ``dashboard_id``, ``tile_options``


A custom class can support the serialization by using the :func:`.json_type` decorator and implementing the methods ``for_json`` and ``from_rawjson``. An example::


    @json_type('A')
    class A(object):

        def __init__(self, x, y):
            self.x = x
            self.y = y

        def for_json(self):
            return {'x': self.x, 'y': self.y}

        @staticmethod
        def from_rawjson(obj):
            return A(obj['x'], obj['y'])

The argument to the :func:`.json_type` decorator defines the value put under the ``__type__`` key. The method ``for_json`` must return a dictionary defining other attributes put in the serialized object. Deserialization is implemented using a static method ``from_rawjson``, which receives a dictionary returned previously by ``for_json`` and based on it should return the class' instance.


DAO interface
-------------

The interface to a database is defined using Data Access Object classes. The classes are defined in the :mod:`.daobase` module. They aren't exposed in the library's API and shouldn't be normally used. Advanced users can access a DAO object implementing support for a database type specified in the configuration module with the context object, for example::

    from mqe import c

    report_dao = c.dao.ReportDAO

The ``c.dao`` object supports retrieving an implementation of a DAO class by accessing an attribute with a name equal to the base class name.

