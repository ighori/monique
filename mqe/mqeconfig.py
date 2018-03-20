### The configuration of the Monique Dashboards library. The configuration can be
### overridden by putting a subset of config variables in mqeconfig_override.py file
### (which must be in PYTHONPATH).


### Database


#: Database type - one of 'sqlite3', 'cassandra'
DATABASE_TYPE = 'sqlalchemy'
#DATABASE_TYPE = 'cassandra'

#: Path to the Sqlite3 database file
SQLITE3_DATABASE = '/var/lib/monique.db'

SQLALCHEMY_ENGINE = 'postgresql+pg8000://postgres@/monique'

#: Connection parameters to the Cassandra database, specified as keyword arguments to the
#: :class:`cassandra.cluster.Cluster` class' ``__init__``.
CASSANDRA_CLUSTER = {
    'contact_points': ['127.0.0.1'],
    'port': 9042,
}

#: Whether to log the executed database queries
DEBUG_QUERIES = False


### Dashboards

#: The number of columns of a grid the dashboard tiles are placed on
DASHBOARD_COLS = 12

#: The default tile width
TILE_DEFAULT_WIDTH = 4

#: The default tile height
TILE_DEFAULT_HEIGHT = 4

#: The default colors to return in :data:`tile_data` for the data series.
DEFAULT_COLORS = ['#4E99B2', '#8ED2AB', '#B875B9', '#D56D4A', '#BDD3FF', '#D0E3A8', '#B9875B'  , '#AAA585', '#8FCFD5', '#CCFFCC', '#7A95D5']


### Reports


#: The maximal number of tags that can be attached to a report instance.
#: Note that a copy of a report instance is stored for each subset of tags
MAX_TAGS = 3



### Hooks


def get_table_from_parsing_result(parsing_result):
    """The function enables custom postprocessing of parsed tables. It must return an
    :class:`mqetables.enrichment.EnrichedTable` based on a :class:`mqetables.parseany.ParsingResult`.
    The returned table will be saved into the database as report instance data.
    If ``None`` will be returned, the report instance will not be created.
    """
    return parsing_result.table


def get_parsing_result_desc(parsing_result, table):
    """Set custom :attr:`.ReportInstance.parsing_result_desc` for a
    :class:`mqetables.parseany.ParsingResult` and an :class:`mqetables.enrichment.EnrichedTable`
    returned by :func:`get_table_from_parsing_result`. The return value must be a JSON-serializable dict.

    The function enables associating custom meta data with report instances and overriding the
    auto-computed attribute ``input_is_json``.
    """
    return {}


### Other limits

MAX_SERIES_POINTS = int(1e9)
MAX_SERIES_POINTS_IN_TILE = 10000
MAX_SERIES = 100
MAX_REPORT_COLUMNS = 1000
MAX_DASHBOARDS_WITH_SSCS_PER_REPORT = 50
MAX_TPCREATORS_PER_REPORT = 200
MAX_TPCREATED = 200


### DAO modules


DAO_MODULES = [
    ('cassandra', 'mqe.dao.cassandradb.cassandradao'),
    ('sqlite3', 'mqe.dao.sqlite3db.sqlite3dao'),
    ('sqlalchemy', 'mqe.dao.sqlalchemydao.sqlalchemydao'),
]


try:
    import mqeconfig_override
    reload(mqeconfig_override)
    from mqeconfig_override import *
except ImportError:
    pass
