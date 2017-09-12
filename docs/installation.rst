.. highlight:: sh


.. _installation:

Installation
============

The library requires Python 2.7. Using `PyPy <http://pypy.org>`_ (a JIT-compiled Python implementation) is strongly recommended due to performance benefits - the algorithms detecting and parsing various input types can use quite a lot of CPU. If performance is not a priority, CPython can be used.

The library can store data in the **SQLite3** database or the `Cassandra <http://cassandra.apache.org/>`_ database. The first option is meant for hobby setups which don't require high-availability or scalability, which the second option supports. Cassandra is normally run on a multi-server cluster, but also works well when run on a single server.


Installation using SQLite3
--------------------------

1. Install the ``monique`` package::

    $ pip install monique

2. Set the path to the database file: create a file ``mqeconfig_override.py`` (which must be in PYTHONPATH) with the content::

    DATABASE_TYPE = 'sqlite3'

    # Path to the Sqlite3 database file
    SQLITE3_DATABASE = '/var/lib/monique.db'

3. Ensure the database file exists::

    $ touch /var/lib/monique.db

4. Execute migration files with the extension ``.sqlite3``. The files are available in the `repository <https://github.com/monique-dashboards/monique/mqe/migrations>`_ and are also present in the Python package ``mqe.migrations``::

    $ migrations_dir=$(python -c 'from os.path import dirname; from mqe import migrations; print(dirname(migrations.__file__))')
    $ cat "$migrations_dir"/*.sqlite3 | sqlite3 /var/lib/monique.db

5. Check if the installation is working by executing sample code::

    $ wget https://raw.githubusercontent.com/monique-dashboards/monique/master/examples/createdashboard.py
    $ python createdashboard.py
    Successfully created a dashboard with a tile


Installation using Cassandra
----------------------------

The library works with Cassandra 3.x.

1. Install the ``monique`` package with the ``[cassandra]`` extras::

    $ pip install 'monique[cassandra]'

2. Set the connection parameters: create a file ``mqeconfig_override.py`` (which must be in PYTHONPATH) with the content::

    DATABASE_TYPE = 'cassandra'

    # Connection parameters to the Cassandra database, specified as keyword arguments to the
    # cassandra.cluster.Cluster class
    CASSANDRA_CLUSTER = {
        'contact_points': ['127.0.0.1'],
        'port': 9042,
    }

3. Create the ``mqe`` keyspace, for example::

    $ cqlsh 127.0.0.1 -e "CREATE KEYSPACE mqe WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"

4. Execute migration files with the extension ``.cql``. The files are available in the `repository <https://github.com/monique-dashboards/monique/mqe/migrations>`_ and are also present in the Python package ``mqe.migrations``::

    $ migrations_dir=$(python -c 'from os.path import dirname; from mqe import migrations; print(dirname(migrations.__file__))')
    $ for file in "$migrations_dir"/*.cql; do cqlsh 127.0.0.1 -f "$file"; done

5. Check if the installation is working by executing sample code::

    $ wget https://raw.githubusercontent.com/monique-dashboards/monique/master/examples/createdashboard.py
    $ python createdashboard.py
    Successfully created a dashboard with a tile


Installing Monique Web and Monique API
--------------------------------------

If you want to install `Monique Web <https://github.com/monique-dashboards/monique-web>`_ and `Monique API <https://github.com/monique-dashboards/monique-api>`_ - sample Web and HTTP API applications that use the Monique Dashboards library, please refer to the projects' Github pages.