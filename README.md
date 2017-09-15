![Logo](http://monique-dashboards.readthedocs.io/en/latest/_images/monique-dashboards-logo.png)

![PyPI](https://img.shields.io/pypi/v/monique.svg)
![readthedocs badge](https://readthedocs.org/projects/monique-dashboards/badge/?version=latest)
[![License](https://img.shields.io/badge/License-BSD%202--Clause-orange.svg)](https://opensource.org/licenses/BSD-2-Clause)


[Monique Dashboards](http://monique-dashboards.readthedocs.io/) is an innovative Python library for creating dashboard and monitoring applications the drops the traditional concept of a metric. It comes with fully functional, sample Web and HTTP API applications for creating custom dashboards: [Monique Web](https://github.com/monique-dashboards/monique-web) and [Monique API](https://github.com/monique-dashboards/monique-api).

## What's wrong with metrics

- a metric is a very low-level concept - it's just a `(name, value)` pair. It works well for monitoring CPU usage of a single server, but doesn't handle well more complex use cases
- it requires manual creation of each metric data point. Pushing metrics from JSON or SQL results is a very unwieldy task, requiring writing a lot of code and maintaining metric naming schemes
- it's made mainly for numbers - single strings might be supported, but forget about JSON documents

## Tagged tables as the solution

The library uses a **table** with an optional header and tags as the base data structure. Each cell is a JSON-serializable value.

Many inputs can be nicely represented as a table - SQL results, output of Unix utilities, or CSV files. The inputs already contain labels and headers that can be directly used as data series labels and chart titles.

JSON files need a flattening algorithm, but the result can be quite natural. For example, a document

    {
        "server": "192.168.1.10",
        "checks": [{
                "url": "http://web/login",
                "length": 12340,
                "timings": {
                    "connect": 0.120,
                    "dns": 0.212,
                    "download": 0.354
                }
            }, {
                "url": "http://web/docs",
                "length": 3245340,
                "timings": {
                    "connect": 0.110,
                    "dns": 0.018,
                    "download": 1.345
                }
            }]
    }



is converted to a table

server | checks.url | checks.length | checks.timings.connect | checks.timings.dns | checks.timings.download
-------|------------|---------------|------------------------|--------------------|----
"192.168.1.10" | "http://web/login" | 12340 | 0.12 | 0.212 | 0.354



status | length | timings.connect | timings.dns | timings.download
-------|--------|-----------------|-------------|-----------------
"OK" | 12340 | 0.12 | 0.212 | 0.354
"OK" | 12340 | 0.15 | 0.012 | 0.831


How the data is extracted from the tables? Using data series specification of the form:

    SELECT data_colno WHERE filtering_colno equals|contains <value>

The library comes with a function that guesses the specification:

![](https://vhex.net/public/ss_guessing.gif)


### Auto-parsing of JSON, ASCII tables, command output

The library includes parsers for multiple formats, like JSON, CSV, ASCII tables, single numbers and words. It also auto-detects an input format, making submitting the data as easy as:

    # directly send 'psql' output
    $ psql -c "SELECT name, points FROM user ORDER BY points DESC" | \
      curl --user WNKCPwiHfvIZRvfqsZa7Kai1: --request POST --data-binary @- https://mqeapi/reports/points

    # directly send 'df' output
    $ df | curl --user WNKCPwiHfvIZRvfqsZa7Kai1: --request POST --data-binary @- https://mqeapi/reports/diskfree
    
    # directly send health check result
    $ echo OK | curl --user WNKCPwiHfvIZRvfqsZa7Kai1: --request POST --data-binary @- https://mqeapi/reports/health

When data is sent using a programming language, the preferred format is JSON. The library will parse any JSON document, but the canonical representation of a table is an array of objects mapping a column name to a value:

    table = [{'name': 'monique', 'points': 123}, {'name': 'john', 'points': 34}]
    r = requests.post('https://mqeapi/reports/points',
                      params={'key': 'WNKCPwiHfvIZRvfqsZa7Kai1'},
                      json=instance)


### Auto-creation of new dashboard tiles

The library supports [automatic creation of tiles](http://monique-dashboards.readthedocs.io/en/latest/tpcreator.html) by employing the concept of a **master tile** that can be copied. The feature handles cases when multiple instances of the same entity are present: servers, microservice instances, stock prices. An entity is identified by **tags**:

    $ echo -en "requests 1000\nhealth OK" | curl --user WNKCPwiHfvIZRvfqsZa7Kai1: --request POST --data-binary @- \
      https://mqeapi/reports/microservice?tags=name:search,pid:21234

The tiles created from a master tile are automatically synchronized:

![](https://vhex.net/public/master-resizing.gif)

### Auto-creation of new data series

When SQL results contain new rows, in most cases they should be also included in a chart. The library supports this behaviour by [an automatic creation of data series](http://monique-dashboards.readthedocs.io/en/latest/sscreator.html).

### Dashboards' layouts management

The library uses a model of [immutable layout and tile definitions](http://monique-dashboards.readthedocs.io/en/latest/layouts.html), making handling concurrency and synchronizing state easy. Ready functions for placing and detaching tiles are available.

### Fetching data formatted for rendering a tile

Data of a dashboard tile, preformatted for rendering, [can be fetched easily](http://monique-dashboards.readthedocs.io/en/latest/tutorial.html#tutorial-tile-data). The library manages data series' names and colors, and enables [customizing tiles](http://monique-dashboards.readthedocs.io/en/latest/tilewidgets.html#formatting-tile-data-tilewidgets-and-drawers). Monique Dashboards don't depend on any frontend library.

### Managing data series directly

A lower-level API for managing [data series](http://monique-dashboards.readthedocs.io/en/latest/dataseries.html#a-lower-level-interface-to-data-series) and [reports](http://monique-dashboards.readthedocs.io/en/latest/reports.html) is available.

## How the library can be used

- for creating full "custom dashboards" applications (similar to [Monique Web](https://github.com/monique-dashboards/monique-web)), allowing a user to dynamically create tiles
- for creating predefined dashboards (replacing a "dashboard framework"). In that case data can be always sent using JSON. Auto-creating new dashboard tiles and data series can still be useful in that case.
- for parsing and storing multiple input types. The data can be retrieved using a lower-level interface.


## Documentation

The documentation is available on [Read The Docs](http://monique-dashboards.readthedocs.io).


## Installation

Monique Dashboards support SQLite3 and Cassandra databases. The first option enables simple, server-less setups, while the second supports mission-critical setups run using multi-server clusters. The installation instructions are available on [Read The Docs](http://monique-dashboards.readthedocs.io/en/latest/installation.html#a-lower-level-interface-to-data-series). [Monique Web](https://github.com/monique-dashboards/monique-web) and [Monique API](https://github.com/monique-dashboards/monique-api) require their own installation steps.

## Who uses Monique Dashboards

* [Monique.io](https://monique.io) - an enhanced web application + Javascript alarming, run using a highly-available cluster

