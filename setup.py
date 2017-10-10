from setuptools import setup, find_packages

setup(
        name = 'monique',
        version = '1.2',
        packages = find_packages(exclude=['mqe.tests',]),
        install_requires = [
            'monique-tables',
            'blinker',
            'pytz',
        ],
        extras_require = {
            'cassandra': ['cassandra-driver']
        },
        zip_safe = False,
        package_data = {
            'mqe.migrations': ['*'],
        },
        author = 'Monique Dashboards',
        description = 'A library for creating dashboards/monitoring apps',
        long_description = '''
See description at `<https://github.com/monique-dashboards/monique>`_.
        ''',
        license = 'BSD',
        keywords = 'dashboards monitoring',
        url = 'https://github.com/monique-dashboards/monique',
        classifiers = [
            'Topic :: Database',
            'Environment :: Web Environment',
            'Development Status :: 5 - Production/Stable',
            'Topic :: Software Development :: Libraries :: Application Frameworks',
            'Topic :: System :: Monitoring',
            'Topic :: System :: Systems Administration',
            'License :: OSI Approved :: BSD License',
            'Programming Language :: Python :: 2.7',
        ],
)
