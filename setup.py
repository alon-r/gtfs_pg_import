from setuptools import setup

setup(
    name='gtfs_import',
    version='0.1.0',
    py_modules=['main'],
    install_requires=[
        'pandas==1.4.2',
        'requests==2.27.1',
        'SQLAlchemy==1.4.36',
        'GeoAlchemy2==0.11.1',
        'Shapely==1.8.2',
        'psycopg2-binary==2.9.3',
        'geopandas==0.10.2',
        'click==8.1.3'
    ],
    entry_points={
        'console_scripts': [
            'gtfs_import=main:cli',
        ],
    },
    package_dir={'': 'src'}
)