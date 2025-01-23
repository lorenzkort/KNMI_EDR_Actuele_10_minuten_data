from setuptools import setup, find_packages

setup(
    name="knmi-weather-pipeline",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "psycopg2-binary",
        "xarray",
        "netcdf4",
        "requests",
        "pyarrow",
        "tenacity",
    ],
) 