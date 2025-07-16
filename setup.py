import setuptools

VERSION = "0.1.3"
PACKAGE_NAME = "saferplacesapi"
AUTHOR = "Valerio Luzzi, Marco Renzi"
EMAIL = "valerio.luzzi@gecosistema.com, marco.renzi@gecosistema.com"
GITHUB = "https://github.com/SaferPlaces2023/saferplacesapi"
DESCRIPTION = "An utils functions package"

setuptools.setup(
    name=PACKAGE_NAME,
    version=VERSION,
    license='MIT',
    author=AUTHOR,
    author_email=EMAIL,
    description=DESCRIPTION,
    long_description=DESCRIPTION,
    url=GITHUB,
    packages=setuptools.find_packages("src"),
    package_dir={'': 'src'},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "pygeoapi",
        "pygrib",
        "boto3",
        "numpy<2.0",
        "gdal2numpy",
        "xarray",
        "rioxarray",
        "shapely",
        "geopandas",
        "rasterio",
        "python-crontab",
        "duckdb"
    ]
)
