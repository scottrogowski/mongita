import os
import re
import setuptools
import sys

DESCRIPTION = "Mongita is a lightweight embedded document database that " \
              "implements a commonly-used subset of the MongoDB/PyMongo interface."

if sys.version_info[:2] < (3, 6):
    print("ERROR: this package requires Python 3.6 or later!")
    sys.exit(1)

with open("README.md", "r") as fh:
    long_description = fh.read()

with open(os.path.join("mongita", "__init__.py")) as f:
    version = re.search(r"^VERSION \= \"([0-9.]+)\"", f.read(),
                        re.MULTILINE).group(1)

url_base = "https://github.com/scottrogowski/mongita"
download_url = '%s/archive/mongita-%s.tar.gz' % (url_base, version)

setuptools.setup(
    name="mongita",
    version=version,
    author="Scott Rogowski",
    author_email="scottmrogowski@gmail.com",
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url=url_base,
    download_url=download_url,
    packages=setuptools.find_packages(),
    scripts=["scripts/mongitasync"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        'pymongo>=3.0,<4.0',
        'sortedcontainers>=2.3,<3.0'
    ],
)
