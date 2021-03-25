import os
import re
import setuptools
import sys

if sys.version_info[:2] < (3, 7):
    print("ERROR: this package requires Python 3.7 or later!")
    sys.exit(1)

with open("README.md", "r") as fh:
    long_description = fh.read()

with open(os.path.join("mongita", "lib.py")) as f:
    version = re.search(r"^CLIENT_VERSION \= \"([0-9.]+)\"", f.read(),
                        re.MULTILINE).group(1)

url_base = "https://github.com/mongita/mongita"
download_url = '%s/archive/mongita-%s.tar.gz' % (url_base, version)

setuptools.setup(
    name="mongita",
    version=version,
    author="Scott Rogowski",
    author_email="scottmrogowski@gmail.com",
    description="Mongita is a lightweight embedded document database that implements a commonly-used subset of the MongoDB/PyMongo interface.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url=url_base,
    download_url=download_url,
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        'bson>=0.5,<0.6'
    ],
    extras_require={
        'aws': ['boto>=2.49,<3.0'],
        'gcp': ['google-cloud-storage>1.36,<=2.0'],
        'all': ['boto>=2.49,<3.0', 'google-cloud-storage>1.36,<=2.0'],
    }
)
