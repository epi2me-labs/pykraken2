pykraken2
=========

`pykraken2` provides a server/client implementation of [kraken2](https://github.com/DerrickWood/kraken2).

Installation
------------

pykraken2 is best installed with conda/mamba:

    mamba create -n pykraken2 -c epi2melabs pykraken2


# Development

For the purposes of development the Python components of pykraken2 can be installed
using an in-place (editable) install:

    pip install -e .

`kraken2` is included as a git submodule. It should be built and added to `PATH`
for use by the Python components.



Usage
-----

