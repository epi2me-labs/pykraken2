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

    make develop

This will make a virtual environment at `./venv` and create and inpalce (editable)
install of the Python code, along with compiling `kraken2` and copying the executables
to the `bin` directory of the virtual environment.


Usage
-----

