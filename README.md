pykraken2
=========

`pykraken2` provides a server/client implementation of [kraken2](https://github.com/DerrickWood/kraken2).

Installation
------------

> pykraken2 is pre-alpha software, there are currently no packaged versions.
> Please use the Development instructions before for installation and use.


**Development**

The Git repository contains Python code implementing a server and client pair,
it also contains a fork of kraken2 as a git submodule. To obtain the repository:

    git clone --recursive https://github.com/epi2me-labs/pykraken2.git

For the purposes of development the Python components of pykraken2 can be installed
using an in-place (editable) install:

    make develop

This will make a virtual environment at `./venv` and create and inplace
(editable) install of the Python code, along with compiling `kraken2` and
copying the executables to the `bin` directory of the virtual environment.


Usage
-----

Two command-line entry points are provided:

    pykraken2 server

to run a kraken2 service to classify reads, and

    pykraken2 client

to send read data to the service and receive results.

**API**

The server and client are straight-forward, we recommend the context managers
are used:

    # create a server
    from pykraken2.server import Server
    with Server(database, address, port, threads=8) as server:
        while True:
            pass

Although this code looks a little goofy, we intend to change the behaviour of
the context manager. `database` is a database directory created by kraken2,
`address` a location on which to listen for requests (e.g. `'localhost`),
`port` the network port on which to listen, and `threads` the number of threads
to kraken2 to use. An optional argument `k2_binary` can be used to specify the
location of a kraken2 binary to use.

A client can be constructed as:

    # create a client
    from pykraken2.client import Client
    with Client(address, ports) as client:
        with open('output.txt', 'w') as fh:
            for chunk in client.process_fastq(args.fastq):
                fh.write(chunk)

Currently the `process_fastq` iterator returns chunks of the kraken2 output.
The chunks are verbatim output: a chunk may contain multiple records, the last
record may be incomplete in any non-final chunk.
