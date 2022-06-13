"""pykraken2 server/client."""

import argparse
from enum import Enum
import importlib
import logging
import pickle

import msgpack
import portpicker

__version__ = "0.0.1"

ZMQ_MSG_SIZE = 10000


def get_named_logger(name):
    """Create a logger with a name."""
    name = name[0:8]
    logger = logging.getLogger('{}.{}'.format(__package__, name))
    logger.name = name
    return logger


def _log_level():
    """Parser to set logging level and acquire software version/commit."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter, add_help=False)

    modify_log_level = parser.add_mutually_exclusive_group()
    modify_log_level.add_argument(
        '--debug', action='store_const',
        dest='log_level', const=logging.DEBUG, default=logging.INFO,
        help='Verbose logging of debug information.')
    modify_log_level.add_argument(
        '--quiet', action='store_const',
        dest='log_level', const=logging.WARNING, default=logging.INFO,
        help='Minimal logging; warnings only).')

    return parser


def free_ports(number=1, lowest=1024):
    """Find a set of consecutive free ports.

    :param number: the number of ports required.
    :param lowest: lowest permissable port.

    ..note:: there maybe be race conditions, such that the
        returned list is not guaranteed to be free ports.
    """
    port_list = list()
    while True:
        if portpicker.is_port_free(lowest):
            port_list.append(lowest)
            if len(port_list) == number:
                break
        else:
            port_list = list()
        lowest += 1
        if lowest == 65536:
            raise RuntimeError("Cannot find free port set.")
    return port_list


class Signals(Enum):
    """Client/Server communication enum."""

    # client to server
    GET_TOKEN = 1
    FINISH_TRANSACTION = 2
    RUN_BATCH = 3
    # server to client
    TRANSACTION_NOT_DONE = 50
    TRANSACTION_COMPLETE = 51
    OK_TO_BEGIN = 52
    WAIT_FOR_TOKEN = 53


def _encode(obj):
    """Encode for msgpack."""
    print(obj, isinstance(obj, Signals))
    if isinstance(obj, Signals):
        return msgpack.ExtType(101, pickle.dumps(obj))
    raise TypeError("Unknown type: {}".format(obj))


def _decode(code, data):
    """Decode from msgpack."""
    if code == 101:
        return pickle.loads(data)
    return msgpack.ExtType(code, data)


def packb(message):
    """Pack data through msgpack."""
    return msgpack.packb(message, default=_encode, use_bin_type=True)


def unpackb(message):
    """Unpack data from msgpack."""
    return msgpack.unpackb(message, ext_hook=_decode, raw=False)


def cli():
    """Run pykraken2 entry point."""
    parser = argparse.ArgumentParser(
        'pykraken2',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        '-v', '--version', action='version',
        version='%(prog)s {}'.format(__version__))

    subparsers = parser.add_subparsers(
        title='subcommands', description='valid commands',
        help='additional help', dest='command')
    subparsers.required = True

    modules = ['server', 'client']
    for module in modules:
        mod = importlib.import_module('pykraken2.{}'.format(module))
        p = subparsers.add_parser(module, parents=[mod.argparser()])
        p.set_defaults(func=mod.main)
    args = parser.parse_args()

    logging.basicConfig(
        format='[%(asctime)s - %(name)s] %(message)s',
        datefmt='%H:%M:%S', level=logging.INFO)
    logger = logging.getLogger(__package__)
    logger.setLevel(args.log_level)
    args.func(args)
