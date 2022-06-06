"""Python kraken2 server/client"""
import argparse
import importlib
import json
import warnings

__version__ = "0.0.1"

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
    args.func(args)
