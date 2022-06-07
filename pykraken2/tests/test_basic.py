import copy
import os
import tempfile
import unittest
from pathlib import Path

from pykraken2.k2server import Server
from pykraken2.k2client import Client

class SimpleTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        # TODO: need a test database and fastq
        data_dir = Path(__file__).parent / 'test_data'
        self.database = data_dir / 'db'
        self.fastq1 = data_dir / 'reads.fastq'
        self.fastq2 = self.fastq1
        self.ports = [5555, 5556]

    def test_001_create_server(self):
        server = Server()

    def test_002_create_client(self):
        client = Client()

    def test_010_process_fastq(self):
        server= Server()
        client = Client()

        server.start()
        results = list(client.process_fastq(self.fastq1))
        # TODO: check results
        assert False

    def test_020_multi_client(self):
        server= Server()
        client1 = Client()
        client2 = Client()

        server.start()
        # TODO: currently this is guaranteed to be fine, because process_fastq is synchronous
        #       put these calls in threads to attempt to interleave the calls
        results1 = list(client1.process_fastq(self.fastq1))
        results2 = list(client2.process_fastq(self.fastq2))
        # TODO: check results
        assert False
