import copy
import os
import tempfile
import unittest

from pykraken2.server import Server
from pykraken2.client import Client

class SimpleTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        # TODO: need a test database and fastq
        data_dir os.path.dirname(__file__)
        self.database = os.path.join(data_dir, 'small_db')
        self.fastq1 = os.path.join(data_dir, 'reads1.fastq')
        self.fastq2 = os.path.join(data_dir, 'reads2.fastq')
        self.ports = [5555, 5556]

    def test_001_create_server(self):
        server = Server()

    def test_002_create_client(self):
        client = Client()

    def test_010_process_fastq(self):
        server= Server()
        client = client()

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
