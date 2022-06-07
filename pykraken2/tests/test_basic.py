import copy
import os
import tempfile
import filecmp
import unittest
from pathlib import Path
import subprocess as sub

from threading import Thread

from pykraken2.k2server import Server
from pykraken2.k2client import Client


class SimpleTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        # TODO: need a test database and fastq
        data_dir = Path(__file__).parent / 'test_data'
        self.outdir = data_dir / 'output'
        self.out1 = self.outdir / 'test1.tsv'
        self.out2 = self.outdir / 'test2.tsv'
        self.database = data_dir / 'db'
        self.fastq1 = data_dir / 'reads.fastq'
        self.correct_output = data_dir / 'correct_output' / 'k2_out.tsv'
        self.fastq2 = self.fastq1
        self.ports = [5555, 5556]
        self.k2_binary = Path(
            __file__).parent.parent / 'bin' / 'kraken2_linux' / 'kraken2'

    # def tearDown(self) -> None:
    #     sub.call('kill -9 $(lsof -i tcp:5555-5556)', shell=True)

    # def test_001_check_binary(self):
    #     self.skipTest('skip')
    #     try:
    #         sub.check_output([self.k2_binary, '--help'])
    #     except sub.CalledProcessError as e:
    #         self.assertTrue(False)
    #         self.skipTest(f"Kraken binary not working\n {e}")
    #     else:
    #         self.assertTrue(True)


    # def test_002_create_client(self):
    #     client = Client(self.ports, self.out1, sample_id=1)
    #
    # def test_010_process_fastq(self):
    #     client = Client(self.ports, self.out2, sample_id=2)
    #     client.process_fastq(self.fastq2)
    #
    #     server.start()
    #     results = list(client.process_fastq(self.fastq1))
    #     # TODO: check results
    #     assert False

    def test_020_multi_client(self):
        server = Server(self.ports, self.database, self.k2_binary, threads=4)
        server_thread = Thread(target=server.run)

        client1 = Client(self.ports, self.out1, sample_id='1')
        client1_thread = Thread(target=client1.process_fastq,
                                args=(self.fastq1,))
        client1_thread.start()

        client2 = Client(self.ports, self.out2, sample_id='2')
        client2_thread = Thread(target=client2.process_fastq,
                                args=(self.fastq2,))
        client2_thread.start()

        server_thread.start()

        client1_thread.join()
        client2_thread.join()

        server.terminate()

        self.assertTrue(filecmp.cmp(self.out1, self.correct_output))
        self.assertTrue(filecmp.cmp(self.out2, self.correct_output))
