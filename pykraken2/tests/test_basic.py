"""pykraken2 tests."""

from pathlib import Path
import shutil
import subprocess as sub
import tempfile
from threading import Thread
import unittest

from pykraken2.client import Client
from pykraken2.server import Server


class SimpleTest(unittest.TestCase):
    """Test class."""

    @classmethod
    def setUpClass(cls):
        """Set paths and other variables for tests."""
        data_dir = Path(__file__).parent / 'test_data'
        cls.out_dir = tempfile.mkdtemp()
        # cls.out_dir = data_dir / 'output'
        cls.database = data_dir / 'db'
        cls.fastq1 = data_dir / 'reads1.fq'
        cls.fastq2 = data_dir / 'reads2.fq'
        cls.expected_output1 = data_dir / 'correct_output' / 'k2out1.tsv'
        cls.expected_output2 = data_dir / 'correct_output' / 'k2out2.tsv'
        # cls.ports = free_ports()
        cls.port = 5555
        cls.address = '127.0.0.1'
        cls.threads = 4
        cls.k2_binary = 'venv/bin/kraken2'

    @classmethod
    def tearDownClass(cls):
        """Remove temporary files on test completion."""
        shutil.rmtree(cls.out_dir)

    def tearDown(self):
        """Clean up after each test."""
        for file_ in Path(self.out_dir).glob('**/*'):
            file_.unlink()

    def test_001_check_kraken_binary(self):
        """Test the bundled kraken2 binary."""
        try:
            sub.check_output([self.k2_binary, '--help'])
        except sub.CalledProcessError as e:
            self.assertTrue(False)
            self.skipTest(f"Kraken binary not working\n {e}")
        else:
            self.assertTrue(True)

    def test_002_create_server(self):
        """Test the server API."""
        server = Server(
            self.database, self.address, self.port,
            self.k2_binary, self.threads)
        server.run()
        server.terminate()

    def test_003_create_client(self):
        """Test the client API.

        The client will terminate automatically after sending all sequences
        to the server and receiving the DONE message. So would not normally
        need to use terminate()
        """
        client = Client(self.address, self.port)
        client.process_fastq(self.fastq2)
        client.terminate()

    def test_010_process_fastq(self):
        """Test single client."""
        server = Server(
            self.database, self.address, self.port,
            self.k2_binary, self.threads)
        server.run()

        client = Client(self.address, self.port)
        result = [x for x in client.process_fastq(self.fastq1)]
        server.terminate()

        with open(self.expected_output1, 'r') as corr_fh:
            corr_line = corr_fh.readlines()
            corr_str = ''.join(corr_line)

            client_str = ''.join(result)
            self.assertEqual(corr_str, client_str)

    def test_011_process_consecutive_fastq(self):
        """Run multiple files through a single Client instance.

        The use case for this would be the processing of a single sample
        split between multiple files.
        """
        server = Server(
            self.database, self.address, self.port,
            self.k2_binary, self.threads)
        server.run()

        client = Client(self.address, self.port)

        result = []
        for file_ in [self.fastq1, self.fastq2]:
            result.extend([x for x in client.process_fastq(file_)])

        expected_str = ""
        for exp in [self.expected_output1, self.expected_output2]:
            with open(exp, 'r') as fh:
                exp_lines = fh.readlines()
                expected_str += ''.join(exp_lines)

        client_str = ''.join(result)
        server.terminate()
        self.assertEqual(expected_str, client_str)

    def test_020_multi_client(self):
        """Client/server integration testing.

        A server instance and two client instances are run on threads.

        The results yielded by client.process_fastq and saved to a list.
        These results are compared to expected kraken2 results generated
        from using kraken2 directly.
        """
        def client_runner(input_, _results):
            client = Client(self.address, self.port)
            for chunk in client.process_fastq(input_):
                _results.extend(chunk)

        server = Server(
            self.database, self.address, self.port,
            self.k2_binary, self.threads)
        server.run()

        # Data for 2 client instances
        # [Sample_id, input, expected output, empty results list]
        client_data = [
            [self.fastq1, self.expected_output1],
            [self.fastq2, self.expected_output2]
        ]

        threads = []
        results = []
        for cdata in client_data:
            res = []
            results.append(res)
            thread = Thread(
                target=client_runner, args=(cdata[0], res))
            threads.append(thread)
            thread.start()

        for t in threads:
            t.join()

        # Compare the outputs
        for result, cdata in zip(results, client_data):
            expected = cdata[1]
            with open(expected, 'r') as corr_fh:
                corr_line = corr_fh.readlines()
                corr_str = ''.join(corr_line)

                client_str = ''.join(result)
                self.assertEqual(corr_str, client_str)

        server.terminate()
