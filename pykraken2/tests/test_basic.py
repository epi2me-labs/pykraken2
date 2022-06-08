"""pykraken tests."""

from pathlib import Path
import subprocess as sub
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
        cls.outdir = data_dir / 'output'
        cls.database = data_dir / 'db'
        cls.fastq1 = data_dir / 'reads1.fq'
        cls.fastq2 = data_dir / 'reads2.fq'
        cls.expected_output1 = data_dir / 'correct_output' / 'k2out1.tsv'
        cls.expected_output2 = data_dir / 'correct_output' / 'k2out2.tsv'
        cls.ports = [5555, 5556]
        cls.address = '127.0.0.1'
        cls.threads = 4
        cls.k2_binary = Path(
            __file__).parent.parent.parent / 'venv' / 'bin' / 'kraken2'

    def tearDown(self):
        """Clean up after each test."""
        for file in Path(self.outdir).glob('**/*'):
            file.unlink()

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
            self.address, self.ports, self.database,
            self.k2_binary, self.threads)
        server.run()
        server.terminate()

    def test_003_create_client(self):
        """Test the client API.

        The client will terminate automatically after sending all sequences
        to the server and receiving the DONE message. So would not normally
        need to use terminate()
        """
        client = Client(self.address, self.ports, 'id1')
        client.process_fastq(self.fastq2)
        client.terminate()

    def test_004_process_fastq(self):
        """Test single client."""
        server = Server(
            self.address, self.ports, self.database,
            self.k2_binary, self.threads)
        server.run()

        client = Client(self.address, self.ports, 'id1')
        result = [x for x in client.process_fastq(self.fastq1)]
        with open(self.expected_output1, 'r') as corr_fh:
            corr_line = corr_fh.readlines()
            corr_str = ''.join(corr_line)

            client_str = ''.join(result)
            self.assertEqual(corr_str, client_str)
        server.terminate()

    def test_005_multi_client(self):
        """Client/server integration testing.

        A server instance and two client instances are run on threads.
        The results yielded by client.process_fastq and saved to a list.
        These results are compared to expected kraken2 results generated
        from using kraken2 directly.
        """
        def client_runner(sample_id, input_, results):
            client = Client(self.address, self.ports, sample_id)
            for chunk in client.process_fastq(input_):
                results.extend(chunk)

        server = Server(
            self.address, self.ports, self.database,
            self.k2_binary, self.threads)
        server_thread = Thread(target=server.run)
        server_thread.start()

        # Data for 2 client instances
        # [Sample_id, input, expected output, empty results list]
        client_data = [
            ['id1', self.fastq1, self.expected_output1, []],
            ['id2', self.fastq2, self.expected_output2, []]
        ]

        for cdata in client_data:
            thread = Thread(
                target=client_runner, args=(cdata[0], cdata[1], cdata[3]))
            # Put thread object onto client_data
            cdata.append(thread)
            thread.start()

        for t in client_data:
            t[4].join()

        # Compare the outputs
        for t in client_data:
            with open(t[2], 'r') as corr_fh:
                corr_line = corr_fh.readlines()
                corr_str = ''.join(corr_line)

                client_str = ''.join(t[3])
                self.assertEqual(corr_str, client_str)

        server.terminate()
