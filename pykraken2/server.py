"""pykraken2 server module."""


import argparse
from enum import Enum
import subprocess
import threading
from threading import Lock, Thread
import time

from msgpack import packb, unpackb
import zmq

import pykraken2
from pykraken2 import _log_level


class KrakenSignals(Enum):
    """Client/Server communication enum."""

    # client to server
    START_SAMPLE = 1
    SAMPLE_FINISHED = 2
    RUN_BATCH = 3
    # server to client
    NOT_DONE = 50
    DONE = 51


class Server:
    """
    Kraken2 server.

    This server runs two threads:

    recv_thread
        receives messages from k2clients, and feeds sentinel-delimited
        sequences to a kraken2 subprocess. After the STOP sentinel,
        dummy sequences are fed into the kraken2 subprocess stdin to flush
        out any remaining sequence results.

    return_thread
        Reads results from the kraken2 subprocess stdout and sends them back
        to the client.

    """

    CLASSIFIED_READS = 'kraken2.classified.fastq'
    UNCLASSIFIED_READS = 'kraken2.unclassified.fastq'
    FAKE_SEQUENCE_LENGTH = 50
    K2_BATCH_SIZE = 20

    def __init__(
            self, kraken_db_dir, address='localhost', ports=[5555, 5556],
            k2_binary='kraken2', threads=1):
        """
        Server constructor.

        :param address: address of the server:
            e.g. 127.0.0.1 or localhost
        :param ports: [input port, output port]
        :param kraken_db_dir: path to kraken2 database directory
        :param k2_binary: path to kraken2 binary
        :param threads: number of threads for kraken2
        """
        self.logger = pykraken2.get_named_logger('Server')
        self.kraken_db_dir = kraken_db_dir

        self.k2_binary = k2_binary
        self.threads = threads
        self.address = address
        self.ports = ports
        self.active = True

        self.recv_thread = None
        self.return_thread = None
        self.k2proc = None

        # self.client_connected = threading.Event()
        self.client_lock = Lock()
        # Have all seqs from current sample been passed to kraken
        self.all_seqs_submitted_event = threading.Event()
        # Are we waiting for processing of a sample to start
        self.start_sample_event = threading.Event()
        self.start_sample_event.set()
        # Signal to the threads to exit
        self.terminate_event = threading.Event()

        self.fake_sequence = (
            "@{}\n"
            f"{'T' * self.FAKE_SEQUENCE_LENGTH}\n"
            "+\n"
            f"{'!' * self.FAKE_SEQUENCE_LENGTH}\n")

        self.flush_seqs = "".join([
            self.fake_sequence.format(f"DUMMY_{x}")
            for x in range(self.K2_BATCH_SIZE)])

        # --batch-size sets number of reads that kraken will process before
        # writing results
        self.logger.info(f'k2 binary: {k2_binary}')

    def run(self):
        """Start the server.

        :raises IOError zmq cannot bind socket.
        """
        # TODO: outputs should go to a temp. directory that we clean up
        #       maybe as optional argument to ease logging/debugging.
        cmd = [
            'stdbuf', '-oL',
            self.k2_binary,
            '--unbuffered-output',
            '--db', self.kraken_db_dir,
            '--threads', str(self.threads),
            '--batch-size', str(self.K2_BATCH_SIZE),
            '/dev/fd/0'
        ]

        self.k2proc = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, universal_newlines=True, bufsize=1)

        self.logger.info('Loading kraken2 database')

        self.recv_thread = Thread(
            target=self.recv)
        self.recv_thread.start()

        self.return_thread = Thread(
            target=self.return_results)
        self.return_thread.start()

    def return_results(self):
        """
        Return kraken2 results to clients.

        Poll the kraken process stdout for results.

        Isolates the real results from the dummy results by looking for
        sentinels.
        """
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(f"tcp://{self.address}:{self.ports[1]}")

        while not self.terminate_event.is_set():
            if self.client_lock.locked():  # Is a client connected?
                if self.start_sample_event.is_set():
                    # remove any remaining dummy seqs from previous
                    # samples
                    while True:
                        line = self.k2proc.stdout.readline()
                        # TODO: Don't hardcode this
                        if line.startswith('U\tSTART'):
                            self.start_sample_event.clear()
                            break

                if self.all_seqs_submitted_event.is_set():
                    # Get the final chunk from the K2 stdout
                    self.logger.info('Checking for sentinel')

                    final_lines = []

                    while True:
                        line = self.k2proc.stdout.readline()

                        if line.startswith('U\tEND'):
                            self.logger.info('Found termination sentinel')

                            final_bit = "".join(final_lines).encode('UTF-8')
                            socket.send_multipart(
                                [packb(KrakenSignals.NOT_DONE.value),
                                 final_bit])
                            socket.recv()

                            # Client can receive no more messages after the
                            # DONE signal is returned
                            # TODO: why are we sending the a list?
                            socket.send_multipart(
                                [packb(KrakenSignals.DONE.value),
                                 packb(KrakenSignals.DONE.value)])
                            socket.recv()

                            self.logger.debug('Stop sentinel found')
                            break
                        else:
                            final_lines.append(line)

                    self.logger.info('Releasing lock')
                    self.client_lock.release()
                    continue

                # Send kraken enough reads so it will spit results out
                # TODO: don't hardcode 10000
                stdout = self.k2proc.stdout.read(10000).encode('UTF-8')

                socket.send_multipart(
                    [packb(KrakenSignals.NOT_DONE.value), stdout])
                socket.recv()

            else:
                self.logger.info('Waiting for lock')
                time.sleep(1)
        socket.close()
        context.term()
        self.logger.info('Return thread exiting')

    def recv(self):
        """
        Receive signals from client.

        Listens for messages from the input socket and forward them to
        the appropriate functions.
        """
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        try:
            socket.bind(f'tcp://{self.address}:{self.ports[0]}')
        except zmq.error.ZMQError as e:
            raise IOError(
                f'Port in use: Try "kill -9 `lsof -i tcp:{self.ports[0]}`"') \
                from e

        poller = zmq.Poller()
        poller.register(socket, flags=zmq.POLLIN)
        self.logger.info('Waiting for connections')

        while not self.terminate_event.is_set():
            if poller.poll(timeout=1000):
                query = (socket.recv_multipart())
                route = KrakenSignals(unpackb(query[0])).name.lower()
                msg = getattr(self, route)(query[1])
                socket.send(msg)
        socket.close()
        context.term()
        self.logger.info('recv thread existing')

    def start_sample(self, sample_id) -> bytes:
        """
        Get locks on client and start processing a sample.

        If no current lock, set lock and inform client to start sending data.
        If lock acquired, return 1 else return 0.
        """
        if not self.client_lock.locked():
            self.client_lock.acquire()
            # TODO: don't hardcode the sequence name
            self.k2proc.stdin.write(self.fake_sequence.format('START'))
            self.start_sample_event.set()
            self.all_seqs_submitted_event.clear()
            self.logger.info(f"Got lock for {sample_id}")
            reply = True
        else:
            reply = False
        return packb(reply)

    def run_batch(self, msg: bytes) -> bytes:
        """Process a data chunk.

        :param msg: a chunk of sequence data.
        """
        seq = msg.decode('UTF-8')
        self.k2proc.stdin.write(seq)
        self.k2proc.stdin.flush()
        return b'Server: Chunk received'

    def sample_finished(self, sample_id: bytes) -> bytes:
        """
        All data has been sent from a client.

        Insert STOP sentinel into kraken2 stdin.
        Flush the buffer with some dummy seqs.
        """
        self.all_seqs_submitted_event.set()
        # Is self.all_seqs_submitted guaranteed to be set in the next iteration
        # of the while loop in the return_results thread?
        self.k2proc.stdin.write(self.fake_sequence.format('END'))
        self.logger.info('flushing')
        self.k2proc.stdin.write(self.flush_seqs)
        self.logger.info("All dummy seqs written")
        return (
            "Got STOP signal from client for "
            f"{sample_id}").encode('UTF-8')

    def terminate(self):
        """Wait for processing and threads to terminate and exit."""
        self.logger.debug('Waiting for processing to finish')
        self.terminate_event.set()
        self.recv_thread.join()
        self.return_thread.join()
        self.logger.info('Exiting!')


def main(args):
    """Entry point to run a kraken2 server."""
    server = Server(
        args.database, args.address, args.ports, args.k2_binary, args.threads)
    server.run()


def argparser():
    """Argument parser for entrypoint."""
    parser = argparse.ArgumentParser(
        "kraken2 server",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[_log_level()], add_help=False)
    parser.add_argument('database')
    parser.add_argument("--address", default='localhost')
    parser.add_argument('--ports', nargs='+', default=[5555, 5556])
    parser.add_argument('--threads', default=8)
    parser.add_argument('--k2-binary', default='kraken2')
    return parser
