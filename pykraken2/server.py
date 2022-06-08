"""pykraken2 server module."""


import argparse
import datetime
from enum import Enum
import subprocess as sub
from threading import Thread
import time

import zmq

from pykraken2 import _log_level


class KrakenSignals(Enum):
    """Client/Sever communication enum."""

    # client to server
    START = b'1'
    STOP = b'2'
    RUN_BATCH = b'3'
    # server to client
    NOT_DONE = b'50'
    DONE = b'51'


def to_bytes(string) -> bytes:
    """Encode strings."""
    return string.encode('UTF-8')


def to_string(bytes_) -> str:
    """Decode strings."""
    return bytes_.decode('UTF-8')


class Server:
    """
    Server class.

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

    FULL_OUTPUT = 'all_kraken2_output'
    CLASSIFIED_READS = 'kraken2.classified.fastq'
    UNCLASSIFIED_READS = 'kraken2.unclassified.fastq'
    FAKE_SEQUENCE_LENGTH = 50
    K2_READBUF_SIZE = 20  # TODO: is this tied to kraken2 executable?

    def __init__(self, address, ports, kraken_db_dir, k2_binary, threads):
        """
        Sever constructor.

        :param address: server ip
        :param ports: [input port, output port]
        :param kraken_db_dir: path to kraken2 database directory
        :param k2_binary: path to kraken2 binary
        :param threads: cpus to use
        """
        self.kraken_db_dir = kraken_db_dir

        self.k2_binary = k2_binary
        self.threads = threads
        self.address = address
        self.ports = ports
        self.active = True

        self.input_context = zmq.Context()
        self.input_socket = self.input_context.socket(zmq.REP)

        self.return_context = zmq.Context()
        self.return_socket = self.return_context.socket(zmq.REQ)

        self.recv_thread = None
        self.return_thread = None
        self.k2proc = None

        # If a client is connected, lock prevents other connections
        self.lock = False
        # Have all seqs from current sample been passed to kraken
        self.all_seqs_submitted = False
        # Are we waiting for processing of a sample to start
        self.starting_sample = True

        self.fake_sequence = (
            "@{}\n"
            f"{'T' * self.FAKE_SEQUENCE_LENGTH}\n"
            "+\n"
            f"{'!' * self.FAKE_SEQUENCE_LENGTH}\n")

        self.flush_seqs = "".join([
            self.fake_sequence.format(f"DUMMY_{x}")
            for x in range(self.K2_READBUF_SIZE)])

        # --batch-size sets number of reads that kraken will process before
        # writing results
        print('k2 binary', k2_binary)

    def run(self):
        """Start the server."""
        # TODO: outputs should go to a temp. directory that we clean up
        #       maybe as optional argument to ease logging/debugging.
        cmd = [
            'stdbuf', '-oL',
            self.k2_binary,
            '--report', 'kraken2_report.txt',
            '--unbuffered-output',
            '--classified-out', self.CLASSIFIED_READS,
            '--unclassified-out', self.UNCLASSIFIED_READS,
            '--db', self.kraken_db_dir,
            '--threads', str(self.threads),
            '--batch-size', str(self.K2_READBUF_SIZE),
            '/dev/fd/0'
        ]

        self.k2proc = sub.Popen(
            cmd, stdin=sub.PIPE, stdout=sub.PIPE, stderr=sub.PIPE,
            universal_newlines=True, bufsize=1)

        # Wait for database loading before binding to input socket
        print('Loading kraken2 database')
        start = datetime.datetime.now()

        while True:
            stderr = self.k2proc.stderr.readline()
            if 'done' in stderr:
                print('Database loaded. Binding to input socket')
                break

        end = datetime.datetime.now()
        delta = end - start
        print(f'Kraken database loading duration: {delta}')

        try:
            self.input_socket.bind(f'tcp://{self.address}:{self.ports[0]}')
        except zmq.error.ZMQError:
            exit(
                f'Sever: Port in use: '
                f'Try "kill -9 `lsof -i tcp:{self.ports[0]}`"')

        self.return_socket.connect(f"tcp://{self.address}:{self.ports[1]}")

        self.recv_thread = Thread(target=self.recv)
        self.recv_thread.start()

        self.return_thread = Thread(target=self.return_results)
        self.return_thread.start()

    def do_final_chunk(self):
        """
        Process the final chunk of data from a sample.

        All data has been submitted to the K2 subprocess along with a stop
        sentinel and dummy seqs to flush.
        Search for this sentinel and send all lines before it to the client
        along with a DONE message.
        """
        lines = []
        while self.active:

            line = self.k2proc.stdout.readline()

            if line.startswith('U\tEND'):
                print('Server: Found termination sentinel')
                # Include last chunk up to (and including for testing)
                # the stop sentinel

                final_bit = "".join(lines)
                self.return_socket.send_multipart(
                    [KrakenSignals.NOT_DONE.value, to_bytes(final_bit)])
                self.return_socket.recv()

                # Client can receive no more messages after the
                # DONE signal is returned
                # TODO: why are we sending the a list?
                self.return_socket.send_multipart(
                    [KrakenSignals.DONE.value, KrakenSignals.DONE.value])
                self.return_socket.recv()

                print('server: Stop sentinel found')
                return
            lines.append(line)

    def return_results(self):
        """
        Return kraken2 results to clients.

        Poll the kraken process stdout for results.

        Isolates the real results from the dummy results by looking for
        sentinels.
        """
        while self.active:
            if self.lock:  # Is a client connected?
                if self.starting_sample:
                    # remove any remaining dummy seqs from previous
                    # samples
                    while True:
                        line = self.k2proc.stdout.readline()
                        # TODO: Don't hardcode this
                        if line.startswith('U\tSTART'):
                            self.starting_sample = False
                            break

                if self.all_seqs_submitted:
                    print('Server: Checking for sentinel')
                    self.do_final_chunk()
                    print('Server: releasing lock')
                    self.lock = False
                    continue

                # Send kraken enough reads so it will spit results out
                # TODO: don't hardcode 10000
                stdout = self.k2proc.stdout.read(10000)

                self.return_socket.send_multipart(
                    [KrakenSignals.NOT_DONE.value, to_bytes(stdout)])
                self.return_socket.recv()

            else:
                print('Server: waiting for lock')
                time.sleep(1)
        self.return_socket.close()
        self.return_context.term()
        print('return thread exiting')

    def recv(self):
        """
        Receive signals from client.

        Listens for messages from the input socket and forward them to
        the appropriate functions.
        """
        poller = zmq.Poller()
        poller.register(self.input_socket, flags=zmq.POLLIN)
        print('Server: Waiting for connections')

        while self.active:
            if poller.poll(timeout=1000):
                query = self.input_socket.recv_multipart()
                route = KrakenSignals(query[0]).name.lower()
                msg = getattr(self, route)(query[1])
                self.input_socket.send(msg)
        self.input_socket.close()
        self.input_context.term()
        print('recv thread existing')

    def start(self, sample_id) -> bytes:
        """
        Get locks on client and start processing a sample.

        If no current lock, set lock and inform client to start sending data.
        If lock acquired, return 1 else return 0.
        """
        if self.lock is False:
            # TODO: don't hardcode the sequence name
            self.k2proc.stdin.write(self.fake_sequence.format('START'))
            self.starting_sample = True
            self.all_seqs_submitted = False
            self.lock = True  # Starts sending results back
            print(f"Server: got lock for {sample_id}")
            reply = '1'
        else:
            reply = '0'
        return to_bytes(reply)

    def run_batch(self, msg: bytes) -> bytes:
        """Process a data chunk.

        :param msg: a chunk of sequence data.
        """
        seq = msg.decode('UTF-8')
        self.k2proc.stdin.write(seq)
        self.k2proc.stdin.flush()
        return b'Server: Chunk received'

    def stop(self, sample_id: bytes) -> bytes:
        """
        All data has been sent from a client.

        Insert STOP sentinel into kraken2 stdin.
        Flush the buffer with some dummy seqs.
        """
        self.all_seqs_submitted = True
        # Is self.all_seqs_submitted guaranteed to be set in the next iteration
        # of the while loop in the return_results thread?

        # TODO: don't hardcode the sequence name
        self.k2proc.stdin.write(self.fake_sequence.format('END'))
        print('Server: flushing')
        self.k2proc.stdin.write(self.flush_seqs)
        print("Server: All dummy seqs written")
        return to_bytes(f'Server got STOP signal from client for {sample_id}')

    def terminate(self):
        """Wait for threads to terminate and exit."""
        self.active = False
        while True:
            if all([not x.is_alive() for x in
                    [self.recv_thread, self.return_thread]]):
                break
            time.sleep(1)
        print('Server: exiting')


def main(args):
    """Entry point to run a kraken2 server."""
    server = Server(args.ports, args.database, args.k2_binary, args.threads)
    server.run()


def argparser():
    """Argument parser for entrypoint."""
    parser = argparse.ArgumentParser(
        "kraken2 server",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[_log_level()], add_help=False)
    parser.add_argument('database')
    # TODO: we only seem to use one port.
    parser.add_argument('--ports', nargs='+')
    parser.add_argument('--threads', default=8)
    parser.add_argument('--k2-binary', default='kraken2')
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    main(argparser())
