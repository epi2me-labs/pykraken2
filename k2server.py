#! /usr/bin/env python

import time
import argparse
from threading import Thread
import subprocess as sub
import datetime

import zmq


# Client to server signals
START = 'start'
STOP = 'stop'
RUN_BATCH = 'run_batch'

# Server to client signals
NOT_DONE = 'notdone'
DONE = 'done'

FULL_OUTPUT = 'all_kraken2_output'
CLASSIFIED_READS = 'kraken2.classified.fastq'
UNCLASSIFIED_READS = 'kraken2.unclassified.fastq'


def to_bytes(string) -> bytes:
    return string.encode('UTF-8')


def to_string(bytes_) -> str:
    return bytes_.decode('UTF-8')


SENTINEL_SIZE = 50

SENTINEL = (
    "@{}\n"
    f"{'T' * SENTINEL_SIZE}\n"
    "+\n"
    f"{'!' * SENTINEL_SIZE}\n"
)


class Server:
    """
    Sever

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
    def __init__(self, ports, kraken_db_dir, k2_binary, threads):
        self.kraken_db_dir = kraken_db_dir
        self.context = zmq.Context()

        k2_readbuf_size = 20
        nuc = 'T' * SENTINEL_SIZE
        qual = '!' * SENTINEL_SIZE

        d = (
            "@DUMMY_{}\n"
            "{}\n"
            "+\n"
            "{}\n"
        )
        self.flush_seqs = "".join([
            d.format(x, nuc, qual) for x in range(k2_readbuf_size)])

        # --batch-size sets number of reads that kraken will process before
        # writing results
        print('k2 binary', k2_binary)

        cmd = [
            'stdbuf', '-oL',
            k2_binary,
            '--report', 'kraken2_report.txt',
            '--unbuffered-output',
            '--classified-out', CLASSIFIED_READS,
            '--unclassified-out', UNCLASSIFIED_READS,
            '--db', self.kraken_db_dir,
            '--threads', str(threads),
            '--batch-size', str(k2_readbuf_size),
            '/dev/fd/0'
        ]

        self.k2proc = sub.Popen(
            cmd, stdin=sub.PIPE, stdout=sub.PIPE, stderr=sub.PIPE,
            universal_newlines=True, bufsize=1)  # Set buffer to 1? line

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

        self.input_socket = self.context.socket(zmq.REP)

        try:
            self.input_socket.bind(f'tcp://127.0.0.1:{ports[0]}')
        except zmq.error.ZMQError:
            exit(f'Port in use: Do "kill -9 `lsof -i tcp:{ports[0]}`"')

        self.return_socket = self.context.socket(zmq.REQ)
        self.return_socket.connect(f"tcp://127.0.0.1:5556")

        # If a client is connected, lock prevents other connections
        self.lock = False
        # Have all seqs from current sample been passed to kraken
        self.all_seqs_submitted = False
        # Are we waiting for processing of a sample to start
        self.starting_sample = True

        self.recv_thread = Thread(target=self.recv)
        self.recv_thread.start()

        self.return_thread = Thread(target=self.return_results)
        self.return_thread.start()

    def do_final_chunk(self):
        """
        All data has been submitted to the K2 subprocess along with a stop
        sentinel and dummy seqs to flush.
        Search for this sentinel and send all lines before it to the client
        along with a DONE message
        """

        lines = []
        while True:

            line = self.k2proc.stdout.readline()
            lines.append(line)
            if line.startswith('U\tEND'):
                print('Server: Found termination sentinel')
                # Include last chunk up to (and including for testing)
                # the stop sentinel

                final_bit = "".join(lines)
                self.return_socket.send_multipart(
                    [to_bytes(NOT_DONE), to_bytes(final_bit)])
                self.return_socket.recv()

                # Client can receive no more messages after the
                # DONE signal is returned
                self.return_socket.send_multipart(
                    [to_bytes(DONE), to_bytes(DONE)])
                self.return_socket.recv()

                print('server: Stop sentinel found')
                return

    def return_results(self):
        """
        Return kraken2 results to clients.

        Poll the kraken process stdout for results.

        Isolates the real results from the dummy results by looking for
        sentinels.
        """

        while True:
            if self.lock:  # Is a client connected?
                if self.starting_sample:
                    # remove any remaining dummy seqs from previous
                    # samples
                    while True:
                        line = self.k2proc.stdout.readline()
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
                stdout = self.k2proc.stdout.read(10000)

                self.return_socket.send_multipart(
                    [to_bytes(NOT_DONE), to_bytes(stdout)])
                self.return_socket.recv()

            else:
                print('Server: waiting for lock')
                time.sleep(1)

    def recv(self):
        """
        Listens for messages from the input socket and forwards them to
        the appropriate functions.
        """
        print('Server: Waiting for connections')
        while True:
            query = self.input_socket.recv_multipart()
            route = to_string(query[0])
            msg = getattr(self, route)(query[1])
            self.input_socket.send(msg)

    def start(self, sample_id) -> bytes:
        """
        Clients try to acquire lock on server.

        """
        if self.lock is False:
            self.k2proc.stdin.write(SENTINEL.format('START'))
            self.starting_sample = True
            self.all_seqs_submitted = False
            self.lock = True  # Starts sending results back
            print(f"Server: got lock for {sample_id}")
            reply = '1'
        else:
            reply = '0'
        return to_bytes(reply)

    def run_batch(self, msg: bytes) -> bytes:
        """
        :param msg: a chunk of sequence data
        """
        seq = msg.decode('UTF-8')
        self.k2proc.stdin.write(seq)
        self.k2proc.stdin.flush()
        return b'Server: Chunk received'

    def stop(self, sample_id: bytes) -> bytes:
        """
        Insert STOP sentinel into kraken2 stdin.
        Flush the buffer with some dummy seqs
        """

        self.all_seqs_submitted = True
        # Is self.all_seqs_submitted guaranteed to be set in the next iteration
        # of the while loop in the return_results thread?
        self.k2proc.stdin.write(SENTINEL.format('END'))
        print('Server: flushing')
        self.k2proc.stdin.write(self.flush_seqs)
        print("Server: All dummy seqs written")
        return to_bytes(f'Server got STOP signal from client for {sample_id}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--ports', nargs='+')
    parser.add_argument('--db')
    parser.add_argument('--threads', default=8)
    parser.add_argument('--k2-binary', default='kraken2')
    args = parser.parse_args()
    Server(args.ports, args.db, args.k2_binary, args.threads)



