#! /usr/bin/env python

import time
import argparse
from typing import List
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

SENTINEL_SIZE = 5

SENTINEL = (
    "@{}\n"
    f"{'T' * SENTINEL_SIZE}\n"
    "+\n"
    f"{'!' * SENTINEL_SIZE}\n"
)

DUMMYSEQ = (
    f"@DUMMY\n"
    f"{'T' * SENTINEL_SIZE}\n"
    "+\n"
    f"{'!' * SENTINEL_SIZE}\n"
)


class Server:
    """
    Sever

    This server runs two threads:
    Two threads:

        recv_thread
        receives messages from k2clients, and feeds sentinel-delimited
        sequences to a kraken2 subprocess. After the STOP sentinel,
        dummy sequences are fed into the kraken2 subprocess stdin to flush
        out any remaining sequence results.

        return_thread
        Reads results from the kraken2 subprocess stdout and sends them back
        to the clinet.

    """
    def __init__(self, ports, kraken_db_dir, threads=8):
        self.kraken_db_dir = kraken_db_dir
        self.context = zmq.Context()

        self.flush_seqs = "".join([DUMMYSEQ] * 100000)

        cmd = [
            'kraken2',
            '--report', 'kraken2_report.txt',
            '--classified-out', CLASSIFIED_READS,
            '--unclassified-out', UNCLASSIFIED_READS,
            '--db', self.kraken_db_dir,
            '--threads', str(threads),
            '/dev/fd/0'
        ]

        self.k2proc = sub.Popen(
            cmd, stdin=sub.PIPE, stdout=sub.PIPE, stderr=sub.PIPE,
            universal_newlines=True, bufsize=0)

        # Wait for database loading before binding to input socket
        print('Loading kraken2 database')
        start = datetime.datetime.now()

        while True:
            err_line = self.k2proc.stderr.readline()
            if 'done' in err_line:
                print('Database loaded. Binding to input socket')
                break

        end = datetime.datetime.now()
        delta = end - start
        print(f'Kraken database loading duration: {delta}')

        self.input_socket = self.context.socket(zmq.REP)
        self.input_socket.bind(f'tcp://127.0.0.1:{ports[0]}')

        self.return_socket = self.context.socket(zmq.REQ)
        self.return_socket.connect(f"tcp://127.0.0.1:5556")

        self.lock = False
        self.seqs_to_process = 0

        self.recv_thread = Thread(target=self.recv)
        self.recv_thread.start()

        self.return_thread = Thread(target=self.return_results)
        self.return_thread.start()

    def do_first_chunk(self,) -> int:
        """
        Search for START sentinel. If processing the servers first client,
        then this will be the first line in the kraken2 stdout.
        If processing subsequent client connections, then there will likely
        be dummy sequences in the stdout that we need to remove first.

        :return: number of sequences from first chunk passed to kraken2 stdin
        """
        while True:
            stdout = self.k2proc.stdout.read(100000)
            lines = stdout.splitlines()

            for i, line in enumerate(lines):
                if line.startswith('U\tSTART'):
                    print('found START')
                    # i -1 includes the sentinel for testing
                    if i < 0:
                        i = 0
                    first_chunk = '\n'.join(lines[i:])
                    first_chunk_size = len(lines) - i
                    print(f'First chunk size {first_chunk_size}')

                    self.return_socket.send_multipart(
                        [to_bytes(NOT_DONE), to_bytes(first_chunk)])
                    self.return_socket.recv()
                    return first_chunk_size

    def do_final_chunk(self, result_lines: List):
        """
        The number of seqs to process should now be <= 0 meaning that the
        STOP sentinel should be in the current chunk kraken2 data
        (result lines). Search for this and send all data before it to client
        along with a DONE message

        :param result_lines: A chunk of kraken2 results lines
        """
        first = True
        while True:
            if first:
                stdout = result_lines
                first = False
            else:
                stdout = self.k2proc.stdout.read(100000)
            lines = stdout.splitlines()

            for i, line in enumerate(lines):
                if line.startswith('U\tEND'):
                    print('Server: Found termination sentinel')
                    # Include last chunk up to (and including for testing) the stop sentinel
                    final_bit = '\n'.join(
                        lines[0: i + 1])

                    self.return_socket.send_multipart(
                        [to_bytes(NOT_DONE), to_bytes(final_bit)])
                    self.return_socket.recv()

                    # Todo: DONE and NOT_DONE can both send sequences

                    # Client can receive no more messages after the
                    # DONE signal is returned
                    self.return_socket.send_multipart(
                        [to_bytes(DONE), to_bytes(DONE)])
                    self.return_socket.recv()

                    print('Stop sentinel found')
                    return

    def return_results(self):
        """
        Return kraken2 results to clients.

        Poll the kraken process stdout for results.
        Isolates the real results from the dummy results by looking for
        sentinels
        """
        first_chunk_to_do = True

        while True:
            if self.lock:  # Is a client connected?

                if first_chunk_to_do:
                    num_seqs = self.do_first_chunk()
                    first_chunk_to_do = False
                    self.seqs_to_process -= num_seqs

                stdout = self.k2proc.stdout.read(100000)
                num_result_lines = stdout.count('\n')
                self.seqs_to_process -= num_result_lines

                if self.seqs_to_process > 0:
                    self.return_socket.send_multipart(
                        [to_bytes(NOT_DONE), to_bytes(stdout)])
                    self.return_socket.recv()

                if self.seqs_to_process <= 0:
                    self.do_final_chunk(stdout)
                    self.seqs_to_process = 0
                    first_chunk_to_do = True
                    print('locking')
                    self.lock = False
            else:
                print('S waiting for lock', self.seqs_to_process)
                time.sleep(1)

    def recv(self):
        """
        Listens for messages from the input socket and forward them to
        the appropriate functions.
        """
        print('Server: Waiting for connections')
        while True:
            query = self.input_socket.recv_multipart()
            route = to_string(query[0])
            msg = getattr(self, route)(query[1])
            self.input_socket.send(msg)

    def start(self, num_seqs: bytes) -> bytes:
        """
        Clients try to acquire lock on server.
        If successful, register the number of sequences to expect
        """
        if self.lock is False:
            self.lock = True
            self.seqs_to_process = int(to_string(num_seqs))
            self.k2proc.stdin.write(SENTINEL.format('START'))
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

        self.k2proc.stdin.write(SENTINEL.format('END'))
        print('Server: flushing')
        self.k2proc.stdin.write(self.flush_seqs)
        print("Server: All dummy seqs written")
        return to_bytes(f'Server got STOP signal from client for {sample_id}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--ports', nargs='+')
    parser.add_argument('--db')
    args = parser.parse_args()
    Server(args.ports, args.db)



