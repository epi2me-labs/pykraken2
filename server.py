#! /usr/bin/env python
from typing import List
import argparse
import zmq
from threading import Thread
import subprocess as sub
import datetime

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

SENTINEL_START = (
    f"@START\n"
    f"{'T' * 50}\n"
    "+\n"
    f"{'!' * 50}\n"
)

SENTINEL_END = (
    f"@END\n"
    f"{'T' * 50}\n"
    "+\n"
    f"{'!' * 50}\n"
)

DUMMYSEQ = (
    f"@DUMMY\n"
    f"{'T' * 50}\n"
    "+\n"
    f"{'!' * 50}\n"
)


class Server:
    def __init__(self, ports, kraken_db_dir, threads=24):
        self.kraken_db_dir = kraken_db_dir
        self.context = zmq.Context()

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


    def return_results(self):
        """
        Return the stdout stream of kraken2 results back to the client.
        Loging in here ensures that .

        """
        print('Server: publish thread started')

        self.sample_started = False

        while True:
            stdout = self.k2proc.stdout.read(100000)
            # Get the number of full sequences analysed.
            # Does not count partial result line that may be present at the end

            num_processed = stdout.count('\n')
            self.seqs_to_process -= num_processed

            if not self.sample_started:
                # Iterate over stdout lines until the start sentinel is found
                # Then send proceeding lines to the client.
                lines = stdout.splitlines()
                for i, line in enumerate(lines):
                    if line.startswith('U\tSTART'):
                        print('found START')
                        self.sample_started = True
                    elif self.sample_started:
                        # -1 includes the sentinel for testing
                        first_chunk = '\n'.join(lines[i-1:])

                        self.return_socket.send_multipart(
                            [to_bytes(NOT_DONE), to_bytes(first_chunk)])
                        self.return_socket.recv()

                        # If there were dummy seqs to remove before the sample,
                        # remove from total number of seqs processed
                        self.seqs_to_process += i
                        break

            if self.sample_started and self.seqs_to_process <= 0:
                # The current stdout chunk should contain the stop sentinel
                # Search for it and finish up
                while True:
                    if not self.sample_started:  # Get rid of self
                        break
                    lines = stdout.splitlines()
                    for i, line in enumerate(lines):
                        print('flush', i)
                        if line.startswith('U\tEND'):
                            print('Server: Found termination sentinel')
                            # Include last chunk up to (and including for testing) the s=stop sentinel
                            final_bit = '\n'.join(
                                lines[0: i+1])

                            self.return_socket.send_multipart(
                                [to_bytes(NOT_DONE), to_bytes(final_bit)])
                            self.return_socket.recv()

                            # Client can receive no more messages after the
                            # DONE signal is returned
                            self.return_socket.send_multipart(
                                [to_bytes(DONE), to_bytes(DONE)])
                            self.return_socket.recv()

                            # Release lock and allow other clients to connect
                            self.lock = False
                            self.seqs_to_process = 0
                            self.sample_started = False
                            print('Stop sentinel found')
                            break
                    stdout = self.k2proc.stdout.read(100000)

            if self.sample_started and self.seqs_to_process > 0:
                print('s3', self.seqs_to_process)
                self.return_socket.send_multipart([to_bytes(NOT_DONE), to_bytes(stdout)])
                self.return_socket.recv()

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
            self.k2proc.stdin.write(SENTINEL_START)
            reply = '1'
        else:
            reply = '0'
        return to_bytes(reply)

    def run_batch(self, msg: bytes) -> bytes:
        seq = msg.decode('UTF-8')
        self.k2proc.stdin.write(seq)
        self.k2proc.stdin.flush()
        return b'Server: awaiting more chunks from the client'

    def stop(self, sample_id: bytes) -> bytes:
        # Insert sentinel in order to determine end of sample results
        self.k2proc.stdin.write(SENTINEL_END)
        n_flushseqs = 100000
        print('Server: flushing')
        for i in range(n_flushseqs):  # Can we do this outside of a loop?
            self.k2proc.stdin.write(DUMMYSEQ)
        print("Server: All dummy seqs written")
        return to_bytes(f'Server got STOP signal from client for {sample_id}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--ports', nargs='+')
    parser.add_argument('--db')
    args = parser.parse_args()
    Server(args.ports, args.db)



