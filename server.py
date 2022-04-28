#! /usr/bin/env python
from typing import List
import argparse
import zmq
from threading import Thread
import subprocess as sub
import datetime

# Sever query signals
START = 'start'
STOP = 'stop'
RUN_BATCH = 'run_batch'

FULL_OUTPUT = 'all_kraken2_output'
CLASSIFIED_READS = 'kraken2.classified.fastq'
UNCLASSIFIED_READS = 'kraken2.unclassified.fastq'


def to_bytes(string) -> bytes:
    return string.encode('UTF-8')


def to_string(bytes_) -> str:
    return bytes_.decode('UTF-8')


SENTINEL = (
    f"@SENTINEL\n"
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
    def __init__(self, ports, kraken_db_dir):
        self.kraken_db_dir = kraken_db_dir
        self.context = zmq.Context()

        cmd = [
            'kraken2',
            '--report', 'kraken2_report.txt',
            '--classified-out', CLASSIFIED_READS,
            '--unclassified-out', UNCLASSIFIED_READS,
            '--db', self.kraken_db_dir,
            '/dev/fd/0'
        ]

        self.k2proc = sub.Popen(
            cmd, stdin=sub.PIPE, stdout=sub.PIPE, stderr=sub.PIPE,
            universal_newlines=True)

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

        self.recv_thread = Thread(target=self.recv)
        self.recv_thread.start()

        self.return_thread = Thread(target=self.return_results)
        self.return_thread.start()

    def return_results(self):
        """
        Return the stdout stream of kraken2 results back to the client.
        Keep a track of how many output lines we have processed by counting
        newlines.

        """
        print('Server: publish thread started')
        while True:
            line = self.k2proc.stdout.readline()

            if line.startswith('U	DUMMY'):
                continue
            elif line.startswith('U	SENTINEL'):
                print('Sever: Terminating connection')
                self.return_socket.send_multipart(
                    [b'DONE', b'done'])
                self.return_socket.recv()
                self.lock = False
                continue

            else:
                self.return_socket.send_multipart([b'NOTDONE', to_bytes(line)])
                self.return_socket.recv()

    def recv(self):
        """
        Listens for connections and route queries
        """
        while True:
            #  0 in the route frame
            # Message part is remaining frames and can vary in length
            query = self.input_socket.recv_multipart()
            route = to_string(query[0])
            msg = getattr(self, route)(query[1:])
            self.input_socket.send(msg)

    def start(self, seq_id: List) -> bytes:
        # Client asking for lock on server.
        sid = to_string(seq_id[0])
        if self.lock is False:
            print(f'Sever: Processing {sid}')
            self.lock = True
            reply = '1'
        else:
            reply = '0'
        return to_bytes(reply)

    def run_batch(self, msg: List) -> bytes:
        # Can we change this to listen on another socket that does not need to
        # reply after each seq input?
        seq = msg[0].decode('UTF-8')
        status = msg[1].decode('UTF-8')

        if status == 'NOTDONE':
            self.k2proc.stdin.write(seq)
            self.k2proc.stdin.flush()
            return b'Server: awaiting more chunks from the client'
        else:
            print('Last batch received from client')
            self.flush()
            return b'Server: Final chunk received. Tidying up ...'

    def flush(self):
        print('flush')
        self.k2proc.stdin.write(SENTINEL)      # This is what we will look for
        for f in range(100000):
            # print(f) # This is now the problem. Why doe sit not fo to the end
            self.k2proc.stdin.write(DUMMYSEQ)  # This is to force flush
            # self.k2proc.stdin.write('\n')
        print('final dummy', f)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--ports', nargs='+')
    parser.add_argument('--db')
    args = parser.parse_args()
    Server(args.ports, args.db)



