#! /usr/bin/env python
import queue

import time
from typing import List
import re
import argparse
from collections import defaultdict
import zmq
from pathlib import Path
from threading import Thread
import subprocess as sub

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
    f"{'T'}\n"
    "+\n"
    f"{'!'}\n"
)

DUMMYSEQ = (
    f"@DUMMY\n"
    f"{'T'}\n"
    "+\n"
    f"{'!'}\n"
)


class Server:
    def __init__(self, ports, kraken_db_dir):
        self.kraken_db_dir = kraken_db_dir
        self.context = zmq.Context()
        self.control_socket = self.context.socket(zmq.REP)
        self.control_socket.bind(f'tcp://127.0.0.1:{ports[0]}')

        # Note: In the PUB/SUB pattern, if the publisher has started
        # publishing before the subscriber connects, the previous messages
        # will be lost
        self.data_socket = self.context.socket(zmq.REQ)
        self.data_socket.connect(f"tcp://127.0.0.1:5556")

        cmd = [
            'kraken2',
            '--report', 'kraken2_report.txt',
            '--classified-out', CLASSIFIED_READS,
            '--unclassified-out', UNCLASSIFIED_READS,
            '--db', self.kraken_db_dir,
            '/dev/fd/0'
        ]

        self.k2proc = sub.Popen(
            cmd, stdin=sub.PIPE, stdout=sub.PIPE,
            universal_newlines=True)

        self.recv_thread = Thread(target=self.recv)
        self.recv_thread.start()

        self.publish_thread = Thread(target=self.publish_results)
        self.publish_thread.start()

        print('Server started')

    def publish_results(self):
        """
        Return the stdout stream of kraken2 results back to the client.
        Keep a track of how many output lines we have processed by counting
        newlines.

        """
        print('publish thread started')
        x = 0
        while True:
            # What is a sensible n for read() here?
            # Are there message size limits? Does ZMQ handle this for us?

            line = self.k2proc.stdout.readline()
            x = x + 1

            if line.startswith('U	DUMMY'):
                if self.processing:  # Still accepting data
                    continue

            if line.startswith('U	SENTINEL'):
                print('Terminating connection')
                self.data_socket.send_multipart(
                    [b'DONE', b'done'])
                self.data_socket.recv()
                continue

            else:
                self.data_socket.send_multipart([b'NOTDONE', to_bytes(line)])
                self.data_socket.recv()

    def recv(self):
        """
        Listens for connections and route queries
        """
        while True:
            #  0 in the route frame
            # Message part is remaining frames and can vary in length
            query = self.control_socket.recv_multipart()
            route = to_string(query[0])
            msg = getattr(self, route)(query[1:])
            self.control_socket.send(msg)

    def stop(self, msg: List):
        """
        Once a stop signal has been received from the client, no more sequences
        should be processed until all sequences from the current sample have
        been processed and the results returned to the client.

        sample_size passed in as part of message contains the number of seqs in
        this sample determined by the client

        self.seqs_processed is a counter of lines output by kraken

        do periodic checks to see if all output has been done before accepting
        new connections

        :param sample_id:
        :return:
        """
        return b'test'
        sample_id, sample_size = msg
        sample_size = int(sample_size)
        while True:
            # print(f'Sample_size: {sample_size}, Seqs processed: {self.seqs_processed}')
            if self.seqs_processed >= sample_size:
                print('All seqs processed')
                self.seqs_processed = 0
                return 'Stop processing {}'.format(sample_id).encode('UTF-8')
            time.sleep(0.5)

    def start(self, seq_id: List) -> bytes:
        # Currently, doesn't do much
        print(f'Now doing seq for {seq_id[0]}')
        self.processing = True
        # Create a filelock here?
        return to_bytes(f'starting {seq_id[0]}')

    def run_batch(self, msg: List) -> bytes:
        # Can we change this to listen on another socket that does not need to
        # reply after each seq input?
        seq = msg[0].decode('UTF-8')
        status = msg[1].decode('UTF-8')

        if status == 'NOTDONE':
            self.k2proc.stdin.write(seq)
            self.k2proc.stdin.flush()
            print('Server: awaiting more chunks from the client')
            return b'Server: awaiting more chunks from the client'
        else:
            print('whooo')
            self.flush()
            return b'Server: Final chunk received. Tidying up ...'

    def flush(self):
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



