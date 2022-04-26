#! /usr/bin/env python
import queue

import time

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


class Server:
    def __init__(self, ports, kraken_db_dir):
        self.kraken_db_dir = kraken_db_dir
        self.context = zmq.Context()
        self.control_socket = self.context.socket(zmq.REP)
        self.control_socket.bind(f'tcp://127.0.0.1:{ports[0]}')
        self.count = 0

        # Note: In the PUB/SUB pattern, if the publisher has started
        # publishing before the subscriber connects, the previous messages
        # will be lost
        self.data_socket = self.context.socket(zmq.PUB)
        self.data_socket.bind(f"tcp://127.0.0.1:5556")
        time.sleep(1)

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
        Return the stream of kraken2 results back to the client
        The k2 process buffers until it has 458752 bytes to write
        ( On my Mac at least). We need a way to get it to flush out the
        remaining output from a sample.

        """
        while True:
            # What is a sensible n for read() here?
            # Are there message size limits?
            line = self.k2proc.stdout.read(10000)
            if not line:
                time.sleep(0.5)
                continue
            self.data_socket.send(to_bytes(line))

    def recv(self):
        """
        Listens for connections and routes queries
        """
        while True:
            query = self.control_socket.recv_multipart()
            route = to_string(query[0])
            msg = getattr(self, route)(query[1])
            self.control_socket.send(msg)

    def stop(self, sample_id):
        """
        Once a stop signal has been received from the client, no more sequences
        should be processed until all sequences from the current sample have
        been processed and the results returned to the client.

        How to do this?
        Sleeping for a few seconds for the flushing to do its job could work,
        but this would not guarantee that  
        :param sample_id:
        :return:
        """
        return 'Stop processing {sample_id}'.format(sample_id).encode('UTF-8')

    def start(self, seq_id) -> bytes:
        # Currently doesn't do much
        print(f'Now doing seq for {seq_id}')
        # Create a filelock here?
        return to_bytes(f'starting {seq_id}')

    def run_batch(self, seq: bytes) -> bytes:
        # Can we change this to listen on another socket that does not need to
        # reply after each seq input?
        try:
            count = self.count
            self.count += 1
            self.k2proc.stdin.write(seq.decode('UTF-8'))
            self.k2proc.stdin.flush()
        except Exception:
            print('oops')
        return b'done'

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--ports', nargs='+')
    parser.add_argument('--db')
    args = parser.parse_args()
    Server(args.ports, args.db)



