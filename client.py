#! /usr/bin/env python

import argparse
from pathlib import Path
import zmq
from typing import List
import time
import sys
from fastx import Fastx
from threading import Thread

from server import START, STOP, RUN_BATCH
from server import to_bytes as b
from server import to_string as s

fakeseq = (
    f"@FAKESEQ_\n"
    f"{'T' * 100}\n"
    "+\n"
    f"{'!' * 100}\n"
)

def main():
    ...


def receive_results(port, outfile):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect(f'tcp://127.0.0.1:{port}')
    socket.setsockopt_string(zmq.SUBSCRIBE, '')
    print("C.receive_results: starting")

    with open(outfile, 'w') as fh:
        while True:
            msg = socket.recv()
            fh.write(msg.decode('UTF-8'))
            fh.flush()


def run_query(ports, query: List, outpath: str, sample_id: str):
    """
    :param port:
    :param query:
        ['RUN_BATCH'], 'path/to/reads.fq']
        ['STOP',]
    :return:
    """
    context = zmq.Context()

    #  Socket to talk to server
    print("Connecting to service")
    socket = context.socket(zmq.REQ)

    # Start thread for receiving input
    th = Thread(target=receive_results, args=(ports[1], outpath))
    th.start()

    socket.connect(f"tcp://127.0.0.1:5555")

    if query[0] == STOP:
        socket.send_multipart([b(STOP), b(sample_id)])
        print("Sending request %s …" % query[0])

    else:
        with open(query[1], 'rb') as fh:
            # print(i)
            while True:
                seq = fh.read(1000000)
                if seq:
                    socket.send_multipart([b(RUN_BATCH), seq])
                    # It is required to receive with the REQ/REP pattern, even
                    # if the msg is not used
                    socket.recv()
                else:
                    # The output from kraken is buffered at ~ 1064 lines
                    # But can be up to 1084.
                    # This bodge sends in 6000 seqs to ensure all the real
                    # outputs are flushed. More fake seqs are needed as the
                    # output of UNCLASSIFIED reads takes up less space.
                    # I anticipate a better solution
                    # is possible. But at least works for now

                    for f in range(6000):
                        socket.send_multipart([b(RUN_BATCH), b(fakeseq)])
                        socket.recv()
                    break
            socket.send_multipart([b(STOP), b(sample_id)])
        # Removed the fakeseq flushers
        # with open()

    # socket.send_multipart([b(STOP), b(sample_id)])  # This would normally be a separate client instance





if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--ports", nargs='+')
    parser.add_argument("--query", nargs='+')
    parser.add_argument("--out")
    parser.add_argument("--sample_id")

    args = parser.parse_args()
    run_query(args.ports, args.query, args.out, args.sample_id)

