#! /usr/bin/env python

import argparse
import zmq
from threading import Thread
import time
import subprocess as sub

from server import START, STOP, RUN_BATCH
from server import to_bytes as b
from server import to_string as s


def receive_results(port, outfile):
    context = zmq.Context()
    socket = context.socket(zmq.REP)

    while True:
        try:
            socket.bind(f'tcp://127.0.0.1:{port}')
        except zmq.error.ZMQError as e:
            'waiting for return socket'
        else:
            break
    print("C.receive_results: starting")

    with open(outfile, 'w') as fh:
        while True:
            status, result = socket.recv_multipart()
            socket.send(b'Recevied')
            status = s(status)
            if status == 'DONE':
                print('Client: Received data processing complete message from '
                      'server')
                return
            fh.write(result.decode('UTF-8'))
            fh.flush()


def main(ports, fastq: str, outpath: str, sample_id: str):

    send_port, recv_port = ports
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(f"tcp://127.0.0.1:{send_port}")

    numseqs = sub.run(
        f"seqkit stats -T {fastq}|cut -f 4|sed -n 2p"
        , capture_output=True, shell=True, text=True).stdout

    print(f'{sample_id}, numseqs: {numseqs}')

    with open(fastq, 'r') as fh:
        while True:
            # Try to get a unique lock on the server
            # Could do this is another control socket.
            # register the number of sequences to expect
            socket.send_multipart([b(START), b(numseqs)])

            lock = int(socket.recv())

            if lock:
                print(f'Acquired lock on server: {sample_id}')

                # Start thread for receiving input
                recv_thread = Thread(target=receive_results,
                                     args=(ports[1], outpath))
                recv_thread.start()
                break
            else:
                time.sleep(3)
                print(f'Waiting to get lock on server for:  {sample_id}')

        while True:
            # There wsa a suggestion to send all the reads from a sample
            # as a single message. But this would require reading the whole
            # fastq file into memory first
            seq = fh.read(10000000)

            if seq:
                socket.send_multipart([b(RUN_BATCH), b(str(seq))])
                # It is required to receive with the REQ/REP pattern, even
                # if the msg is not used
                socket.recv()
            else:
                socket.send_multipart([b(STOP), b(sample_id)])
                socket.recv()
                print('Client sending finished')
                break


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--ports", nargs='+')
    parser.add_argument("--fastq")
    parser.add_argument("--out")
    parser.add_argument("--sample_id")

    args = parser.parse_args()
    main(args.ports, args.fastq, args.out, args.sample_id)

