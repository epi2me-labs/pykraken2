"""
K2client.py

Main thread acquires lock from the server. Read chunks of sequence data and
feed this to the server.

recv_thread receives results back from the server, and listens for a DONE
message before terminating.
"""

import argparse
import zmq
from threading import Thread
import time
import subprocess as sub

from pykraken2.server import START, STOP, RUN_BATCH, DONE
from pykraken2.server import to_bytes as b
from pykraken2.server import to_string as s


def receive_results(port, outfile, sample_id):
    """Worker to receive results."""
    context = zmq.Context()
    socket = context.socket(zmq.REP)

    while True:
        try:
            socket.bind(f'tcp://127.0.0.1:{port}')
        except zmq.error.ZMQError as e:
            print(f'Port in use?: Try "kill -9 `lsof -i tcp:{port}`"')
            print(e)
        else:
            break
    print(f"f{sample_id}: receive_results thread listening")

    with open(outfile, 'w') as fh:
        while True:
            status, result = socket.recv_multipart()
            socket.send(b'Recevied')
            if s(status) == DONE:
                print('Client: Received data processing complete message from '
                      'server')
                return
            fh.write(result.decode('UTF-8'))
            fh.flush()


def run_client(ports, fastq: str, outpath: str, sample_id: str):
    """Create and run a client."""

    send_port, recv_port = ports
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(f"tcp://127.0.0.1:{send_port}")

    with open(fastq, 'r') as fh:
        while True:
            # Try to get a unique lock on the server
            # register the number of sequences to expect
            socket.send_multipart([b(START), b(sample_id)])

            lock = int(socket.recv())

            if lock:
                print(f'Client: Acquired lock on server: {sample_id}')

                # Start thread for receiving input
                recv_thread = Thread(target=receive_results,
                                     args=(ports[1], outpath, sample_id))
                recv_thread.start()
                break
            else:
                time.sleep(1)
                print(f'Client: Waiting to get lock on server for: {sample_id}')

        while True:
            # There was a suggestion to send all the reads from a sample
            # as a single message. But this would require reading the whole
            # fastq file into memory first.
            seq = fh.read(100000) # Increase?

            if seq:
                socket.send_multipart([b(RUN_BATCH), b(seq)])
                # It is required to receive with the REQ/REP pattern, even
                # if the msg is not used
                socket.recv()
            else:
                socket.send_multipart([b(STOP), b(sample_id)])
                socket.recv()
                print('Client: sending finished')
                break


def main(args):
    """Entry point to run a kraken2 client."""
    run_client(args.ports, args.fastq, args.out, args.sample_id)


def argparser():
    """Argument parser for entrypoint."""
    parser = argparse.ArgumentParser(
        "kraken2 client",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        add_help=False)
    # TODO: server port should be required argument, the second
    # port isn't a concern for the user.
    parser.add_argument("--ports", nargs='+')
    parser.add_argument("--fastq")
    parser.add_argument("--out")
    parser.add_argument("--sample_id")
    return parser
