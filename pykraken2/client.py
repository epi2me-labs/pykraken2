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

from pykraken2 import _log_level
from pykraken2.server import KrakenSignals
from pykraken2.server import to_bytes as b


class Client:

    def __init__(self, address, ports, outpath: str, sample_id: str):
        """Create and run a client."""

        self.address = address
        self.send_port, self.recv_port = ports
        self.sample_id = sample_id
        self.outpath = outpath
        self.context = zmq.Context()

    def process_fastq(self, fastq):

        send_socket = self.context.socket(zmq.REQ)
        # TODO: should be arbitrary server
        send_socket.connect(f"tcp://{self.address}:{self.send_port}")

        while True:
            # Try to get a unique lock on the server
            # register the number of sequences to expect
            send_socket.send_multipart(
                [KrakenSignals.START.value, b(self.sample_id)])

            lock = int(send_socket.recv())

            if lock:
                print(f'Client: Acquired lock on server: {self.sample_id}')

                # Start thread for receiving input
                # TODO: we shouldn't write results to file but yield to caller
                # OK, will do once tests working
                break
            else:
                time.sleep(1)
                print(f'Client: Waiting to get lock on server for:'
                      f'{self.sample_id}')

        send_thread = Thread(
            target=self._send_worker, args=(fastq, send_socket))
        send_thread.start()

        for chunk in self._receiver():
            yield chunk

    def _send_worker(self, fastq, socket):

        with open(fastq, 'r') as fh:
            while True:
                # There was a suggestion to send all the reads from a sample
                # as a single message. But this would require reading the whole
                # fastq file into memory first.
                # TODO: better to send a fixed number of lines, even one record
                #       at a time?
                # NEil: I thought sending single records at a time would slow
                # things down, but I will test this.
                seq = fh.read(100000)  # Increase?

                if seq:
                    socket.send_multipart(
                        [KrakenSignals.RUN_BATCH.value, b(seq)])
                    # It is required to receive with the REQ/REP pattern, even
                    # if the msg is not used
                    socket.recv()
                else:
                    socket.send_multipart(
                        [KrakenSignals.STOP.value, b(self.sample_id)])
                    socket.recv()
                    print('Client: sending finished')
                    socket.close()
                    break

    def _receiver(self):
        """Worker to receive results."""
        context = self.context
        socket = context.socket(zmq.REP)

        while True:
            try:
                # TODO: should be an arbitrary server
                socket.bind(f'tcp://{self.address}:{self.recv_port}')
            except zmq.error.ZMQError as e:
                print(f'Client: Port in use?: '
                      f'Try "kill -9 `lsof -i tcp:{self.recv_port}`"')
                print(e)
            else:
                break
            time.sleep(1)
        print(f"{self.sample_id}: receive_results thread listening")

        while True:
            status, result = socket.recv_multipart()
            socket.send(b'Recevied')
            if status == KrakenSignals.DONE.value:
                print('Client: '
                      'Received data processing complete message')
                socket.close()
                context.term()
                return
            yield result.decode('UTF-8')


def main(args):
    """Entry point to run a kraken2 client."""
    client = Client(args.ports, args.out, args.sample_id)
    client.process_fastq(args.fastq)


def argparser():
    """Argument parser for entrypoint."""
    parser = argparse.ArgumentParser(
        "kraken2 client",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[_log_level()], add_help=False)
    # TODO: server port should be required argument, the second
    # port isn't a concern for the user.
    parser.add_argument(
        "fastq")
    parser.add_argument(
        "--ports", default=[5556, 5561],
        nargs='+',
        help="")
    parser.add_argument(
        "--out",
        help="")
    parser.add_argument(
        "--sample_id", default="no_sample",
        help="")
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    main(argparser())
