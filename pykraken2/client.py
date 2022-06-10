"""pykraken2 client module."""

import argparse
import threading
from threading import Thread
import time

from msgpack import packb, unpackb
import zmq

import pykraken2
from pykraken2 import _log_level
from pykraken2.server import KrakenSignals


class Client:
    """Client class to stream sequence data to kraken2  server."""

    def __init__(
            self,  sample_id: str, address='localhost', ports=[5555, 5556]):
        """Init function.

        :param address: server address
        :param ports:  [send data port, receive results port]
        :param sample_id:
        """
        self.logger = pykraken2.get_named_logger(f'Client-{sample_id}')
        self.address = address
        self.send_port, self.recv_port = ports
        self.sample_id = sample_id
        self.context = zmq.Context()
        self.terminate_event = threading.Event()

    def process_fastq(self, fastq):
        """Process a fastq file."""
        send_context = zmq.Context()
        send_socket = send_context.socket(zmq.REQ)
        send_socket.connect(f"tcp://{self.address}:{self.send_port}")

        while True:
            # Try to get a unique lock on the server
            # register the number of sequences to expect
            send_socket.send_multipart(
                [packb(KrakenSignals.START_SAMPLE.value),
                 self.sample_id.encode('UTF-8')])

            lock = unpackb(send_socket.recv())

            if lock:
                self.logger.info('Acquired lock on server')
                # Start thread for receiving input
                break
            else:
                time.sleep(1)
                self.logger.info('Waiting for lock on server')

        send_thread = Thread(
            target=self._send_worker, args=(fastq, send_socket))
        send_thread.start()

        for chunk in self._receiver():
            yield chunk

        send_thread.join()

        send_socket.close()
        send_context.term()

    def _send_worker(self, fastq, socket):

        with open(fastq, 'r') as fh:
            while not self.terminate_event.is_set():
                # There was a suggestion to send all the reads from a sample
                # as a single message. But this would require reading the whole
                # fastq file into memory first.
                # TODO: better to send a fixed number of lines, even one record
                #       at a time?

                seq = fh.read(100000)

                if seq:
                    socket.send_multipart(
                        [packb(KrakenSignals.RUN_BATCH.value),
                         seq.encode('UTF-8')])
                    # It is required to receive with the REQ/REP pattern, even
                    # if the msg is not used
                    socket.recv()
                else:
                    socket.send_multipart(
                        [packb(KrakenSignals.SAMPLE_FINISHED.value),
                         self.sample_id.encode('UTF-8')])
                    socket.recv()
                    self.logger.info('Sending data finished')
                    socket.close()
                    break

    def _receiver(self):
        """Worker to receive results."""
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        poller = zmq.Poller()
        poller.register(socket, flags=zmq.POLLIN)

        # TODO: If Linger not set, we get stuck somewhere.
        # I think it means there are unsent messages and the socket will not
        # get closed with default -1. With this setm it discards unsent
        # messages after 1s. Fix this
        socket.setsockopt(zmq.LINGER, 1)

        while not self.terminate_event.is_set():
            try:
                socket.bind(f'tcp://{self.address}:{self.recv_port}')
            except zmq.error.ZMQError as e:
                self.logger.warn(
                    f'Client: Port in use?: '
                    f'Try "kill -9 `lsof -i tcp:{self.recv_port}`"')
                self.logger.exception(e)
            else:
                break
            time.sleep(1)
        self.logger.info("_receiver listening")

        while not self.terminate_event.is_set():
            if poller.poll(timeout=1000):
                status, result = socket.recv_multipart()
                socket.send(b'Received')
                if unpackb(status) == KrakenSignals.DONE.value:
                    self.logger.info(
                        'Received data processing complete message')
                    break
                else:
                    yield result.decode('UTF-8')

        socket.close()
        context.term()

    def terminate(self):
        """Terminate the client."""
        self.terminate_event.set()


def main(args):
    """Entry point to run a kraken2 client."""
    client = Client(args.sample_id, args.ports, args.out)
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
        "--ports", default=[5555, 5556],
        nargs='+',
        help="")
    parser.add_argument(
        "--out",
        help="")
    parser.add_argument(
        "--sample_id", default="no_sample",
        help="")
    return parser
