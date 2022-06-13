"""pykraken2 client module."""

import argparse
import threading
from threading import Thread
import time

import zmq

import pykraken2
from pykraken2 import _log_level, packb, Signals, unpackb


class Client:
    """Client class to stream sequence data to kraken2  server."""

    def __init__(
            self, address='localhost', ports=[5555, 5556]):
        """Init function.

        :param address: server address
        :param ports:  [send data port, receive results port]
        """
        self.context = zmq.Context.instance()
        self.logger = pykraken2.get_named_logger('Client}')
        self.address = address
        self.send_port, self.recv_port = ports
        self.terminate_event = threading.Event()
        self.token = None

    def __enter__(self):
        """Enter context manager."""
        return self

    def __exit__(self, etype, value, traceback):
        """Exit context manager."""
        self.terminate()

    def terminate(self):
        """Terminate the client."""
        self.terminate_event.set()

    def process_fastq(self, fastq):
        """Process a fastq file."""
        send_socket = self.context.socket(zmq.REQ)
        send_socket.connect(f"tcp://{self.address}:{self.send_port}")

        while True:
            # Try to get a unique lock on the server
            # register the number of sequences to expect
            send_socket.send_multipart(
                [packb(Signals.GET_TOKEN)])

            signal, token = send_socket.recv_multipart()
            signal = unpackb(signal)

            if signal == Signals.OK_TO_BEGIN:
                self.token = token
                self.logger.info('Acquired server token')
                # Start thread for receiving input
                break
            elif signal == Signals.WAIT_FOR_TOKEN:
                time.sleep(1)
                self.logger.info('Waiting for lock on server')

        send_thread = Thread(
            target=self._send_worker, args=(fastq, send_socket))
        send_thread.start()

        for chunk in self._receiver():
            yield chunk

        send_thread.join()
        send_socket.close()

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
                        [packb(Signals.RUN_BATCH),
                         self.token, seq.encode('UTF-8')])
                    # It is required to receive with the REQ/REP pattern, even
                    # if the msg is not used
                    socket.recv_multipart()
                else:
                    socket.send_multipart(
                        [packb(Signals.FINISH_TRANSACTION),
                         self.token])
                    socket.recv()
                    self.logger.info('Sending data finished')
                    break

    def _receiver(self):
        """Worker to receive results."""
        socket = self.context.socket(zmq.REP)
        poller = zmq.Poller()
        poller.register(socket, flags=zmq.POLLIN)

        # TODO: If Linger not set, we get stuck somewhere.
        # I think it means there are unsent messages and the socket will not
        # get closed with default -1. With this set it discards unsent
        # messages after 1s. Look into this
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
                msg, token, payload = socket.recv_multipart()
                if token != self.token:
                    raise ValueError(
                        "Client received results with incorrect token")
                status = unpackb(msg)
                socket.send(b'Received')

                result = payload.decode('UTF-8')
                yield result

                if status == Signals.TRANSACTION_COMPLETE:
                    self.logger.info(
                        'Received data processing complete message')
                    break
                elif status == Signals.TRANSACTION_NOT_DONE:
                    continue

        socket.close()


def main(args):
    """Entry point to run a kraken2 client."""
    with Client(args.address, args.ports) as client:
        with open(args.out, 'w') as fh:
            for chunk in client.process_fastq(args.fastq):
                fh.write(chunk)


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
        "--address", default='localhost'
    )
    parser.add_argument(
        "--ports", default=[5555, 5556],
        nargs='+',
        help="")
    parser.add_argument(
        "--out",
        help="")
    return parser
