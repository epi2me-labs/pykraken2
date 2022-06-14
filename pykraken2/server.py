"""pykraken2 server module."""
import argparse
import subprocess
import threading
from threading import Lock, Thread
import time
import uuid

import zmq

import pykraken2
from pykraken2 import _log_level, packb, Signals, unpackb, ZMQ_MSG_SIZE


class Server:
    """Kraken2 server.

    This server runs two threads:

    recv_thread
        receives messages from k2clients, and feeds sentinel-delimited
        sequences to a kraken2 subprocess. After the STOP sentinel,
        dummy sequences are fed into the kraken2 subprocess stdin to flush
        out any remaining sequence results.

    send_thread
        Reads results from the kraken2 subprocess stdout and sends them back
        to the client.

    """

    FAKE_SEQUENCE_LENGTH = 50
    K2_BATCH_SIZE = 20  # number of seqs processed together in kraken2
    START_SENTINEL_NAME = 'START'
    END_SENTINEL_NAME = 'END'

    def __init__(
            self, kraken_db_dir, address='localhost', port=5555,
            k2_binary='kraken2', threads=1):
        """
        Server constructor.

        :param kraken_db_dir: path to kraken2 database directory
        :param address: address of the server:
            e.g. 127.0.0.1 or localhost
        :param port: port for initial connection
        :param k2_binary: path to kraken2 binary
        :param threads: number of threads for kraken2
        """
        self.logger = pykraken2.get_named_logger('Server')
        self.logger.debug(f'k2 binary: {k2_binary}')
        self.context = zmq.Context.instance()
        self.kraken_db_dir = kraken_db_dir

        self.k2_binary = k2_binary
        self.threads = threads
        self.address = address
        self.recv_port = port
        self.send_port = None
        self.recv_thread = None
        self.send_thread = None
        self.k2proc = None
        self.token = None
        self.client_lock = Lock()

        # Have all seqs from current sample been passed to kraken
        self.all_seqs_submitted_event = threading.Event()
        # Are we waiting for processing of a sample to start
        self.start_sample_event = threading.Event()
        self.start_sample_event.set()
        # Signal to the threads to exit
        self.terminate_event = threading.Event()

        self.fake_sequence = (
            "@{}\n"
            f"{'T' * self.FAKE_SEQUENCE_LENGTH}\n"
            "+\n"
            f"{'!' * self.FAKE_SEQUENCE_LENGTH}\n")

        self.flush_seqs = "".join([
            self.fake_sequence.format(f"DUMMY_{x}")
            for x in range(self.K2_BATCH_SIZE)])

    def __enter__(self):
        """Enter context manager."""
        self.run()
        return self

    def __exit__(self, etype, value, traceback):
        """Exit context manager."""
        self.terminate()

    def terminate(self):
        """Wait for processing and threads to terminate and exit."""
        self.logger.debug('Terminate calls, waiting for worker threads.')
        self.terminate_event.set()
        self.recv_thread.join()
        self.send_thread.join()
        self.logger.info('Termination complete.')

    def run(self):
        """Start the server.

        :raises IOError if zmq cannot bind socket.
        """
        self.logger.info('Loading kraken2 database')
        cmd = [
            'stdbuf', '-oL',
            self.k2_binary,
            '--db', self.kraken_db_dir,
            '--threads', str(self.threads),
            '--batch-size', str(self.K2_BATCH_SIZE),
            '/dev/fd/0']

        self.send_port = pykraken2.free_ports(1, lowest=self.recv_port+1)[0]

        self.k2proc = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, universal_newlines=True, bufsize=1)

        self.recv_thread = Thread(target=self.recv)
        self.recv_thread.start()
        self.send_thread = Thread(target=self.send_results)
        self.send_thread.start()
        self.logger.info("Initialisation complete.")

    def send_results(self):
        """Return kraken2 results to clients.

        Poll the kraken process stdout for results.

        Isolates the real results from the dummy results by looking for
        sentinels.
        """
        self.logger.info("Starting send results thread.")
        socket = self.context.socket(zmq.REQ)
        socket.connect(f"tcp://{self.address}:{self.send_port}")

        while not self.terminate_event.is_set():
            if self.client_lock.locked():  # Is a client connected?
                if self.start_sample_event.is_set():
                    # remove any remaining dummy seqs from previous clients
                    while True:
                        line = self.k2proc.stdout.readline()
                        if line.startswith(f'U\t{self.START_SENTINEL_NAME}'):
                            self.start_sample_event.clear()
                            break

                if self.all_seqs_submitted_event.is_set():
                    # get remaining kraken2 output checking for end sentinel
                    # TODO: reevaluate full line-by-line processing
                    self.logger.info('Checking for sentinel')
                    final_lines = []
                    while True:
                        line = self.k2proc.stdout.readline()
                        if line.startswith(f'U\t{self.END_SENTINEL_NAME}'):
                            self.logger.debug('Found termination sentinel')
                            final_bit = "".join(final_lines).encode('UTF-8')
                            socket.send_multipart(
                                [packb(Signals.TRANSACTION_COMPLETE),
                                 self.token, final_bit])
                            socket.recv()
                            break
                        else:
                            final_lines.append(line)
                    # TODO: is this the best place to be releasing?
                    self.logger.info('Releasing lock.')
                    self.client_lock.release()
                    continue

                # TODO: there's a possible race condition here where we may
                #       gobble up the end sentinel unintentionally. See
                #       finish_transaction
                stdout = self.k2proc.stdout.read(ZMQ_MSG_SIZE).encode('UTF-8')
                socket.send_multipart([
                    packb(Signals.TRANSACTION_NOT_DONE),
                    self.token, stdout])
                socket.recv()
            else:  # no client connected
                self.logger.info('Waiting for client.')
                time.sleep(1)
        socket.close()
        self.logger.info('Send results thread finished.')

    def recv(self):
        """Receive signals from client.

        Listens for messages from the input socket and forward them to
        the appropriate functions.
        """
        self.logger.info("Starting API router thread.")
        socket = self.context.socket(zmq.REP)
        try:
            socket.bind(f'tcp://{self.address}:{self.recv_port}')
        except zmq.error.ZMQError as e:
            raise IOError(
                f'Port in use: Try "kill -9 `lsof -i tcp:{self.recv_port}`"') \
                from e

        poller = zmq.Poller()
        poller.register(socket, flags=zmq.POLLIN)
        self.logger.info('Waiting for connections')

        while not self.terminate_event.is_set():
            if poller.poll(timeout=1000):
                query = socket.recv_multipart()
                route = Signals(unpackb(query[0])).name.lower()
                msg = getattr(self, route)(*query[1:])
                socket.send_multipart(msg)
        socket.close()
        self.logger.info("API router thread finished.")

    def get_token(self):
        """Set a token that client and server share.

        :returns: (Signals.OK_TO_BEGIN, token, port) if no client
            is already connected or (Signals.WAIT_FOR_TOKEN, None, None)
            if a client should wait.
        """
        if not self.client_lock.locked():
            self.client_lock.acquire()
            self.token = str(uuid.uuid4()).encode('UTF-8')
            self.k2proc.stdin.write(
                self.fake_sequence.format(self.START_SENTINEL_NAME))
            self.start_sample_event.set()
            self.all_seqs_submitted_event.clear()
            self.logger.info("Got lock")
            reply = [
                packb(Signals.OK_TO_BEGIN), self.token,
                packb(self.send_port)]
        else:
            reply = [
                packb(Signals.WAIT_FOR_TOKEN),
                packb(None), packb(None)]
        return reply

    def run_batch(self, token, data):
        """Process a data chunk.

        :param data: a chunk of sequence data.
        :param token: client-server validation token.
        """
        if token != self.token:
            self.logger.error('run_batch received incorrect token.')
            msg = [None, None]
        else:
            self.k2proc.stdin.write(data.decode('UTF-8'))
            self.k2proc.stdin.flush()
            msg = [packb('Server: Chunk received'), packb(None)]
        return msg

    def finish_transaction(self, token):
        """All data has been sent from a client.

        Insert STOP sentinel into kraken2 stdin.
        Flush the buffer with some dummy seqs.
        """
        if token != self.token:
            self.logger.error(
                'finish transaction received incorrect token.')
            msg = [None, None]
        else:
            # TODO: see note on race condition in send_results. We
            #       need to wait here until that thread is ready to
            #       catch the end sentinel.
            self.all_seqs_submitted_event.set()
            self.k2proc.stdin.write(
                self.fake_sequence.format(self.END_SENTINEL_NAME))
            self.logger.info('flushing')
            self.k2proc.stdin.write(self.flush_seqs)
            self.logger.info("All dummy seqs written")
            msg = [packb("Transaction finished"), packb(None)]
        return msg


def main(args):
    """Entry point to run a kraken2 server."""
    with Server(
            args.database, args.address, args.port,
            args.k2_binary, args.threads):
        while True:
            pass


def argparser():
    """Argument parser for entrypoint."""
    parser = argparse.ArgumentParser(
        "kraken2 server",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[_log_level()], add_help=False)
    parser.add_argument(
        'database',
        help="kraken2 database directory.")
    parser.add_argument(
        "--address", default='localhost',
        help="location on which to listen for clients.")
    parser.add_argument(
        '--port', default=5555,
        help="port on which to listen for clients.")
    parser.add_argument(
        '--threads', default=8,
        help="kraken2 compute threads.")
    parser.add_argument(
        '--k2-binary', default='kraken2',
        help="location of kraken2 binary.")
    return parser
