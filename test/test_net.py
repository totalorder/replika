# encoding: utf-8
from unittest.mock import Mock, ANY, patch
import pytest
import net
import asyncio
import asyncio.base_events
import asyncio.tasks
from test import testutils
from test.testutils import create_future_result as cfr
from test.testutils import get_future_result as gfr


def create_start_server_side_effect(loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()
    # loop is shadowed in the inner function
    outer_loop = loop
    @asyncio.coroutine
    def start_server_side_effect(client_connected_cb, host=None, port=None, *,
                                 loop=None, limit=None, **kwds):
        def client_connected_event():
            stream_writer_mock = Mock(spec=asyncio.streams.StreamWriter)
            stream_writer_mock.transport.get_extra_info.return_value = ('127.0.0.1', 8001)
            client_connected_cb(Mock(spec=asyncio.streams.StreamReader),
                                stream_writer_mock)
        outer_loop.call_soon(client_connected_event)
        return Mock(spec=asyncio.base_events.Server)
    return start_server_side_effect


def create_open_connection_side_effect(loop, reader_mock=None, fail=False):
    # loop is shadowed in the inner function
    outer_loop = loop
    # reader_mock cannot be written to from the inner function scope
    # read outer_reader_mock and write to shadowed reader_mock
    outer_reader_mock = reader_mock
    @asyncio.coroutine
    def open_connection_side_effect(host=None, port=None, *,
                                 loop=None, limit=None, **kwds):
        if fail:
            raise ConnectionRefusedError()

        writer_mock = Mock(spec=asyncio.streams.StreamWriter)
        writer_mock.transport.get_extra_info.return_value = (host, port)

        if outer_reader_mock is None:
            reader_mock = Mock(spec=asyncio.streams.StreamReader)
        else:
            reader_mock = outer_reader_mock
        return cfr((reader_mock, writer_mock))
    return open_connection_side_effect


class TestNetwork:
    def setup_method(self, method):
        self.loop = testutils.TestLoop()
        asyncio.set_event_loop(self.loop)

        self.asyncio_start_server_patcher = patch(
            'asyncio.start_server',
            Mock(spec=asyncio.start_server)
        )

        self.asyncio_start_server_mock = \
            self.asyncio_start_server_patcher.start()

        self.asyncio_start_server_mock.side_effect = \
            create_start_server_side_effect(self.loop)

        self.asyncio_open_connection_patcher = patch(
            'asyncio.open_connection',
            Mock(spec=asyncio.open_connection)
        )

        self.asyncio_open_connection_mock = \
            self.asyncio_open_connection_patcher.start()

        self.network = net.Network(self.client_accepted_cb)
        self._client_accepted_cb = lambda *args, **kwargs: None

    def set_client_accepted_cb(self, cb):
        self._client_accepted_cb = cb

    def client_accepted_cb(self, *args, **kwargs):
        self._client_accepted_cb(*args, **kwargs)

    def teardown_method(self, method):
        self.asyncio_start_server_patcher.stop()
        self.asyncio_open_connection_patcher.stop()
        self.loop.close()

    def test_client_accepted_cb_called_on_listen(self):
        self.asyncio_start_server_mock.side_effect = create_start_server_side_effect()

        self.called = []
        self.set_client_accepted_cb(lambda *args, **kwargs: self.called.append((args, kwargs)))
        self.network.listen(("127.0.0.1", 8000))
        self.loop.run_until_no_events()
        assert len(self.called) == 1
        args, kwargs = self.called[0]
        assert len(args) == 1
        client = args[0]
        assert client.address == ("127.0.0.1", 8001)

    def test_recv_data(self):
        reader_mock = Mock(spec=asyncio.streams.StreamReader)
        self.asyncio_open_connection_mock.side_effect = create_open_connection_side_effect(self.loop, reader_mock)
        reader_mock.read.return_value = cfr(b"Hello bytes!")
        client = gfr(self.network.connect(("127.0.0.1", 8000)))
        assert gfr(client.recv()) == b"Hello bytes!"

    def test_connect(self):
        self.asyncio_open_connection_mock.side_effect = create_open_connection_side_effect(self.loop)
        client_fut = self.network.connect(("127.0.0.1", 8001))
        self.loop.run_until_no_events()
        assert client_fut.done()
        assert client_fut.result().address == ("127.0.0.1", 8001)

    def test_failed_connect(self):
        self.asyncio_open_connection_mock.side_effect = create_open_connection_side_effect(self.loop, fail=True)
        client_fut = self.network.connect(("127.0.0.1", 8001))
        self.loop.run_until_no_events(raise_exceptions=False)
        with pytest.raises(ConnectionError) as excinfo:
            client_fut.result()


class TestClient:
    def setup_method(self, method):
        self.loop = testutils.TestLoop()
        asyncio.set_event_loop(self.loop)
        sender_writer_transport_mock = Mock(spec=asyncio.transports.WriteTransport)
        self.sender = net.Client(("127.0.0.1", 8000),
                                 asyncio.streams.StreamReader(),
                                 asyncio.streams.StreamWriter(
                                     sender_writer_transport_mock, None, None, None),
                                 True)

        receiver_stream_reader = asyncio.streams.StreamReader()
        self.receiver = net.Client(("127.0.0.1", 8001),
                                   receiver_stream_reader,
                                   asyncio.streams.StreamWriter(
                                       Mock(spec=asyncio.transports.WriteTransport), None, None, None),
                                   False)

        sender_writer_transport_mock.write.side_effect = lambda data: receiver_stream_reader.feed_data(data)

    def teardown_method(self, method):
        self.loop.close()

    def test_recvbytes(self):
        self.receiver.reader.feed_data(b"Hello bytes!")
        assert gfr(self.receiver.recvbytes(5)) == b"Hello"
        assert gfr(self.receiver.recvbytes(1)) == b" "
        assert gfr(self.receiver.recvbytes(3)) == b"byt"
        assert gfr(self.receiver.recvbytes(3)) == b"es!"

    def test_transfer_data(self):
        self.sender.senddata("I?", 123456, True)
        assert gfr(self.receiver.recvdata("I?")) == (123456, True)

    def test_transfer_message(self):
        self.sender.sendmessage(b"Hello bytes!")
        assert gfr(self.receiver.recvmessage()) == b"Hello bytes!"

    def test_recvbytes_async(self):
        bytes_fut = self.receiver.recvbytes(12)
        [self.receiver.reader.feed_data(bytes(c, 'utf-8')) for c in "Hello by"]
        self.receiver.reader.feed_data(b"tes!")

        assert gfr(bytes_fut) == b"Hello bytes!"

    def test_transfer_message_async(self):
        message_fut = self.receiver.recvmessage()
        self.loop._run_once()  # TODO: Find nicer way to do this
        assert not message_fut.done()
        self.sender.sendmessage(b"Hello bytes!")
        self.loop.run_until_no_events()
        assert message_fut.result() == b"Hello bytes!"

    def test_conversation_with_delay(self):
        @asyncio.coroutine
        def receiver():
            message = yield from self.receiver.recvmessage()
            return message

        @asyncio.coroutine
        def sender():
            return self.sender.sendmessage(b"Hello!")

        receiver_fut = asyncio.async(receiver())
        asyncio.async(sender())
        self.loop.run_until_no_events()

        assert receiver_fut.result() == b"Hello!"


class TestIntegration:
    def setup_method(self, method):
        self.loop = testutils.TestLoop()
        asyncio.set_event_loop(self.loop)

        self.sending_network = net.Network(None)
        self.receiving_network = net.Network(self.receiving_client_accepted_cb)
        self.receiving_client = None

    def teardown_method(self, method):
        self.receiving_network.stop()
        self.sending_network.stop()
        self.loop.stop()

    def receiving_client_accepted_cb(self, client):
        self.receiving_client = client

    def test_establish_connection(self):
        listen_fut = self.receiving_network.listen(8000)
        self.loop.run_until_complete(listen_fut, raise_exceptions=False)
        listen_fut.result()

        sending_client_fut = self.sending_network.connect(("127.0.0.1", 8000))
        self.loop.run_until_complete(sending_client_fut)
        assert sending_client_fut.done()
        assert sending_client_fut.result().address == ("127.0.0.1", 8000)
        assert self.receiving_client.address == ("127.0.0.1", ANY)

    def test_send_and_receive_data(self):
        listen_fut = self.receiving_network.listen(8000)
        self.loop.run_until_complete(listen_fut, raise_exceptions=False)
        listen_fut.result()

        sending_client_fut = self.sending_network.connect(("127.0.0.1", 8000))
        self.loop.run_until_complete(sending_client_fut)
        assert sending_client_fut.done()
        sending_client = sending_client_fut.result()
        assert sending_client.address == ("127.0.0.1", 8000)
        assert self.receiving_client.address == ("127.0.0.1", ANY)

        sending_client.send(b"Hello bytes!")
        receiving_recv_fut = self.receiving_client.recv()
        self.loop.run_until_complete(receiving_recv_fut)
        assert receiving_recv_fut.result() == b"Hello bytes!"

        self.receiving_client.send(b"Hello! I got your message!")

        sending_client_recv_fut = sending_client.recv()
        self.loop.run_until_complete(sending_client_recv_fut)
        assert sending_client_recv_fut.result() == b"Hello! I got your message!"