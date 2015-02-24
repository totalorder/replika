# encoding: utf-8
import asyncio
import asyncio.base_events
import asyncio.tasks
from test import testutils


class TestAsyncIO:
    def setup_method(self, method):
        self.loop = testutils.TestLoop()
        self.result = []
        asyncio.set_event_loop(self.loop)

    def teardown_method(self, method):
        pass

    def test_run_until_no_events(self):
        @asyncio.coroutine
        def coro2():
            self.result.append("async ")
            yield
            return "?"

        @asyncio.coroutine
        def coro3():
            self.result.append("World")

        def call_at_callback():
            self.result.append("!")

        @asyncio.coroutine
        def coro1():
            self.result.append("Hello ")
            asyncio.async(coro3())
            coro2_result = yield from coro2()
            self.result.append(coro2_result)

        self.loop.call_at(self.loop.time() + 0.1, call_at_callback)
        asyncio.async(coro1())

        # Execution order:
        #   schedule call_at_callback in the future
        #   >>> coro1: Hello
        #   schedule coro3
        #   run coro2
        #   >>> coro2: async
        #   yield to coro3
        #   >>> coro3: World
        #   * resume coro2, returning result to coro1
        #   >>> coro1: ?
        #   * execute scheduled call_at_callback
        #   >>> call_at_callback: !

        self.loop.run_until_no_events()

        assert "".join(self.result) == "Hello async World?!"

    def test_run_until_no_events_network(self):
        @asyncio.coroutine
        def client_connected_handler(client_reader, client_writer):
            self.result.append("Connection accepted")

        server_task = asyncio.async(asyncio.start_server(client_connected_handler, 'localhost', 2222))

        @asyncio.coroutine
        def connect_to_server():
            reader, writer = yield from asyncio.open_connection('localhost', 2222)
            self.result.append("Client connected")
            writer.transport.close()
            server_task.result().close()
            return

        asyncio.async(connect_to_server())
        self.loop.run_until_no_events()
        assert self.result == ["Connection accepted", "Client connected"]
