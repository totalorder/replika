# encoding: utf-8
import Queue
from mock import Mock
import async


class FakeRunner(async.EventThread):
    def __init__(self, runs):
        super(FakeRunner, self).__init__()
        self.runs = runs
        self.steps = 0

    def step(self):
        self.steps += 1
        self.runs -= 1
        if self.runs <= 0:
            return True

class TestLoop:
    def test_run_until_done_single(self):
        loop = async.Loop()
        loop.runners = Mock(spec=Queue.Queue, wraps=loop.runners)
        fake_runner_1 = FakeRunner(1)
        loop.add_runner(fake_runner_1)
        loop.run_until_done()
        assert fake_runner_1.steps == 2
        assert loop.runners.get.call_count == 2

    def test_run_until_done_two(self):
        loop = async.Loop()
        loop.runners = Mock(spec=Queue.Queue, wraps=loop.runners)
        fake_runner_1 = FakeRunner(1)
        fake_runner_2 = FakeRunner(2)
        loop.add_runner(fake_runner_1)
        loop.add_runner(fake_runner_2)
        loop.run_until_done()
        assert fake_runner_1.steps == 3
        assert fake_runner_2.steps == 2
        assert loop.runners.get.call_count == 5
