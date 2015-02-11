# encoding: utf-8
from mock import Mock
import signals


def get_spy_processor():
    processor = signals.Processor(async=True)
    return Mock(spec=signals.Processor, wraps=processor)

