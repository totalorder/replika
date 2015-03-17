# encoding: utf-8
from event import EventType


class TestEvent:
    def setup_method(self, method):
        pass

    def teardown_method(self, method):
        pass

    def test_serialize_event(self):
        evt = EventType.create(EventType.FETCH, "sync_point", "source_path")
        assert EventType.serialize(evt) == \
            b"\x00\n\x00\x0bsync_pointsource_path"

    def test_deserialize_event(self):
        evt = EventType.deserialize(b"\x00\n\x00\x0bsync_pointsource_path")
        assert evt.type == EventType.FETCH
        assert evt.sync_point == "sync_point"
        assert evt.source_path == "source_path"

    def test_serialize_and_deserialize_event(self):
        evt = EventType.create(EventType.FETCH, "sync_point", "source_path")
        serialized_event = EventType.serialize(evt)
        deserialized_event = EventType.deserialize(serialized_event)
        assert evt.type == deserialized_event.type
        assert evt.sync_point == deserialized_event.sync_point
        assert evt.source_path == deserialized_event.source_path
