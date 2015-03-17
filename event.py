# encoding: utf-8
from collections import namedtuple
import struct


class EventType(object):
    FETCH = 0
    CREATE = 1
    DELETE = 2
    MODIFY = 3
    MOVE = 4

    fields = {
        FETCH: [('source_path', 's')],
        CREATE: [('source_path', 's'), ('is_directory', '?')],
        DELETE: [('source_path', 's'), ('is_directory', '?')],
        MODIFY: [('source_path', 's'), ('size', 'I'), ('hash', 's'), ('modified_date', 'd')],
        MOVE: [('source_path', 's'), ('destination_path', 's'), ('is_directory', '?')]
    }

    header = "!BB"
    header_size = struct.calcsize(header)

    pack_format = {event_type: "".join([field_type if field_type != 's' else 'H'
                                        for field_name, field_type in fields_])
                   for event_type, fields_ in list(fields.items())}

    pack_size = {event_type: struct.calcsize("!" + pack_format_[event_type])
                 for event_type, pack_format_ in
                 zip(list(fields.keys()), [pack_format] * len(fields))}

    type = {id: namedtuple("Event%s" % id, ['type', 'sync_point'] + [field_name for field_name, field_type in fields_])
            for id, fields_ in list(fields.items())}

    @staticmethod
    def serialize(evt):
        pack = [evt.type, len(evt.sync_point)]
        strings = [evt.sync_point]
        for field_name, field_type in EventType.fields[evt.type]:
            if field_type != 's':
                pack.append(getattr(evt, field_name))
            else:
                string = getattr(evt, field_name)
                pack.append(len(string))
                strings.append(string)
        return struct.pack(EventType.header + EventType.pack_format[evt.type], *pack) + b"".join([string.encode("utf-8") for string in strings])

    @staticmethod
    def deserialize(msg):
        pos = EventType.header_size
        event_type, sync_point_len = struct.unpack(EventType.header, msg[:pos])

        unpacked = struct.unpack("!" + EventType.pack_format[event_type],
                                 msg[pos:pos + EventType.pack_size[event_type]])
        pos += EventType.pack_size[event_type]

        sync_point = msg[pos:pos + sync_point_len].decode('utf-8')
        pos += sync_point_len

        fields = []
        for idx, field in enumerate(unpacked):
            field_name, field_type = EventType.fields[event_type][idx]
            if field_type != 's':
                fields.append(field)
            else:
                fields.append(msg[pos:pos + field].decode('utf-8'))
                pos += field
        return EventType.type[event_type](event_type, sync_point, *fields)

    @staticmethod
    def create(type, sync_point, *args, **kwargs):
        return EventType.type[type](type, sync_point, *args, **kwargs)