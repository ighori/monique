import uuid
import datetime
import sys
import json

from mqe.util import run_once, datetime_to_timestamp, datetime_from_timestamp, datetime_from_date


# Patch json encoding method to indent nested lists less aggressively.
# Requires pypy.

if getattr(sys, 'pypy_version_info', None):
    if hasattr(json.JSONEncoder, '_JSONEncoder__encode_list'):
        def __encode_list_mqe(self, l, markers, builder, _current_indent_level):
            self._JSONEncoder__mark_markers(markers, l)
            builder.append('[')
            first = True
            to_indent = isinstance(l[0], (list, tuple))
            if to_indent:
                separator, _current_indent_level = self._JSONEncoder__emit_indent(builder,
                                                                                  _current_indent_level)
            for elem in l:
                if first:
                    first = False
                else:
                    if to_indent:
                        builder.append(separator)
                    else:
                        builder.append(self.item_separator)
                self._JSONEncoder__encode(elem, markers, builder, _current_indent_level)
                del elem # XXX grumble
            if to_indent:
                self._JSONEncoder__emit_unindent(builder, _current_indent_level)
            builder.append(']')
            self._JSONEncoder__remove_markers(markers, l)
        json.JSONEncoder._JSONEncoder__encode_list = __encode_list_mqe


_TYPE_NAME_TO_CLASS = {}

@run_once
def _init_lib_classes():
    from mqetables import enrichment
    register_json_type(enrichment.EnrichedTable, 'ET')

    from mqe import dataseries

def _type_name_to_class(type_name):
    _init_lib_classes()
    return _TYPE_NAME_TO_CLASS.get(type_name)

def json_type(type_name):
    """A class decorator that registers the class as supporting JSON serialization (see
    :ref:`guide_serialization`). The ``type_name`` is put under ``__type__`` key of the JSON object
    representing the class' instance.
    """
    def decorator(cls):
        register_json_type(cls, type_name)
        return cls
    return decorator

def register_json_type(cls, type_name):
    cls._json_type = type_name
    _TYPE_NAME_TO_CLASS[type_name] = cls


def encoder_default(obj):
    if hasattr(obj, 'for_json'):
        res = obj.for_json()
        if isinstance(res, dict) and '__type__' not in res:
            res['__type__'] = obj._json_type
        return res
    if isinstance(obj, datetime.date):
        if not isinstance(obj, datetime.datetime):
            obj = datetime_from_date(obj)
        return {'__type__': 'date', 'arg': datetime_to_timestamp(obj)*1000}
    if isinstance(obj, uuid.UUID):
        return {'__type__': 'UUID', 'arg': obj.hex}
    raise TypeError('Not JSON-serializable: type %s object %r' % (type(obj), obj))

def external_encoder_default(obj):
    if hasattr(obj, 'for_external_json'):
        return obj.for_external_json()
    if isinstance(obj, datetime.date):
        return obj.isoformat()
    if isinstance(obj, uuid.UUID):
        return obj.hex
    raise TypeError('Not JSON-serializable: type %s object %r' % (type(obj), obj))


class MqeJSONEncoder(json.JSONEncoder):
    """A :class:`json.JSONEncoder` subclass that supports the library's serialization"""

    def default(self, obj):
        res = encoder_default(obj)
        if res is not None:
            return res
        return json.JSONEncoder.default(self, obj)


def decoder_object_hook(obj):
    custom_type = obj.get('__type__')
    if custom_type is not None:
        cls = _type_name_to_class(custom_type)
        if cls is not None and hasattr(cls, 'from_rawjson'):
            del obj['__type__']
            return cls.from_rawjson(obj)
        if custom_type == 'UUID':
            return uuid.UUID(obj['arg'])
        if custom_type == 'date':
            return datetime_from_timestamp(obj['arg'] / 1000)

    return obj


class MqeJSONDecoder(json.JSONDecoder):
    """A :class:`json.JSONDecoder` subclass that supports the library's serialization"""

    def __init__(self, *args, **kwargs):
        if 'object_hook' not in kwargs:
            kwargs['object_hook'] = decoder_object_hook
        super(MqeJSONDecoder, self).__init__(*args, **kwargs)


def json_dumps(obj, indent=2):
    """Serialize ``obj`` to a string"""
    return json.dumps(obj, default=encoder_default, indent=indent)

def json_dumps_sorted(obj, indent=2):
    """Sort keys of ``obj`` and serialize it to a string"""
    return json.dumps(obj, default=encoder_default, indent=indent, sort_keys=True)

def json_dumps_external(obj, indent=2):
    """Serialize ``obj`` to a string, but drop the library's support of ``__type__`` keys and use a
    simplified representation:

    * encode UUIDs as hex strings
    * encode datetimes by calling :meth:`datetime.datetime.isoformat`
    * encode custom classes by calling ``for_external_json``
    """
    return json.dumps(obj, default=external_encoder_default, indent=indent)

def mjson(obj):
    """Serialize ``obj`` to a string and use a minimal representation (no unneeded whitespaces
    and newlines)"""
    return json.dumps(obj, default=encoder_default, indent=None, separators=(',', ':'))

def mjson_external(obj):
    """The same as :func:`mjson`, but use a format described for :func:`json_dumps_external`"""
    return json.dumps(obj, default=external_encoder_default, indent=None, separators=(',', ':'))

def json_loads(s):
    """Deserialize a JSON document contained in the string ``s``"""
    return json.loads(s, object_hook=decoder_object_hook)

