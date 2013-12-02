#!/usr/bin/env python

import msgpack
import sys
import numpy as np

from pyon.core.bootstrap import get_obj_registry
from pyon.core.exception import BadRequest
from pyon.core.interceptor.interceptor import Interceptor
from pyon.core.object import IonObjectBase
from pyon.util.containers import get_safe
from pyon.util.log import log

numpy_floats = (np.float, np.float16, np.float32, np.float64)
numpy_ints = (np.int, np.int8, np.int16, np.int32, np.int64, np.uint8, np.uint16, np.uint32, np.uint64)
numpy_bool = (np.bool, )
numpy_complex = (np.complex, np.complex64, np.complex128)


class EncodeTypes(object):
    SET = 's'
    LIST = 'l'
    NPARRAY = 'a'
    COMPLEX = 'c'
    DTYPE = 'd'
    SLICE = 'i'
    NPVAL = 'n'

# Global lazy load reference to the Pyon object registry
obj_registry = None


def decode_ion(obj):
    """msgpack object hook to decode granule (numpy) types and IonObjects"""

    if "type_" in obj:
        global obj_registry
        if obj_registry is None:
            obj_registry = get_obj_registry()

        ion_obj = obj_registry.new(obj["type_"])
        for k, v in obj.iteritems():
            if k != "type_":
                setattr(ion_obj, k, v)
        return ion_obj

    if 't' not in obj:
        return obj

    objt = obj['t']

    if objt == EncodeTypes.LIST:
        return list(obj['o'])

    elif objt == EncodeTypes.NPARRAY:
        return np.array(obj['o'], dtype=np.dtype(obj['d']))

    elif objt == EncodeTypes.COMPLEX:
        return complex(obj['o'][0], obj['o'][1])
    
    elif objt == EncodeTypes.DTYPE:
        return np.dtype(obj['o'])

    elif objt == EncodeTypes.SLICE:
        return slice(obj['o'][0], obj['o'][1], obj['o'][2])

    elif objt == EncodeTypes.SET:
        return set(obj['o'])

    elif objt == EncodeTypes.NPVAL:
        dt = np.dtype(obj['d'])
        return dt.type(obj['o'])

    return obj


def encode_ion(obj):
    """
    msgpack object hook to encode granule/numpy types and IonObjects
    """

    if isinstance(obj, IonObjectBase):
        # There must be a type_ in here
        return obj.__dict__

    if isinstance(obj, list):
        return {'t': EncodeTypes.LIST, 'o': tuple(obj)}

    if isinstance(obj, set):
        return {'t': EncodeTypes.SET, 'o': tuple(obj)}

    if isinstance(obj, np.ndarray):
        return {'t': EncodeTypes.NPARRAY, 'o': obj.tolist(), 'd': obj.dtype.str}

    if isinstance(obj, complex):
        return {'t': EncodeTypes.COMPLEX, 'o': (obj.real, obj.imag)}

    if isinstance(obj, np.number):
        if isinstance(obj, numpy_floats):
            return {'t': EncodeTypes.NPVAL, 'o': float(obj.astype(float)), 'd': obj.dtype.str}
        elif isinstance(obj, numpy_ints):
            return {'t': EncodeTypes.NPVAL, 'o': int(obj.astype(int)), 'd': obj.dtype.str}
        else:
            raise TypeError('Unsupported type "%s"' % type(obj))

    if isinstance(obj, slice):
        return {'t': EncodeTypes.SLICE, 'o': (obj.start, obj.stop, obj.step)}

    if isinstance(obj, np.dtype):
        return {'t': EncodeTypes.DTYPE, 'o': obj.str}

    # Must raise type error for any unknown object
    raise TypeError('Unknown type "%s" in user specified encoder: "%s"' % (type(obj), obj))


class EncodeInterceptor(Interceptor):

    def __init__(self):
        self.max_message_size = sys.maxint  # Will be set appropriately from interceptor config

    def configure(self, config):
        self.max_message_size = get_safe(config, 'max_message_size', 20000000)
        log.debug("EncodeInterceptor enabled")

    def outgoing(self, invocation):
        log.debug("EncodeInterceptor.outgoing: %s", invocation)
        log.debug("Pre-transform: %s", invocation.message)

        # msgpack the content (ensures string)
        invocation.message = msgpack.packb(invocation.message, default=encode_ion)

        # make sure no Nones exist in headers - this indicates a problem somewhere up the stack
        # pika will choke hard on them as well, masking the actual problem, so we catch here.
        nonelist = [(k, v) for k, v in invocation.headers.iteritems() if v is None]
        if nonelist:
            raise BadRequest("Invalid headers containing None values: %s" % str(nonelist))

        msg_size = len(invocation.message)
        log.debug("message size: %s", msg_size)
        if msg_size > self.max_message_size:
            raise BadRequest('The message size %s is larger than the max_message_size value of %s' % (
                msg_size, self.max_message_size))

        return invocation

    def incoming(self, invocation):
        log.debug("EncodeInterceptor.incoming: %s", invocation)

        invocation.message = msgpack.unpackb(invocation.message, object_hook=decode_ion, use_list=1)
        log.debug("Post-transform: %s", invocation.message)

        return invocation
