#!/usr/bin/env python

"""
Entry point for importing common Ion packages. Most files should only need to import from here.
"""

__author__ = 'Adam R. Smith'
__license__ = 'Apache 2.0'

__all__ = []

# Tell the magic import log setup to pass through this file
from pyon.util.log import import_paths
import_paths.append(__name__)

from pyon.util.log import log
__all__ += ['log']

from pyon.core.bootstrap import CFG, SERVICE_CFG, obj_registry, IonObject
__all__ += ['CFG', 'SERVICE_CFG', 'obj_registry', 'IonObject']

from pyon.util.async import spawn, switch
__all__ += ['spawn', 'switch']

from pyon.core.process import IonProcessError, GreenProcess, GreenProcessSupervisor, PythonProcess
__all__ += ['IonProcessError', 'GreenProcess', 'GreenProcessSupervisor', 'PythonProcess']

from pyon.net import messaging, channel
__all__ += ['messaging', 'channel']

#from pyon.container.cc import Container
#__all__ += ['Container']
