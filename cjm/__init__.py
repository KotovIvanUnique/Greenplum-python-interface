from __future__ import (absolute_import)

__docformat__ = 'restructuredtext'

from ._version import __version__
__version__ = __version__

# module level doc-string
__doc__ = """
cjm - библиотека для клиентской аналитики и разработки сценариев продаж команды CJM
"""

# from . import core
# from . import util
from .core import *
from .util import *