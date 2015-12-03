import sys

if sys.version_info[0] < 3:
    def b(x):
        return x
    from itertools import imap
    long = long
    basestring = basestring
    unicode = unicode
    iterkeys = lambda d: d.iterkeys()
    itervalues = lambda d: d.itervalues()
else:
    def b(x):
        return x.encode('latin-1') if not isinstance(x, bytes) else x
    imap = map
    long = int
    basestring = str
    unicode = str
    iterkeys = lambda d: d.keys()
    itervalues = lambda d: d.values()
