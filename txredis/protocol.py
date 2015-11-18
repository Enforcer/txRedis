"""
@file protocol.py

@mainpage

txRedis is an asynchronous, Twisted, version of redis.py (included in the
redis server source).

The official Redis Command Reference:
http://code.google.com/p/redis/wiki/CommandReference

@section An example demonstrating how to use the client in your code:
@code
from twisted.internet import reactor
from twisted.internet import protocol
from twisted.internet import defer

from txredis.protocol import Redis

@defer.inlineCallbacks
def main():
    clientCreator = protocol.ClientCreator(reactor, Redis)
    redis = yield clientCreator.connectTCP(HOST, PORT)

    res = yield redis.ping()
    print res

    res = yield redis.set('test', 42)
    print res

    test = yield redis.get('test')
    print res

@endcode

Redis google code project: http://code.google.com/p/redis/
Command doc strings taken from the CommandReference wiki page.

"""
from collections import deque

from twisted.internet import defer, protocol
from twisted.protocols import policies

from txredis import exceptions
from txredis._compat import b, imap, long, basestring, unicode


SYM_STAR = b('*')
SYM_DOLLAR = b('$')
SYM_CRLF = b('\r\n')
SYM_EMPTY = b('')


class Token(object):
    """
    Literal strings in Redis commands, such as the command names and any
    hard-coded arguments are wrapped in this class so we know not to apply
    and encoding rules on them.
    """
    def __init__(self, value):
        if isinstance(value, Token):
            value = value.value
        self.value = value

    def __repr__(self):
        return self.value

    def __str__(self):
        return self.value


class RedisBase(protocol.Protocol, policies.TimeoutMixin, object):
    """The main Redis client."""

    ERROR = b('-')
    SINGLE_LINE = b('+')
    INTEGER = b(':')
    BULK = b('$')
    MULTI_BULK = b('*')

    def __init__(self, db=None, password=None, charset='utf8', errors='strict'):
        self.charset = charset
        self.db = db if db is not None else 0
        self.password = password
        self.errors = errors
        self._buffer = b('')
        self._bulk_length = None
        self._disconnected = False
        # Format of _multi_bulk_stack elements is:
        # [[length-remaining, [replies] | None]]
        self._multi_bulk_stack = deque()
        self._request_queue = deque()

    def dataReceived(self, data):
        """Receive data.

        Spec: http://redis.io/topics/protocol
        """
        self.resetTimeout()
        self._buffer = self._buffer + data

        while self._buffer:

            # if we're expecting bulk data, read that many bytes
            if self._bulk_length is not None:
                # wait until there's enough data in the buffer
                # we add 2 to _bulk_length to account for \r\n
                if len(self._buffer) < self._bulk_length + 2:
                    return
                data = self._buffer[:self._bulk_length].decode(self.charset, self.errors)
                self._buffer = self._buffer[self._bulk_length + 2:]
                self.bulkDataReceived(data)
                continue

            # wait until we have a line
            if SYM_CRLF not in self._buffer:
                return

            # grab a line
            line, self._buffer = self._buffer.split(SYM_CRLF, 1)
            if len(line) == 0:
                continue

            # first byte indicates reply type
            reply_type = line[:1]
            reply_data = line[1:].decode(self.charset, self.errors)

            # Error message (-)
            if reply_type == self.ERROR:
                self.errorReceived(reply_data)
            # Integer number (:)
            elif reply_type == self.INTEGER:
                self.integerReceived(reply_data)
            # Single line (+)
            elif reply_type == self.SINGLE_LINE:
                self.singleLineReceived(reply_data)
            # Bulk data (&)
            elif reply_type == self.BULK:
                try:
                    self._bulk_length = int(reply_data)
                except ValueError:
                    r = exceptions.InvalidResponse(
                        "Cannot convert data '%s' to integer" % reply_data)
                    self.responseReceived(r)
                    return
                # requested value may not exist
                if self._bulk_length == -1:
                    self.bulkDataReceived(None)
            # Multi-bulk data (*)
            elif reply_type == self.MULTI_BULK:
                # reply_data will contain the # of bulks we're about to get
                try:
                    multi_bulk_length = int(reply_data)
                except ValueError:
                    r = exceptions.InvalidResponse(
                        "Cannot convert data '%s' to integer" % reply_data)
                    self.responseReceived(r)
                    return
                if multi_bulk_length == -1:
                    self._multi_bulk_stack.append([-1, None])
                    self.multiBulkDataReceived()
                    return
                else:
                    self._multi_bulk_stack.append([multi_bulk_length, []])
                    if multi_bulk_length == 0:
                        self.multiBulkDataReceived()

    def failRequests(self, reason):
        while self._request_queue:
            d = self._request_queue.popleft()
            d.errback(reason)

    def connectionMade(self):
        """ Called when incoming connections is made to the server. """
        d = defer.succeed(True)

        # if we have a password set, make sure we auth
        if self.password:
            d.addCallback(lambda _res: self.auth(self.password))

        # select the db passsed in
        if self.db:
            d.addCallback(lambda _res: self.select(self.db))

        def done_connecting(_res):
            # set our state as soon as we're properly connected
            self._disconnected = False
        d.addCallback(done_connecting)

        return d

    def connectionLost(self, reason):
        """Called when the connection is lost.

        Will fail all pending requests.

        """
        self._disconnected = True
        self.failRequests(reason)

    def timeoutConnection(self):
        """Called when the connection times out.

        Will fail all pending requests with a TimeoutError.

        """
        self.failRequests(defer.TimeoutError("Connection timeout"))
        self.transport.loseConnection()

    def errorReceived(self, data):
        """Error response received."""
        if data[:4] == 'ERR ':
            reply = exceptions.ResponseError(data[4:])
        elif data[:9] == 'NOSCRIPT ':
            reply = exceptions.NoScript(data[9:])
        elif data[:8] == 'NOTBUSY ':
            reply = exceptions.NotBusy(data[8:])
        else:
            reply = exceptions.ResponseError(data)

        if self._request_queue:
            # properly errback this reply
            self._request_queue.popleft().errback(reply)
        else:
            # we should have a request queue. if not, just raise this exception
            raise reply

    def singleLineReceived(self, data):
        """Single line response received."""
        if data == 'none':
            # should this happen here in the client?
            reply = None
        else:
            reply = data

        self.responseReceived(reply)

    def handleMultiBulkElement(self, element):
        top = self._multi_bulk_stack[-1]
        top[1].append(element)
        top[0] -= 1
        if top[0] == 0:
            self.multiBulkDataReceived()

    def integerReceived(self, data):
        """Integer response received."""
        try:
            reply = int(data)
        except ValueError:
            reply = exceptions.InvalidResponse(
                "Cannot convert data '%s' to integer" % data)
        if self._multi_bulk_stack:
            self.handleMultiBulkElement(reply)
            return

        self.responseReceived(reply)

    def bulkDataReceived(self, data):
        """Bulk data response received."""
        self._bulk_length = None
        self.responseReceived(data)

    def multiBulkDataReceived(self):
        """Multi bulk response received.

        The bulks making up this response have been collected in
        the last entry in self._multi_bulk_stack.

        """
        reply = self._multi_bulk_stack.pop()[1]
        if self._multi_bulk_stack:
            self.handleMultiBulkElement(reply)
        else:
            self.handleCompleteMultiBulkData(reply)

    def handleCompleteMultiBulkData(self, reply):
        self.responseReceived(reply)

    def responseReceived(self, reply):
        """Handle a server response.

        If we're waiting for multibulk elements, store this reply. Otherwise
        provide the reply to the waiting request.

        """
        if self._multi_bulk_stack:
            self.handleMultiBulkElement(reply)
        elif self._request_queue:
            self._request_queue.popleft().callback(reply)

    def getResponse(self):
        """
        @retval a deferred which will fire with response from server.
        """
        if self._disconnected:
            return defer.fail(RuntimeError("Not connected"))

        d = defer.Deferred()
        self._request_queue.append(d)
        return d

    def encode(self, value):
        "Ported from redis-py"
        "Return a bytestring representation of the value"

        if isinstance(value, Token):
            return b(value.value)
        elif isinstance(value, bytes):
            return value
        elif isinstance(value, (int, long)):
            value = b(str(value))
        elif isinstance(value, float):
            value = b(repr(value))
        elif not isinstance(value, basestring):
            value = unicode(value)
        if isinstance(value, unicode):
            value = value.encode(self.charset, self.errors)
        return value

    def _pack(self, *args):
        output = []
        # the client might have included 1 or more literal arguments in
        # the command name, e.g., 'CONFIG GET'. The Redis server expects these
        # arguments to be sent separately, so split the first argument
        # manually. All of these arguements get wrapped in the Token class
        # to prevent them from being encoded.
        command = args[0]
        if ' ' in command:
            args = tuple([Token(s) for s in command.split(' ')]) + args[1:]
        else:
            args = (Token(command),) + args[1:]

        buff = SYM_EMPTY.join(
            (SYM_STAR, b(str(len(args))), SYM_CRLF))

        for arg in imap(self.encode, args):
            # to avoid large string mallocs, chunk the command into the
            # output list if we're sending large values
            if len(buff) > 6000 or len(arg) > 6000:
                buff = SYM_EMPTY.join(
                    (buff, SYM_DOLLAR, b(str(len(arg))), SYM_CRLF))
                output.append(buff)
                output.append(arg)
                buff = SYM_CRLF
            else:
                buff = SYM_EMPTY.join((buff, SYM_DOLLAR, b(str(len(arg))),
                                       SYM_CRLF, arg, SYM_CRLF))
        output.append(buff)
        return output

    def _send(self, *args):
        """Encode and send a request

        Uses the 'unified request protocol' (aka multi-bulk)

        """
        command = self._pack(*args)
        for part in command:
            self.transport.write(part)

    def send(self, command, *args):
        self._send(command, *args)
        return self.getResponse()


class HiRedisBase(RedisBase):
    """A subclass of the RedisBase protocol that uses the hiredis library for
    parsing.
    """

    def dataReceived(self, data):
        """Receive data.
        """
        self.resetTimeout()
        if data:
            self._reader.feed(data)
        res = self._reader.gets()
        while res is not False:
            if isinstance(res, exceptions.ResponseError):
                self._request_queue.popleft().errback(res)
            else:
                if isinstance(res, basestring) and res == 'none':
                    res = None
                self._request_queue.popleft().callback(res)
            res = self._reader.gets()
