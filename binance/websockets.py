import asyncio
import json
import logging
import time
from functools import partial
from random import random
import websockets as ws

from .client import Client


class ReconnectingWebsocket:
    MAX_RECONNECTS = 1000
    MAX_RECONNECT_SECONDS = 1
    MIN_RECONNECT_WAIT = 0.1
    TIMEOUT = 2

    def __init__(self, loop, ws_domain, path, coro, prefix='ws/'):
        self._loop = loop
        self._log = logging.getLogger(__name__)
        self._path = path
        self._coro = coro
        self._prefix = prefix
        self._reconnects = 0
        self._conn = None
        self._socket = None
        self.ws_domain = ws_domain

        self._connect()

    def _connect(self):
        self._conn = asyncio.ensure_future(self._run(), loop=self._loop)

    async def _run(self):
        self._log.debug('running ws')
        keep_waiting = True

        ws_url = self.ws_domain + self._prefix + self._path
        async with ws.connect(ws_url) as socket:
            self._socket = socket
            self._reconnects = 0

            try:
                while keep_waiting:
                    try:
                        evt = await asyncio.wait_for(self._socket.recv(), timeout=self.TIMEOUT)
                    except asyncio.TimeoutError:
                        self._log.debug("no message in {} seconds".format(self.TIMEOUT))
                        await self.send_ping()
                    except asyncio.CancelledError:
                        self._log.debug("cancelled error")
                        await self.send_ping()
                    else:
                        try:
                            evt_obj = json.loads(evt)
                        except ValueError:
                            self._log.debug('error parsing evt json:{}'.format(evt))
                        else:
                            asyncio.run_coroutine_threadsafe(self._coro(evt_obj), self._loop)
            except ws.ConnectionClosed as e:
                self._log.debug('ws connection closed:{}'.format(e))
                # await self._reconnect()
                self._reconnect_sync()
            except Exception as e:
                self._log.debug('ws exception:{}'.format(e))
                # await self._reconnect()
                self._reconnect_sync()

    def _get_reconnect_wait(self, attempts: int) -> int:
        expo = 2 ** attempts
        return round(random() * min(self.MAX_RECONNECT_SECONDS, expo - 1) + 1)

    async def _reconnect(self):
        await self.cancel()
        self._reconnects += 1
        if self._reconnects < self.MAX_RECONNECTS:

            self._log.debug("websocket {} reconnecting {} reconnects left".format(
                self._path, self.MAX_RECONNECTS - self._reconnects)
            )
            reconnect_wait = self._get_reconnect_wait(self._reconnects)
            await asyncio.sleep(reconnect_wait)
            self._connect()
        else:
            self._log.error('Max reconnections {} reached:'.format(self.MAX_RECONNECTS))

    def _reconnect_sync(self):
        self._log.debug('cancelling ws connection and reconnecting')
        if self._conn:
            if not self._conn.cancelled():
                self._conn.cancel()
            else:
                self._conn = None
        self._socket = None
        # self._log.debug('cancelled')
        self._reconnects += 1
        if self._reconnects < self.MAX_RECONNECTS:

            self._log.debug("websocket {} reconnecting {} reconnects left".format(
                self._path, self.MAX_RECONNECTS - self._reconnects)
            )
            time.sleep(self.MIN_RECONNECT_WAIT)
            self._connect()
        else:
            self._log.error('Max reconnections {} reached:'.format(self.MAX_RECONNECTS))

    async def send_ping(self):
        if self._socket:
            await self._socket.ping()

    async def cancel(self):
        self._conn.cancel()
        self._socket = None


class BinanceSocketManager:
    STREAM_URL = 'wss://stream.binance.com:9443/'
    FSTREAM_URL = 'wss://fstream.binance.com/'
    DSTREAM_URL = 'wss://dstream.binance.com/'

    WEBSOCKET_DEPTH_5 = '5'
    WEBSOCKET_DEPTH_10 = '10'
    WEBSOCKET_DEPTH_20 = '20'

    DEFAULT_USER_TIMEOUT = 30 * 60  # 30 minutes

    def __init__(self, client, loop, user_timeout=DEFAULT_USER_TIMEOUT):
        """Initialise the BinanceSocketManager

        :param client: Binance API client
        :type client: binance.Client

        """
        self._conns = {}
        self._client = client
        self._loop = loop
        self._log = logging.getLogger(__name__)
        self._user_timeout = user_timeout
        self._timers = {'user': None, 'margin': None}
        self._listen_keys = {'user': None, 'margin': None}
        self._account_callbacks = {'user': None, 'margin': None}

    async def _start_socket(self, path, coro, prefix='ws/'):
        if path in self._conns:
            return False

        self._conns[path] = ReconnectingWebsocket(self._loop, self.STREAM_URL, path, coro, prefix)

        return path

    async def _start_futures_socket(self, path, coro, prefix='ws/'):
        if path in self._conns:
            return False

        # factory_url = self.FSTREAM_URL + prefix + path
        # factory = BinanceClientFactory(factory_url)
        # factory.protocol = BinanceClientProtocol
        # factory.callback = callback
        # factory.reconnect = True
        # context_factory = ssl.ClientContextFactory()

        # self._conns[path] = connectWS(factory, context_factory)
        self._conns[path] = ReconnectingWebsocket(self._loop, self.FSTREAM_URL, path, coro, prefix)
        return path

    async def start_depth_socket(self, symbol, coro, depth=20, update_time_ms=100):
        """Start a websocket for symbol market depth returning either a diff or a partial book

        https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md#partial-book-depth-streams

        :param symbol: required
        :type symbol: str
        :param coro: callback coroutine to handle messages
        :type coro: async coroutine
        :param depth: optional Number of depth entries to return, default 20. If passed returns a partial book instead of a diff
        :type depth: int
        :param update_time_ms: optional int of update frequency in ms, default 100ms.
        :type update_time_ms: int

        :returns: connection key string if successful, False otherwise

        Partial Message Format

        .. code-block:: python

            {
                "lastUpdateId": 160,  # Last update ID
                "bids": [             # Bids to be updated
                    [
                        "0.0024",     # price level to be updated
                        "10",         # quantity
                        []            # ignore
                    ]
                ],
                "asks": [             # Asks to be updated
                    [
                        "0.0026",     # price level to be updated
                        "100",        # quantity
                        []            # ignore
                    ]
                ]
            }


        Diff Message Format

        .. code-block:: python

            {
                "e": "depthUpdate", # Event type
                "E": 123456789,     # Event time
                "s": "BNBBTC",      # Symbol
                "U": 157,           # First update ID in event
                "u": 160,           # Final update ID in event
                "b": [              # Bids to be updated
                    [
                        "0.0024",   # price level to be updated
                        "10",       # quantity
                        []          # ignore
                    ]
                ],
                "a": [              # Asks to be updated
                    [
                        "0.0026",   # price level to be updated
                        "100",      # quantity
                        []          # ignore
                    ]
                ]
            }

        """
        path = symbol.lower() + '@depth'
        if update_time_ms == 1000:
            path = '{}{}'.format(path, depth)
        else:
            path = '{}{}@{}ms'.format(path, depth, update_time_ms)
        await self._start_socket(path, coro)
        return path

    async def start_kline_socket(self, symbol, coro, interval=Client.KLINE_INTERVAL_1MINUTE):
        """Start a websocket for symbol kline data

        https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md#klinecandlestick-streams

        :param symbol: required
        :type symbol: str
        :param coro: callback function to handle messages
        :type coro: async coroutine
        :param interval: Kline interval, default KLINE_INTERVAL_1MINUTE
        :type interval: str

        :returns: connection key string if successful, False otherwise

        Message Format

        .. code-block:: python

            {
                "e": "kline",					# event type
                "E": 1499404907056,				# event time
                "s": "ETHBTC",					# symbol
                "k": {
                    "t": 1499404860000, 		# start time of this bar
                    "T": 1499404919999, 		# end time of this bar
                    "s": "ETHBTC",				# symbol
                    "i": "1m",					# interval
                    "f": 77462,					# first trade id
                    "L": 77465,					# last trade id
                    "o": "0.10278577",			# open
                    "c": "0.10278645",			# close
                    "h": "0.10278712",			# high
                    "l": "0.10278518",			# low
                    "v": "17.47929838",			# volume
                    "n": 4,						# number of trades
                    "x": false,					# whether this bar is final
                    "q": "1.79662878",			# quote volume
                    "V": "2.34879839",			# volume of active buy
                    "Q": "0.24142166",			# quote volume of active buy
                    "B": "13279784.01349473"	# can be ignored
                    }
            }
        """
        path = '{}@kline_{}'.format(symbol.lower(), interval)
        await self._start_socket(path, coro)
        return path

    async def start_miniticker_socket(self, coro, update_time=1000):
        """Start a miniticker websocket for all trades

        This is not in the official Binance api docs, but this is what
        feeds the right column on a ticker page on Binance.

        :param coro: callback function to handle messages
        :type coro: async coroutine
        :param update_time: time between callbacks in milliseconds, must be 1000 or greater
        :type update_time: int

        :returns: connection key string if successful, False otherwise

        Message Format

        .. code-block:: python

            [
                {
                    'e': '24hrMiniTicker',  # Event type
                    'E': 1515906156273,     # Event time
                    's': 'QTUMETH',         # Symbol
                    'c': '0.03836900',      # close
                    'o': '0.03953500',      # open
                    'h': '0.04400000',      # high
                    'l': '0.03756000',      # low
                    'v': '147435.80000000', # volume
                    'q': '5903.84338533'    # quote volume
                }
            ]
        """
        path = '!miniTicker@arr@{}ms'.format(update_time)
        await self._start_socket(path, coro)
        return path

    async def start_trade_socket(self, symbol, coro):
        """Start a websocket for symbol trade data

        https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md#trade-streams

        :param symbol: required
        :type symbol: str
        :param coro: async coroutine function to handle messages
        :type coro: async function

        :returns: connection key string if successful, False otherwise

        Message Format

        .. code-block:: python

            {
                "e": "trade",     # Event type
                "E": 123456789,   # Event time
                "s": "BNBBTC",    # Symbol
                "t": 12345,       # Trade ID
                "p": "0.001",     # Price
                "q": "100",       # Quantity
                "b": 88,          # Buyer order Id
                "a": 50,          # Seller order Id
                "T": 123456785,   # Trade time
                "m": true,        # Is the buyer the market maker?
                "M": true         # Ignore.
            }

        """

        # this allows execution to keep going
        path = symbol.lower() + '@trade'
        await self._start_socket(path, coro)
        return path

    async def start_aggtrade_socket(self, symbol, coro):
        """Start a websocket for symbol trade data

        https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md#aggregate-trade-streams

        :param symbol: required
        :type symbol: str
        :param coro: callback function to handle messages
        :type coro: function

        :returns: connection key string if successful, False otherwise

        Message Format

        .. code-block:: python

            {
                "e": "aggTrade",		# event type
                "E": 1499405254326,		# event time
                "s": "ETHBTC",			# symbol
                "a": 70232,				# aggregated tradeid
                "p": "0.10281118",		# price
                "q": "8.15632997",		# quantity
                "f": 77489,				# first breakdown trade id
                "l": 77489,				# last breakdown trade id
                "T": 1499405254324,		# trade time
                "m": false,				# whether buyer is a maker
                "M": true				# can be ignored
            }

        """
        path = symbol.lower() + '@aggTrade'
        await self._start_socket(path, coro)
        return path

    async def start_symbol_ticker_socket(self, symbol, coro):
        """Start a websocket for a symbol's ticker data

        https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md#individual-symbol-ticker-streams

        :param symbol: required
        :type symbol: str
        :param coro: callback function to handle messages
        :type coro: function

        :returns: connection key string if successful, False otherwise

        Message Format

        .. code-block:: python

            {
                "e": "24hrTicker",  # Event type
                "E": 123456789,     # Event time
                "s": "BNBBTC",      # Symbol
                "p": "0.0015",      # Price change
                "P": "250.00",      # Price change percent
                "w": "0.0018",      # Weighted average price
                "x": "0.0009",      # Previous day's close price
                "c": "0.0025",      # Current day's close price
                "Q": "10",          # Close trade's quantity
                "b": "0.0024",      # Best bid price
                "B": "10",          # Bid bid quantity
                "a": "0.0026",      # Best ask price
                "A": "100",         # Best ask quantity
                "o": "0.0010",      # Open price
                "h": "0.0025",      # High price
                "l": "0.0010",      # Low price
                "v": "10000",       # Total traded base asset volume
                "q": "18",          # Total traded quote asset volume
                "O": 0,             # Statistics open time
                "C": 86400000,      # Statistics close time
                "F": 0,             # First trade ID
                "L": 18150,         # Last trade Id
                "n": 18151          # Total number of trades
            }

        """
        path = symbol.lower() + '@ticker'
        await self._start_socket(path, coro)
        return path

    async def start_ticker_socket(self, coro):
        """Start a websocket for all ticker data

        By default all markets are included in an array.

        https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md#all-market-tickers-stream

        :param coro: callback function to handle messages
        :type coro: function

        :returns: connection key string if successful, False otherwise

        Message Format

        .. code-block:: python

            [
                {
                    'F': 278610,
                    'o': '0.07393000',
                    's': 'BCCBTC',
                    'C': 1509622420916,
                    'b': '0.07800800',
                    'l': '0.07160300',
                    'h': '0.08199900',
                    'L': 287722,
                    'P': '6.694',
                    'Q': '0.10000000',
                    'q': '1202.67106335',
                    'p': '0.00494900',
                    'O': 1509536020916,
                    'a': '0.07887800',
                    'n': 9113,
                    'B': '1.00000000',
                    'c': '0.07887900',
                    'x': '0.07399600',
                    'w': '0.07639068',
                    'A': '2.41900000',
                    'v': '15743.68900000'
                }
            ]
        """
        path = '!ticker@arr'
        await self._start_socket(path, coro)
        return path

    async def start_allticker_futures_socket(self, coro):
        """Start a websocket for all ticker data

        By default all markets are included in an array.

        https://binanceapitest.github.io/Binance-Futures-API-doc/wss/#all-book-tickers-stream

        :param coro: callback function to handle messages
        :type coro: function

        :returns: connection key string if successful, False otherwise

        Message Format

        .. code-block:: python

            [
                {
                  "u":400900217,     // order book updateId
                  "s":"BNBUSDT",     // symbol
                  "b":"25.35190000", // best bid price
                  "B":"31.21000000", // best bid qty
                  "a":"25.36520000", // best ask price
                  "A":"40.66000000"  // best ask qty
                }
            ]
        """
        path = '!bookTicker'
        await self._start_futures_socket(path, coro)
        return path

    async def start_symbol_ticker_futures_socket(self, symbol, coro):
        """Start a websocket for all ticker data

        By default all markets are included in an array.

        https://binanceapitest.github.io/Binance-Futures-API-doc/wss/#individual-symbol-mini-ticker-stream

        :param symbol: required
        :type symbol: str
        :param coro: callback function to handle messages
        :type coro: function

        :returns: connection key string if successful, False otherwise

        .. code-block:: python

            [
                {
                  "u":400900217,     // order book updateId
                  "s":"BNBUSDT",     // symbol
                  "b":"25.35190000", // best bid price
                  "B":"31.21000000", // best bid qty
                  "a":"25.36520000", // best ask price
                  "A":"40.66000000"  // best ask qty
                }
            ]
        """
        path = symbol.lower() + '@bookTicker'
        await self._start_futures_socket(path, coro)
        return path

    async def start_symbol_book_ticker_socket(self, symbol, coro):
        """Start a websocket for the best bid or ask's price or quantity for a specified symbol.

        https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md#individual-symbol-book-ticker-streams

        :param symbol: required
        :type symbol: str
        :param coro: callback function to handle messages
        :type coro: function

        :returns: connection key string if successful, False otherwise

        Message Format

        .. code-block:: python

            {
                "u":400900217,     // order book updateId
                "s":"BNBUSDT",     // symbol
                "b":"25.35190000", // best bid price
                "B":"31.21000000", // best bid qty
                "a":"25.36520000", // best ask price
                "A":"40.66000000"  // best ask qty
            }

        """
        path = symbol.lower() + '@bookTicker'
        await self._start_socket(path, coro)
        return path

    async def start_book_ticker_socket(self, coro):
        """Start a websocket for the best bid or ask's price or quantity for all symbols.

        https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md#all-book-tickers-stream

        :param coro: callback function to handle messages
        :type coro: function

        :returns: connection key string if successful, False otherwise

        Message Format

        .. code-block:: python

            {
                // Same as <symbol>@bookTicker payload
            }

        """
        path = '!bookTicker'
        await self._start_socket(path, coro)
        return path

    async def start_multiplex_socket(self, streams, coro):
        """Start a multiplexed socket using a list of socket names.
        User stream sockets can not be included.

        Symbols in socket name must be lowercase i.e bnbbtc@aggTrade, neobtc@ticker

        Combined stream events are wrapped as follows: {"stream":"<streamName>","data":<rawPayload>}

        https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md

        :param streams: list of stream names in lower case
        :type streams: list
        :param coro: callback function to handle messages
        :type coro: async function

        :returns: connection key string if successful, False otherwise

        Message Format - see Binance API docs for all types

        """
        path = 'streams={}'.format('/'.join(streams))
        await self._start_socket(path, coro, 'stream?')
        return path

    async def start_arbitrary_socket(self, ws_domain, path, coro, prefix='ws/'):
        if path in self._conns:
            return False

        self._conns[path] = ReconnectingWebsocket(self._loop, ws_domain, path, coro, prefix)

        return path

    async def start_user_socket(self, coro):
        """Start a websocket for user data

        https://www.binance.com/restapipub.html#user-wss-endpoint

        :param coro: callback function to handle messages
        :type coro: function

        :returns: connection key string if successful, False otherwise

        Message Format - see Binance API docs for all types
        """
        # Get the user listen key
        user_listen_key = await self._client.stream_get_listen_key()
        # and start the socket with this specific key
        conn_key = await self._start_account_socket('user', user_listen_key, coro)
        return conn_key

    def _start_socket_timer(self, socket_type):
        keepalive_func = partial(self._keepalive_account_socket, socket_type)
        self._timers[socket_type] = self._loop.call_later(self._user_timeout, keepalive_func)

    def _keepalive_account_socket(self, socket_type):
        async def _run():
            if socket_type == 'user':
                listen_key = await self._client.stream_get_listen_key()
                callback = self._account_callbacks[socket_type]
            else:
                listen_key = await self._client.margin_stream_get_listen_key()
                callback = self._account_callbacks[socket_type]
            self._log.debug("new key {} old key {}".format(listen_key, self._listen_keys[socket_type]))
            if listen_key != self._listen_keys[socket_type]:
                # Start a new socket with the key received
                # `_start_account_socket` automatically cleanup open sockets
                # and starts timer to keep socket alive
                await self._start_account_socket(socket_type, listen_key, callback)
            else:
                # Restart timer only if the user listen key remains the same
                self._start_socket_timer()

        # this allows execution to keep going
        asyncio.ensure_future(_run())

    async def stop_socket(self, conn_key):
        """Stop a websocket given the connection key

        :param conn_key: Socket connection key
        :type conn_key: string

        :returns: connection key string if successful, False otherwise
        """
        if conn_key not in self._conns:
            return

        # disable reconnecting if we are closing
        await self._conns[conn_key].cancel()
        del (self._conns[conn_key])

        # check if we have a user stream socket
        if len(conn_key) >= 60 and conn_key[:60] == self._listen_keys['user']:
            await self._stop_account_socket('user')

        # or a margin stream socket
        if len(conn_key) >= 60 and conn_key[:60] == self._listen_keys['margin']:
            await self._stop_account_socket('margin')

    async def _stop_account_socket(self, socket_type):
        if not self._listen_keys[socket_type]:
            return
        # stop the timer
        if self._timers[socket_type]:
            self._timers[socket_type].cancel()
            self._timers[socket_type] = None
        # close the stream
        if socket_type == 'user':
            await self._client.stream_close(listenKey=self._listen_keys[socket_type])
        else:
            await self._client.margin_stream_close(listenKey=self._listen_keys[socket_type])
        self._listen_keys[socket_type] = None

    async def start_margin_socket(self, callback):
        """Start a websocket for margin data
        https://github.com/binance-exchange/binance-official-api-docs/blob/master/user-data-stream.md
        :param callback: callback function to handle messages
        :type callback: function
        :returns: connection key string if successful, False otherwise
        Message Format - see Binance API docs for all types
        """
        # Get the user margin listen key
        margin_listen_key = self._client.margin_stream_get_listen_key()
        # and start the socket with this specific key
        conn_key = self._start_account_socket('margin', margin_listen_key, callback)
        return conn_key

    async def _start_account_socket(self, socket_type, listen_key, callback):
        """Starts one of user or margin socket"""
        await self._check_account_socket_open(listen_key)
        self._listen_keys[socket_type] = listen_key
        self._account_callbacks[socket_type] = callback
        if socket_type == 'user':
            conn_key = await self._start_socket(listen_key, callback)
        else:
            conn_key = await self._start_futures_socket(listen_key, callback)
        if conn_key:
            # start timer to keep socket alive
            self._start_socket_timer(socket_type)
        return conn_key

    async def _check_account_socket_open(self, listen_key):
        # With this function we can start a user socket with a specific key
        if not listen_key:
            return
        for conn_key in self._conns:
            # cleanup any sockets with this key
            if len(conn_key) >= 60 and conn_key[:60] == listen_key:
                await self.stop_socket(conn_key)
                break

    async def close(self):
        """Close all connections

        """
        keys = set(self._conns.keys())
        for key in keys:
            await self.stop_socket(key)

        self._conns = {}
