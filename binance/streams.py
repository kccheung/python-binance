import asyncio
import json
import orjson
import logging
import time
from enum import Enum
from socket import gaierror
from typing import Optional, List, Dict, Callable, Any

import websockets as ws
from websockets.legacy.protocol import State
from websockets.exceptions import ConnectionClosed

from .client import AsyncClient
from .exceptions import BinanceWebsocketUnableToConnect
from .enums import KLINE_INTERVAL_1MINUTE, FuturesType
from .threaded_stream import ThreadedApiManager

import hashlib
import hmac
from base64 import b64encode
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa


KEEPALIVE_TIMEOUT = 5 * 60  # 5 minutes
WS_API_TIMEOUT = 60  # 1 minutes


class WSListenerState(Enum):
    INITIALISING = 'I'
    CONNECTING = 'C'
    STREAMING = 'S'
    RECONNECTING = 'R'
    EXITING = 'E'


class BinanceSocketType(str, Enum):
    SPOT = 'S'
    SPOT_SBE = 'B'
    USD_M_FUTURES = 'U'
    COIN_M_FUTURES = 'C'
    OPTIONS = 'V'
    ACCOUNT = 'A'


class ReconnectingWebsocket:
    MAX_RECONNECTS = 1000
    MAX_RECONNECT_SECONDS = 1
    MIN_RECONNECT_WAIT = 0.1
    TIMEOUT = 60

    def __init__(
            self, loop, url: str, path: Optional[str] = None, prefix: str = 'ws/', exit_coro=None
    ):
        self._loop = loop or asyncio.get_event_loop()
        self._log = logging.getLogger(__name__)
        self._path = path
        self._url = url
        self._exit_coro = exit_coro
        self._prefix = prefix
        self._reconnects = 0
        self._conn = None
        self._socket = None
        self.ws: Optional[ws.WebSocketClientProtocol] = None
        self.ws_state = WSListenerState.INITIALISING
        self._queue = asyncio.Queue(loop=self._loop)
        self._handle_read_loop = None
        self._read_loop_finish = asyncio.Event(loop=self._loop)
        self._reconnect_waiter = asyncio.Event(loop=self._loop)
        self._reconnect_waiter.clear()

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect(exc_type, exc_val, exc_tb)

    async def connect(self):
        self.ws_state = WSListenerState.CONNECTING
        await self._before_connect()
        assert self._path
        ws_url = self._url + self._prefix + self._path
        self._conn = ws.connect(ws_url, close_timeout=0.1, ping_interval=None)
        try:
            self.ws = await self._conn.__aenter__()
        except:  # noqa
            asyncio.ensure_future(self._reconnect(), loop=self._loop)
            return
        self.ws_state = WSListenerState.STREAMING
        self._reconnects = 0
        await self._after_connect()
        self._reconnect_waiter.set()
        if self._handle_read_loop:
            await self._read_loop_finish.wait()
        self._handle_read_loop = self._loop.call_soon_threadsafe(asyncio.create_task, self._read_loop())

    async def disconnect(self, exc_type=None, exc_val=None, exc_tb=None):
        self.ws_state = WSListenerState.EXITING
        if self._exit_coro:
            await self._exit_coro(self._path)
        if self.ws:
            # self.ws.fail_connection()
            await self.ws.close()
        if self._conn and hasattr(self._conn, 'protocol'):
            await self._conn.__aexit__(exc_type, exc_val, exc_tb)
        self.ws = None
        if self._handle_read_loop:
            await self._read_loop_finish.wait()

    async def _before_connect(self):
        pass

    async def _after_connect(self):
        pass

    def _handle_message(self, evt):
        try:
            return orjson.loads(evt)
        except ValueError:
            self._log.debug(f'error parsing evt json:{evt}')
            return None

    async def send(self, msg: str):
        while 1:
            try:
                await self.ws.send(msg)
                break
            except asyncio.CancelledError as e:
                self._log.debug(f"cancelled error {e}")
                break
            except ConnectionClosed as e:
                self._log.debug(f"connection close error ({e})")
                if self.ws_state == WSListenerState.EXITING:
                    break
                if self.ws:
                    if self.ws.state == State.CLOSED:
                        self._reconnect_waiter.clear()
                        asyncio.ensure_future(self._reconnect(), loop=self._loop)
                await self._reconnect_waiter.wait()
            except gaierror as e:
                self._log.debug(f"DNS Error ({e})")
                break
            except BinanceWebsocketUnableToConnect as e:
                self._log.debug(f"BinanceWebsocketUnableToConnect ({e})")
                break
            except Exception as e:
                self._log.debug(f"Unknown exception ({e})")
                break

    async def _read_loop(self):
        self._read_loop_finish.clear()
        while 1:
            try:
                if not self.ws or self.ws_state != WSListenerState.STREAMING:
                    break
                else:
                    res = await self.ws.recv()
                    res = self._handle_message(res)
                    if res:
                        self._queue.put_nowait(res)
            except asyncio.CancelledError as e:
                self._log.debug(f"cancelled error {e}")
                break
            except asyncio.IncompleteReadError as e:
                self._log.debug(f"incomplete read error ({e})")
            except ConnectionClosed as e:
                self._log.debug(f"connection close error ({e})")
                if self.ws_state == WSListenerState.EXITING:
                    break
                if self.ws:
                    if self.ws.state == State.CLOSED:
                        self._reconnect_waiter.clear()
                        asyncio.ensure_future(self._reconnect(), loop=self._loop)
                break
            except gaierror as e:
                self._log.debug(f"DNS Error ({e})")
            except BinanceWebsocketUnableToConnect as e:
                self._log.debug(f"BinanceWebsocketUnableToConnect ({e})")
                break
            except Exception as e:
                self._log.debug(f"Unknown exception ({e})")
        self._handle_read_loop = None  # Signal the coro is stopped
        if not self._read_loop_finish.is_set():
            self._read_loop_finish.set()

    async def recv(self):
        while 1:
            try:
                if self._queue.empty():
                    msgs = [await asyncio.wait_for(self._queue.get(), timeout=self.TIMEOUT, loop=self._loop)]
                    if self._queue.empty():
                        return msgs
                    else:
                        return msgs + [self._queue.get_nowait() for _ in range(self._queue.qsize())]
                else:
                    return [self._queue.get_nowait() for _ in range(self._queue.qsize())]
            except asyncio.TimeoutError:
                self._log.debug(f"no message in {self.TIMEOUT} seconds")

    def _get_reconnect_wait(self, attempts: int) -> float:
        # expo = 2 ** attempts
        # return round(random() * min(self.MAX_RECONNECT_SECONDS, expo - 1) + 1)
        return self.MIN_RECONNECT_WAIT

    async def before_reconnect(self):
        if self.ws:
            self.ws = None

        if self._conn and hasattr(self._conn, 'protocol'):
            await self._conn.__aexit__(None, None, None)

        self._reconnects += 1

    async def _reconnect(self):
        if self.ws_state == WSListenerState.RECONNECTING:
            return
        self.ws_state = WSListenerState.RECONNECTING
        await self.before_reconnect()
        if self._reconnects < self.MAX_RECONNECTS:
            reconnect_wait = self._get_reconnect_wait(self._reconnects)
            self._log.debug(
                f"websocket reconnecting. {self.MAX_RECONNECTS - self._reconnects} reconnects left - "
                f"waiting {reconnect_wait}"
            )
            await asyncio.sleep(reconnect_wait)
            asyncio.ensure_future(self.connect(), loop=self._loop)
        else:
            self._log.error(f'Max reconnections {self.MAX_RECONNECTS} reached:')
            raise BinanceWebsocketUnableToConnect


class KeepAliveWebsocket(ReconnectingWebsocket):

    def __init__(
            self, client: AsyncClient, loop, url, keepalive_type, prefix='ws/', exit_coro=None,
            user_timeout=None
    ):
        super().__init__(loop=loop, path=None, url=url, prefix=prefix, exit_coro=exit_coro)
        self._keepalive_type = keepalive_type
        self._client = client
        self._user_timeout = user_timeout or KEEPALIVE_TIMEOUT
        self._timer = None
        self._keepalive_socket_coro = None

    async def disconnect(self, *args, **kwargs):
        if not self._path:
            return
        if self._keepalive_socket_coro:
            self._keepalive_socket_coro.close()
        if self._timer:
            self._timer.cancel()
            self._timer = None
        await super().disconnect(*args, **kwargs)

    async def _before_connect(self):
        if not self._path:
            self._path = await self._get_listen_key()

    async def _after_connect(self):
        self._start_socket_timer()

    def _start_socket_timer(self):
        self._keepalive_socket_coro = self._keepalive_socket()
        self._timer = self._loop.call_later(
            self._user_timeout,
            asyncio.create_task,
            self._keepalive_socket_coro
        )

    async def _get_listen_key(self):
        i = 0
        while i < 10:
            try:
                if self._keepalive_type == 'user':
                    listen_key = await self._client.stream_get_listen_key()
                elif self._keepalive_type == 'margin':  # cross-margin
                    listen_key = await self._client.margin_stream_get_listen_key()
                elif self._keepalive_type == 'futures':
                    listen_key = await self._client.futures_stream_get_listen_key()
                elif self._keepalive_type == 'coin_futures':
                    listen_key = await self._client.tfutures_stream_get_listen_key()
                else:  # isolated margin
                    # Passing symbol for isolated margin
                    listen_key = await self._client.isolated_stream_get_listen_key(self._keepalive_type)
                return listen_key
            except Exception as e:
                self._log.debug("get_listen_key exception: %s" % str(e))
                i += 1

    async def _keepalive_socket(self):
        listen_key = await self._get_listen_key()

        if listen_key != self._path:
            self._log.debug("listen key changed: reconnect")
            self._path = listen_key
            await self._reconnect()
        else:
            self._log.debug("listen key same: keepalive")
            i = 0
            while i < 10:
                try:
                    if self._keepalive_type == 'user':
                        await self._client.stream_keepalive(self._path)
                    elif self._keepalive_type == 'margin':  # cross-margin
                        await self._client.margin_stream_keepalive(self._path)
                    elif self._keepalive_type == 'futures':
                        await self._client.futures_stream_keepalive(self._path)
                    elif self._keepalive_type == 'coin_futures':
                        await self._client.tfutures_stream_keepalive(self._path)
                    else:  # isolated margin
                        # Passing symbol for isolated margin
                        await self._client.isolated_stream_keepalive(self._path, self._keepalive_type)
                    break
                except Exception as e:
                    self._log.debug("keepalive_socket exception: %s" % str(e))
                    i += 1
            self._start_socket_timer()


class ReconnectingWebsocketSBE(ReconnectingWebsocket):

    def __init__(self, client: AsyncClient, loop, url: str, path: Optional[str] = None, prefix: str = 'ws/', exit_coro=None):
        super().__init__(loop=loop, url=url, path=path, prefix=prefix, exit_coro=exit_coro)
        self._client = client

    async def connect(self):
        self.ws_state = WSListenerState.CONNECTING
        await self._before_connect()
        assert self._path
        ws_url = self._url + self._prefix + self._path
        self._conn = ws.connect(ws_url, close_timeout=0.1, ping_interval=None, extra_headers={'X-MBX-APIKEY': self._client.API_KEY})
        try:
            self.ws = await self._conn.__aenter__()
        except:  # noqa
            asyncio.ensure_future(self._reconnect(), loop=self._loop)
            return
        self.ws_state = WSListenerState.STREAMING
        self._reconnects = 0
        await self._after_connect()
        self._reconnect_waiter.set()
        if self._handle_read_loop:
            await self._read_loop_finish.wait()
        self._handle_read_loop = self._loop.call_soon_threadsafe(asyncio.create_task, self._read_loop())

    def _handle_message(self, evt):
        return evt


class UserDataWebsocket(ReconnectingWebsocket):

    def __init__(self, client: AsyncClient, loop, url: str, path: Optional[str] = 'v3?returnRateLimits=false', prefix: str = 'ws-api/', exit_coro=None):
        super().__init__(loop=loop, url=url, path=path, prefix=prefix, exit_coro=exit_coro)
        self._client = client
        self.timestamp_offset = client.timestamp_offset

    async def connect(self):
        self.ws_state = WSListenerState.CONNECTING
        await self._before_connect()
        ws_url = self._url + self._prefix
        self._conn = ws.connect(ws_url, close_timeout=0.1, ping_interval=None)
        try:
            self.ws = await self._conn.__aenter__()
        except:  # noqa
            asyncio.ensure_future(self._reconnect(), loop=self._loop)
            return
        self.ws_state = WSListenerState.STREAMING
        self._reconnects = 0
        await self._after_connect()
        self._reconnect_waiter.set()
        if self._handle_read_loop:
            await self._read_loop_finish.wait()
        self._handle_read_loop = self._loop.call_soon_threadsafe(asyncio.create_task, self._read_loop())

    def _sign(self, msg) -> str:
        # default to ed25519
        return b64encode(self._client.API_SECRET.sign(msg.encode())).decode().replace('=', '%3D').replace('/', '%2F').replace('+', '%2B')

    async def _request(self, rid: str, method: str, **params):
        if params:
            await self.send(json.dumps({'id': rid, 'method': method, 'params': params}))
        else:
            await self.send(json.dumps({'id': rid, 'method': method}))

    async def _request_signed(self, rid: str, method: str, ai=0, **params):
        params['apiKey'] = self._client.API_KEY
        params['timestamp'] = int(time.time() * 1000 + self.timestamp_offset)
        params['signature'] = self._sign('&'.join([f'{kv[0]}={kv[1]}' for kv in sorted(params.items())]))
        await self.send(json.dumps({'id': rid, 'method': method, 'params': params}))

    async def logon(self, **params):
        await self._request_signed('logon', 'session.logon', **params)
        return {'success': True}

    async def logout(self):
        await self._request('logout', 'session.logout')
        return {'success': True}

    async def subscribe(self):
        await self._request('subscribe', 'userDataStream.subscribe')
        return {'success': True}

    async def unsubscribe(self):
        await self._request('unsubscribe', 'userDataStream.unsubscribe')
        return {'success': True}

    async def _after_connect(self):
        await self.logon()
        await self.subscribe()


class UserDataWebsocketSBE(ReconnectingWebsocket):

    def __init__(self, client: AsyncClient, loop, url: str, path: Optional[str] = None, prefix: str = 'ws-api/v3?returnRateLimits=false&responseFormat=sbe&sbeSchemaId=3&sbeSchemaVersion=0', exit_coro=None):
        super().__init__(loop=loop, url=url, path=path, prefix=prefix, exit_coro=exit_coro)
        self._client = client
        self.timestamp_offset = client.timestamp_offset

    def _sign(self, msg) -> str:
        # default to ed25519
        return b64encode(self._client.API_SECRET.sign(msg.encode())).decode().replace('=', '%3D').replace('/', '%2F').replace('+', '%2B')

    def _handle_message(self, evt):
        return evt

    async def _request(self, rid: str, method: str, **params):
        if params:
            await self.send(json.dumps({'id': rid, 'method': method, 'params': params}))
        else:
            await self.send(json.dumps({'id': rid, 'method': method}))

    async def _request_signed(self, rid: str, method: str, ai=0, **params):
        params['apiKey'] = self._client.API_KEY
        params['timestamp'] = int(time.time() * 1000 + self.timestamp_offset)
        params['signature'] = self._sign('&'.join([f'{kv[0]}={kv[1]}' for kv in sorted(params.items())]))
        await self.send(json.dumps({'id': rid, 'method': method, 'params': params}))

    async def logon(self, **params):
        await self._request_signed('logon', 'session.logon', **params)
        return {'success': True}

    async def logout(self):
        await self._request('logout', 'session.logout')
        return {'success': True}

    async def subscribe(self):
        await self._request('subscribe', 'userDataStream.subscribe')
        return {'success': True}

    async def unsubscribe(self):
        await self._request('unsubscribe', 'userDataStream.unsubscribe')
        return {'success': True}

    async def _after_connect(self):
        await self.logon()
        await self.subscribe()


class BinanceWebsocketApi(ReconnectingWebsocket):

    WS_API_URL = 'wss://ws-api.binance.com:443/'
    WS_API_TESTNET_URL = 'wss://ws-api.testnet.binance.vision/'

    def __init__(self, clients: List[AsyncClient], loop, prefix='ws-api/v3?returnRateLimits=false', exit_coro=None, user_timeout=None, testnet=False):
        self.ws_api_url = self.WS_API_TESTNET_URL if testnet else self.WS_API_URL
        super().__init__(loop=loop, url=self.ws_api_url, path=prefix, prefix='', exit_coro=exit_coro)
        self.API_KEYs = []
        self.API_SECRETs = []
        self._signs = []
        for ai in range(len(clients)):
            self.API_KEYs.append(clients[ai].API_KEY)
            self.API_SECRETs.append(clients[ai].API_SECRET)
            self._signs.append(self._no_sign)
            if self.API_SECRETs[ai]:
                if type(self.API_SECRETs[ai]) is bytes:
                    self._signs[ai] = self._hmac
                elif isinstance(self.API_SECRETs[ai], rsa.RSAPrivateKey):
                    self._signs[ai] = self._rsa
                else:
                    self._signs[ai] = self._ed25519
        self.timestamp_offset = clients[0].timestamp_offset
        self._user_timeout = user_timeout or WS_API_TIMEOUT
        self._timer = None
        self._pong_coro = None

    async def disconnect(self, *args, **kwargs):
        if self._pong_coro:
            self._pong_coro.close()
        if self._timer:
            self._timer.cancel()
            self._timer = None
        await super().disconnect(*args, **kwargs)

    async def _after_connect(self):
        self._start_socket_timer()

    def _start_socket_timer(self):
        self._pong_coro = self._pong()
        self._timer = self._loop.call_later(
            self._user_timeout,
            asyncio.create_task,
            self._pong_coro
        )

    async def _pong(self):
        await self.ws.pong('')
        self._start_socket_timer()

    def _hmac(self, msg, ai=0) -> str:
        return hmac.new(self.API_SECRETs[ai], msg.encode(), hashlib.sha256).hexdigest()

    def _rsa(self, msg, ai=0) -> str:
        return b64encode(self.API_SECRETs[ai].sign(msg.encode(), padding.PKCS1v15(), hashes.SHA256())).decode().replace('=', '%3D').replace('/', '%2F').replace('+', '%2B')

    def _ed25519(self, msg, ai=0) -> str:
        return b64encode(self.API_SECRETs[ai].sign(msg.encode())).decode().replace('=', '%3D').replace('/', '%2F').replace('+', '%2B')

    def _no_sign(self, msg, ai=0) -> str:
        return ''

    async def _request(self, rid: str, method: str, **params):
        if params:
            await self.send(json.dumps({'id': rid, 'method': method, 'params': params}))
        else:
            await self.send(json.dumps({'id': rid, 'method': method}))

    async def _request_signed(self, rid: str, method: str, ai=0, **params):
        params['apiKey'] = self.API_KEYs[ai]
        params['timestamp'] = int(time.time() * 1000 + self.timestamp_offset)
        params['signature'] = self._signs[ai]('&'.join([f'{kv[0]}={kv[1]}' for kv in sorted(params.items())]), ai)
        await self.send(json.dumps({'id': rid, 'method': method, 'params': params}))

    async def ping(self, rid: str):
        """Test connectivity to the WebSocket API.
        https://binance-docs.github.io/apidocs/websocket_api/en/#test-connectivity
        :returns: Empty array
        .. code-block:: python
            {
              "id": "922bcc6e-9de8-440d-9e84-7c80933a8d0d",
              "status": 200,
              "result": {}
            }
        """
        await self._request(rid, 'ping')
        return {'success': True}

    async def logon(self, ai=0, **params):
        """
        Only ONE api key can be logon per connection
        """
        await self._request_signed(f'logon_{ai}', 'session.logon', ai, **params)
        return {'success': True}

    async def logout(self):
        await self._request('logout', 'session.logout')
        return {'success': True}

    async def create_order(self, ai=0, **params):
        """Send in a new order
        Any order with an icebergQty MUST have timeInForce set to GTC.
        https://binance-docs.github.io/apidocs/spot/en/#new-order-trade
        :param ai: account index
        :type ai: int
        :param symbol: required
        :type symbol: str
        :param side: required
        :type side: str
        :param type: required
        :type type: str
        :param timeInForce: required if limit order
        :type timeInForce: str
        :param quantity: required
        :type quantity: decimal
        :param quoteOrderQty: amount the user wants to spend (when buying) or receive (when selling)
            of the quote asset, applicable to MARKET orders
        :type quoteOrderQty: decimal
        :param price: required
        :type price: str
        :param newClientOrderId: A unique id for the order. Automatically generated if not sent.
        :type newClientOrderId: str
        :param icebergQty: Used with LIMIT, STOP_LOSS_LIMIT, and TAKE_PROFIT_LIMIT to create an iceberg order.
        :type icebergQty: decimal
        :param newOrderRespType: Set the response JSON. ACK, RESULT, or FULL; default: RESULT.
        :type newOrderRespType: str
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        Response ACK:
        .. code-block:: python
            {
              "id": "56374a46-3061-486b-a311-99ee972eb648",
              "status": 200,
              "result": {
                "symbol": "BTCUSDT",
                "orderId": 12569099453,
                "orderListId": -1, // always -1 for singular orders
                "clientOrderId": "4d96324ff9d44481926157ec08158a40",
                "transactTime": 1660801715639
              }
            }
        Response RESULT:
        .. code-block:: python
            {
              "id": "56374a46-3061-486b-a311-99ee972eb648",
              "status": 200,
              "result": {
                "symbol": "BTCUSDT",
                "orderId": 12569099453,
                "orderListId": -1, // always -1 for singular orders
                "clientOrderId": "4d96324ff9d44481926157ec08158a40",
                "transactTime": 1660801715639,
                "price": "23416.10000000",
                "origQty": "0.00847000",
                "executedQty": "0.00000000",
                "cummulativeQuoteQty": "0.00000000",
                "status": "NEW",
                "timeInForce": "GTC",
                "type": "LIMIT",
                "side": "SELL",
                "workingTime": 1660801715639,
                "selfTradePreventionMode": "NONE"
              }
            }
        Response FULL:
        .. code-block:: python
            {
              "id": "56374a46-3061-486b-a311-99ee972eb648",
              "status": 200,
              "result": {
                "symbol": "BTCUSDT",
                "orderId": 12569099453,
                "orderListId": -1,
                "clientOrderId": "4d96324ff9d44481926157ec08158a40",
                "transactTime": 1660801715793,
                "price": "23416.10000000",
                "origQty": "0.00847000",
                "executedQty": "0.00847000",
                "cummulativeQuoteQty": "198.33521500",
                "status": "FILLED",
                "timeInForce": "GTC",
                "type": "LIMIT",
                "side": "SELL",
                "workingTime": 1660801715793,
                // FULL response is identical to RESULT response, with the same optional fields
                // based on the order type and parameters. FULL response additionally includes
                // the list of trades which immediately filled the order.
                "fills": [
                  {
                    "price": "23416.10000000",
                    "qty": "0.00635000",
                    "commission": "0.000000",
                    "commissionAsset": "BNB",
                    "tradeId": 1650422481
                  },
                  {
                    "price": "23416.50000000",
                    "qty": "0.00212000",
                    "commission": "0.000000",
                    "commissionAsset": "BNB",
                    "tradeId": 1650422482
                  }
                ],
                "selfTradePreventionMode": "NONE"
              }
            }
        :raises: BinanceRequestException, BinanceAPIException, BinanceOrderException, BinanceOrderMinAmountException, BinanceOrderMinPriceException, BinanceOrderMinTotalException, BinanceOrderUnknownSymbolException, BinanceOrderInactiveSymbolException
        """
        await self._request_signed(params['newClientOrderId'], 'order.place', ai, **params)
        return {'success': True}


class BinanceSocketManager:
    STREAM_URLS = ['wss://stream.binance.com:9443/', 'wss://stream.binance.com:443/']
    STREAM_TESTNET_URL = 'wss://testnet.binance.vision/'
    SBE_STREAM_URLS = ['wss://stream-sbe.binance.com/', 'wss://stream-sbe.binance.com:9443/']
    SBE_STREAM_TESTNET_URLS = ['wss://stream-sbe.testnet.binance.vision/', 'wss://stream-sbe.testnet.binance.vision:9443/']
    WS_API_URLS = ['wss://ws-api.binance.com:443/', 'wss://ws-api.binance.com:9443/']
    WS_API_TESTNET_URL = 'wss://ws-api.testnet.binance.vision/'
    DATA_STREAM_URL = 'wss://data-stream.binance.vision/'
    DATA_STREAM_URL_OLD = 'wss://data-stream.binance.com/'
    FSTREAM_URL = 'wss://fstream.binance.com/'
    FSTREAM_TESTNET_URL = 'wss://stream.binancefuture.com/'
    DSTREAM_URL = 'wss://dstream.binance.com/'
    DSTREAM_TESTNET_URL = 'wss://dstream.binancefuture.com/'

    def __init__(self, client: AsyncClient, loop=None, option=0, user_timeout=KEEPALIVE_TIMEOUT):
        """Initialise the BinanceSocketManager
        :param client: Binance API client
        :type client: binance.AsyncClient
        """
        self._conns = {}
        self._loop = loop or asyncio.get_event_loop()
        self._client = client
        if option == 1:
            self._default_option = 1
            self._default_stream_url = self.STREAM_URLS[1]
            self._default_ws_api_url = self.WS_API_URLS[1]
            self._default_sbe_stream_url = self.SBE_STREAM_URLS[1]
            self._default_sbe_stream_testnet_url = self.SBE_STREAM_TESTNET_URLS[1]
        else:
            self._default_option = 0
            self._default_stream_url = self.STREAM_URLS[0]
            self._default_ws_api_url = self.WS_API_URLS[0]
            self._default_sbe_stream_url = self.SBE_STREAM_URLS[0]
            self._default_sbe_stream_testnet_url = self.SBE_STREAM_TESTNET_URLS[0]
        self._user_timeout = user_timeout

        self.testnet = False

    def _get_stream_url(self, option: Optional[int] = None):
        if option:
            if option == 2:
                return self.DATA_STREAM_URL
            elif option in [0, 1]:
                return self.STREAM_URLS[option]
            elif option == 3:
                return self.DATA_STREAM_URL_OLD
        return self._default_stream_url

    def _get_ws_api_url(self, option: Optional[int] = None):
        if option:
            if option in [0, 1]:
                return self.WS_API_URLS[option]
        return self._default_ws_api_url

    def _get_sbe_stream_url(self, option: Optional[int] = None):
        if option:
            if option in [0, 1]:
                return self.SBE_STREAM_URLS[option]
        return self._default_sbe_stream_url

    def _get_sbe_stream_testnet_url(self, option: Optional[int] = None):
        if option:
            if option in [0, 1]:
                return self.SBE_STREAM_TESTNET_URLS[option]
        return self._default_sbe_stream_testnet_url

    def _get_socket(self, path: str, option: Optional[int] = None, prefix: str = 'ws/'):
        conn_id = f'{BinanceSocketType.SPOT}{option if option in [0, 1, 2, 3] else self._default_option}{path}'
        if conn_id not in self._conns:
            self._conns[conn_id] = ReconnectingWebsocket(
                loop=self._loop,
                path=path,
                url=self._get_stream_url(option),
                prefix=prefix,
                exit_coro=self._stop_socket,
            )
        return self._conns[conn_id]

    def _get_account_socket_old(self, path: str, option: Optional[int] = None, prefix: str = 'ws/'):
        conn_id = f'{BinanceSocketType.ACCOUNT}{option if option in [0, 1] else self._default_option}{path}'
        if conn_id not in self._conns:
            self._conns[conn_id] = KeepAliveWebsocket(
                client=self._client,
                loop=self._loop,
                url=self._get_stream_url(option),
                keepalive_type=path,
                prefix=prefix,
                exit_coro=self._stop_socket,
                user_timeout=self._user_timeout
            )
        return self._conns[conn_id]

    def _get_account_socket(self, option: Optional[int] = None, prefix: str = 'ws-api/v3?returnRateLimits=false'):
        conn_id = f'{BinanceSocketType.ACCOUNT}{option if option in [0, 1] else self._default_option}'
        if conn_id not in self._conns:
            self._conns[conn_id] = UserDataWebsocket(
                client=self._client,
                loop=self._loop,
                url=self._get_ws_api_url(option),
                prefix=prefix,
                exit_coro=self._stop_socket
            )
        return self._conns[conn_id]

    def _get_account_sbe_socket(self, option: Optional[int] = None, prefix: str = 'ws-api/v3?returnRateLimits=false'):
        conn_id = f'{BinanceSocketType.ACCOUNT}{option if option in [0, 1] else self._default_option}'
        if conn_id not in self._conns:
            self._conns[conn_id] = KeepAliveWebsocket(
                client=self._client,
                loop=self._loop,
                url=self._get_stream_url(option),
                keepalive_type=path,
                prefix=prefix,
                exit_coro=self._stop_socket,
                user_timeout=self._user_timeout
            )
        return self._conns[conn_id]

    def _get_sbe_socket(self, path: str, option: Optional[int] = None, prefix: str = 'ws/'):
        conn_id = f'{BinanceSocketType.SPOT_SBE}{option if option in [0, 1] else self._default_option}{path}'
        if conn_id not in self._conns:
            self._conns[conn_id] = ReconnectingWebsocketSBE(
                client=self._client,
                loop=self._loop,
                path=path,
                url=self._get_sbe_stream_url(option),
                prefix=prefix,
                exit_coro=self._stop_socket,
            )
        return self._conns[conn_id]

    def _get_sbe_testnet_socket(self, path: str, option: Optional[int] = None, prefix: str = 'ws/'):
        conn_id = f'{BinanceSocketType.SPOT_SBE}{option if option in [0, 1] else self._default_option}{path}'
        if conn_id not in self._conns:
            self._conns[conn_id] = ReconnectingWebsocketSBE(
                client=self._client,
                loop=self._loop,
                path=path,
                url=self._get_sbe_stream_testnet_url(option),
                prefix=prefix,
                exit_coro=self._stop_socket,
            )
        return self._conns[conn_id]

    def _get_futures_socket(self, path: str, futures_type: FuturesType, prefix: str = 'stream?streams='):
        conn_id = f'{BinanceSocketType.USD_M_FUTURES}{path}'
        if conn_id not in self._conns:
            self._conns[conn_id] = ReconnectingWebsocket(
                loop=self._loop,
                path=path,
                url=(self.FSTREAM_TESTNET_URL if self.testnet else self.FSTREAM_URL) if futures_type == FuturesType.USD_M else (self.DSTREAM_TESTNET_URL if self.testnet else self.DSTREAM_URL),
                prefix=prefix,
                exit_coro=self._stop_socket,
            )
        return self._conns[conn_id]

    def _get_futures_account_socket(self, path: str, prefix: str = 'ws/'):
        conn_id = f'{BinanceSocketType.ACCOUNT}{path}'
        if conn_id not in self._conns:
            self._conns[conn_id] = KeepAliveWebsocket(
                client=self._client,
                loop=self._loop,
                url=self.FSTREAM_URL if path == 'futures' else self.DSTREAM_URL,
                keepalive_type=path,
                prefix=prefix,
                exit_coro=self._stop_socket,
                user_timeout=self._user_timeout
            )
        return self._conns[conn_id]

    def depth_socket(self, symbol: str, depth: Optional[int] = 20, interval: Optional[int] = 100, option: Optional[int] = None):
        """Start a websocket for symbol market depth returning either a diff or a partial book
        https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md#partial-book-depth-streams
        :param symbol: required
        :type symbol: str
        :param depth: optional Number of depth entries to return, default None. If passed returns a partial book instead of a diff
        :type depth: str
        :param interval: optional interval for updates, default None. If not set, updates happen every second. Must be 0, None (1s) or 100 (100ms)
        :type interval: int
        :param option: base endpoint used, default 2 is data endpoint, 0 and 1 are the main endpoints
        :type option: int
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
        socket_name = symbol.lower() + '@depth'
        if depth:
            if depth in [20, 10, 5]:
                socket_name = f'{socket_name}{depth}'
            else:
                raise ValueError("Websocket depth value not allowed. Allowed values are {None, 5, 10, 20}")
        if interval:
            if interval in [100, 1000]:
                socket_name = f'{socket_name}@{interval}ms'
            else:
                raise ValueError("Websocket interval value not allowed. Allowed values are {100, 1000}")
        return self._get_socket(socket_name, option)

    def kline_socket(self, symbol: str, interval=KLINE_INTERVAL_1MINUTE, option: Optional[int] = None):
        """Start a websocket for symbol kline data
        https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md#klinecandlestick-streams
        :param symbol: required
        :type symbol: str
        :param interval: Kline interval, default KLINE_INTERVAL_1MINUTE
        :type interval: str
        :param option: base endpoint used, default 2 is data endpoint, 0 and 1 are the main endpoints
        :type option: int
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
        return self._get_socket(f'{symbol.lower()}@kline_{interval}', option)

    def miniticker_socket(self, update_time: int = 1000, option: Optional[int] = None):
        """Start a miniticker websocket for all trades
        This is not in the official Binance api docs, but this is what
        feeds the right column on a ticker page on Binance.
        :param update_time: time between callbacks in milliseconds, must be 1000 or greater
        :type update_time: int
        :param option: base endpoint used, default 2 is data endpoint, 0 and 1 are the main endpoints
        :type option: int
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
        return self._get_socket(f'!miniTicker@arr@{update_time}ms', option)

    def trade_socket(self, symbol: str, option: Optional[int] = None):
        """Start a websocket for symbol trade data
        https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md#trade-streams
        :param symbol: required
        :type symbol: str
        :param option: base endpoint used, default 2 is data endpoint, 0 and 1 are the main endpoints
        :type option: int
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
        return self._get_socket(symbol.lower() + '@trade', option)

    def aggtrade_socket(self, symbol: str, option: Optional[int] = None):
        """Start a websocket for symbol trade data
        https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md#aggregate-trade-streams
        :param symbol: required
        :type symbol: str
        :param option: base endpoint used, default 2 is data endpoint, 0 and 1 are the main endpoints
        :type option: int
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
        return self._get_socket(symbol.lower() + '@aggTrade', option)

    def aggtrade_futures_socket(self, symbol: str, futures_type: FuturesType = FuturesType.USD_M):
        """Start a websocket for aggregate symbol trade data for the futures stream
        :param symbol: required
        :param futures_type: use USD-M or COIN-M futures default USD-M
        :returns: connection key string if successful, False otherwise
        Message Format
        .. code-block:: python
            {
                "e": "aggTrade",  // Event type
                "E": 123456789,   // Event time
                "s": "BTCUSDT",    // Symbol
                "a": 5933014,     // Aggregate trade ID
                "p": "0.001",     // Price
                "q": "100",       // Quantity
                "f": 100,         // First trade ID
                "l": 105,         // Last trade ID
                "T": 123456785,   // Trade time
                "m": true,        // Is the buyer the market maker?
            }
        """
        return self._get_futures_socket(symbol.lower() + '@aggTrade', futures_type=futures_type)

    def symbol_miniticker_socket(self, symbol: str, option: Optional[int] = None):
        """Start a websocket for a symbol's miniTicker data
                https://binance-docs.github.io/apidocs/spot/en/#individual-symbol-mini-ticker-stream
                :param symbol: required
                :type symbol: str
                :param option: base endpoint used, default 2 is data endpoint, 0 and 1 are the main endpoints
                :type option: int
                :returns: connection key string if successful, False otherwise
                Message Format
                .. code-block:: python
                    {
                        "e": "24hrMiniTicker",  // Event type
                        "E": 123456789,         // Event time
                        "s": "BNBBTC",          // Symbol
                        "c": "0.0025",          // Close price
                        "o": "0.0010",          // Open price
                        "h": "0.0025",          // High price
                        "l": "0.0010",          // Low price
                        "v": "10000",           // Total traded base asset volume
                        "q": "18"               // Total traded quote asset volume
                    }
                """
        return self._get_socket(symbol.lower() + '@miniTicker', option)

    def symbol_ticker_socket(self, symbol: str, option: Optional[int] = None):
        """Start a websocket for a symbol's ticker data
        https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md#individual-symbol-ticker-streams
        :param symbol: required
        :type symbol: str
        :param option: base endpoint used, default 2 is data endpoint, 0 and 1 are the main endpoints
        :type option: int
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
        return self._get_socket(symbol.lower() + '@ticker', option)

    def ticker_socket(self, option: Optional[int] = None):
        """Start a websocket for all ticker data
        By default all markets are included in an array.
        https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md#all-market-tickers-stream
        :param option: base endpoint used, default 2 is data endpoint, 0 and 1 are the main endpoints
        :type option: int
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
        return self._get_socket('!ticker@arr', option)

    def index_price_socket(self, symbol: str, fast: bool = True):
        """Start a websocket for a symbol's futures mark price
        https://binance-docs.github.io/apidocs/delivery/en/#index-price-stream
        :param symbol: required
        :param fast: use faster or 1s default
        :returns: connection key string if successful, False otherwise
        Message Format
        .. code-block:: python
            {
                "e": "indexPriceUpdate",  // Event type
                "E": 1591261236000,       // Event time
                "i": "BTCUSD",            // Pair
                "p": "9636.57860000",     // Index Price
              }
        """
        stream_name = '@indexPrice@1s' if fast else '@indexPrice'
        return self._get_futures_socket(symbol.lower() + stream_name, futures_type=FuturesType.COIN_M)

    def symbol_mark_price_socket(self, symbol: str, fast: bool = True, futures_type: FuturesType = FuturesType.USD_M):
        """Start a websocket for a symbol's futures mark price
        https://binance-docs.github.io/apidocs/futures/en/#mark-price-stream
        :param symbol: required
        :param fast: use faster or 1s default
        :param futures_type: use USD-M or COIN-M futures default USD-M
        :returns: connection key string if successful, False otherwise
        Message Format
        .. code-block:: python
            {
                "e": "markPriceUpdate",  // Event type
                "E": 1562305380000,      // Event time
                "s": "BTCUSDT",          // Symbol
                "p": "11185.87786614",   // Mark price
                "r": "0.00030000",       // Funding rate
                "T": 1562306400000       // Next funding time
            }
        """
        stream_name = '@markPrice@1s' if fast else '@markPrice'
        return self._get_futures_socket(symbol.lower() + stream_name, futures_type=futures_type)

    def all_mark_price_socket(self, fast: bool = True, futures_type: FuturesType = FuturesType.USD_M):
        """Start a websocket for all futures mark price data
        By default all symbols are included in an array.
        https://binance-docs.github.io/apidocs/futures/en/#mark-price-stream-for-all-market
        :param fast: use faster or 1s default
        :param futures_type: use USD-M or COIN-M futures default USD-M
        :returns: connection key string if successful, False otherwise
        Message Format
        .. code-block:: python
            [
                {
                    "e": "markPriceUpdate",  // Event type
                    "E": 1562305380000,      // Event time
                    "s": "BTCUSDT",          // Symbol
                    "p": "11185.87786614",   // Mark price
                    "r": "0.00030000",       // Funding rate
                    "T": 1562306400000       // Next funding time
                }
            ]
        """
        stream_name = '!markPrice@arr@1s' if fast else '!markPrice@arr'
        return self._get_futures_socket(stream_name, futures_type=futures_type)

    def symbol_ticker_futures_socket(self, symbol: str, futures_type: FuturesType = FuturesType.USD_M):
        """Start a websocket for a symbol's ticker data
        By default all markets are included in an array.
        https://binance-docs.github.io/apidocs/futures/en/#individual-symbol-book-ticker-streams
        :param symbol: required
        :param futures_type: use USD-M or COIN-M futures default USD-M
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
        return self._get_futures_socket(symbol.lower() + '@bookTicker', futures_type=futures_type)

    def individual_symbol_ticker_futures_socket(self, symbol: str, futures_type: FuturesType = FuturesType.USD_M):
        """Start a futures websocket for a single symbol's ticker data
        https://binance-docs.github.io/apidocs/futures/en/#individual-symbol-ticker-streams
        :param symbol: required
        :type symbol: str
        :param futures_type: use USD-M or COIN-M futures default USD-M
        :returns: connection key string if successful, False otherwise
        .. code-block:: python
            {
                "e": "24hrTicker",  // Event type
                "E": 123456789,     // Event time
                "s": "BTCUSDT",     // Symbol
                "p": "0.0015",      // Price change
            }
        """
        return self._get_futures_socket(symbol.lower() + '@ticker', futures_type=futures_type)

    def all_ticker_futures_socket(self, futures_type: FuturesType = FuturesType.USD_M):
        """Start a websocket for all ticker data
        By default all markets are included in an array.
        https://binance-docs.github.io/apidocs/futures/en/#all-book-tickers-stream
        :param futures_type: use USD-M or COIN-M futures default USD-M
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
        return self._get_futures_socket('!bookTicker', futures_type=futures_type)

    def symbol_book_ticker_socket(self, symbol: str, option: Optional[int] = None):
        """Start a websocket for the best bid or ask's price or quantity for a specified symbol.
        https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md#individual-symbol-book-ticker-streams
        :param symbol: required
        :type symbol: str
        :param option: base endpoint used, default 2 is data endpoint, 0 and 1 are the main endpoints
        :type option: int
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
        return self._get_socket(symbol.lower() + '@bookTicker', option)

    def book_ticker_socket(self, option: Optional[int] = None):
        """Start a websocket for the best bid or ask's price or quantity for all symbols.
        https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md#all-book-tickers-stream
        :param option: base endpoint used, default 2 is data endpoint, 0 and 1 are the main endpoints
        :type option: int
        :returns: connection key string if successful, False otherwise
        Message Format
        .. code-block:: python
            {
                // Same as <symbol>@bookTicker payload
            }
        """
        return self._get_socket('!bookTicker', option)

    def multiplex_socket(self, streams: List[str], option: Optional[int] = None):
        """Start a multiplexed socket using a list of socket names.
        User stream sockets can not be included.
        Symbols in socket name must be lowercase i.e bnbbtc@aggTrade, neobtc@ticker
        Combined stream events are wrapped as follows: {"stream":"<streamName>","data":<rawPayload>}
        https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md
        :param streams: list of stream names in lower case
        :type streams: list
        :param option: base endpoint used, default 2 is data endpoint, 0 and 1 are the main endpoints
        :type option: int
        :returns: connection key string if successful, False otherwise
        Message Format - see Binance API docs for all types
        """
        path = f'streams={"/".join(streams)}'
        return self._get_socket(path, option, prefix='stream?')

    def multiplex_socket_sbe(self, streams: List[str], option: Optional[int] = None):
        """Start a multiplexed socket using a list of socket names.
        User stream sockets can not be included.
        Symbols in socket name must be lowercase i.e bnbbtc@aggTrade, neobtc@ticker
        Combined stream events are wrapped as follows: {"stream":"<streamName>","data":<rawPayload>}
        https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md
        :param streams: list of stream names in lower case
        :type streams: list
        :param option: base endpoint used, default 2 is data endpoint, 0 and 1 are the main endpoints
        :type option: int
        :returns: connection key string if successful, False otherwise
        Message Format - see Binance API docs for all types
        """
        path = f'streams={"/".join(streams)}'
        return self._get_sbe_socket(path, option, prefix='stream?')

    def multiplex_socket_sbe_testnet(self, streams: List[str], option: Optional[int] = None):
        """Start a multiplexed socket using a list of socket names.
        User stream sockets can not be included.
        Symbols in socket name must be lowercase i.e bnbbtc@aggTrade, neobtc@ticker
        Combined stream events are wrapped as follows: {"stream":"<streamName>","data":<rawPayload>}
        https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md
        :param streams: list of stream names in lower case
        :type streams: list
        :param option: base endpoint used, default 2 is data endpoint, 0 and 1 are the main endpoints
        :type option: int
        :returns: connection key string if successful, False otherwise
        Message Format - see Binance API docs for all types
        """
        path = f'streams={"/".join(streams)}'
        return self._get_sbe_testnet_socket(path, option, prefix='stream?')

    def multiplex_socket_mus(self, streams: List[str], option: Optional[int] = None):
        """Start a multiplexed socket using a list of socket names.
        User stream sockets can not be included.
        Symbols in socket name must be lowercase i.e bnbbtc@aggTrade, neobtc@ticker
        Combined stream events are wrapped as follows: {"stream":"<streamName>","data":<rawPayload>}
        https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md
        :param streams: list of stream names in lower case
        :type streams: list
        :param option: base endpoint used, default 2 is data endpoint, 0 and 1 are the main endpoints
        :type option: int
        :returns: connection key string if successful, False otherwise
        Message Format - see Binance API docs for all types
        """
        path = f'streams={"/".join(streams)}&timeUnit=microsecond'
        return self._get_socket(path, option, prefix='stream?')

    def futures_multiplex_socket(self, streams: List[str], futures_type: FuturesType = FuturesType.USD_M):
        """Start a multiplexed socket using a list of socket names.
        User stream sockets can not be included.
        Symbols in socket name must be lowercase i.e bnbbtc@aggTrade, neobtc@ticker
        Combined stream events are wrapped as follows: {"stream":"<streamName>","data":<rawPayload>}
        https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md
        :param streams: list of stream names in lower case
        :param futures_type: use USD-M or COIN-M futures default USD-M
        :returns: connection key string if successful, False otherwise
        Message Format - see Binance API docs for all types
        """
        path = f'streams={"/".join(streams)}'
        return self._get_futures_socket(path, prefix='stream?', futures_type=futures_type)

    def user_socket(self, option: Optional[int] = None):
        """Start a websocket for user data
            https://github.com/binance-exchange/binance-official-api-docs/blob/master/user-data-stream.md
            https://binance-docs.github.io/apidocs/spot/en/#listen-key-spot
        :param option: base endpoint used, default 2 is data endpoint, 0 and 1 are the main endpoints
        :type option: int
        :returns: connection key string if successful, False otherwise
        Message Format - see Binance API docs for all types
        """
        # return self._get_account_socket('user', option)
        return self._get_account_socket(option)

    def margin_socket(self, option: Optional[int] = None):
        """Start a websocket for cross-margin data
        https://binance-docs.github.io/apidocs/spot/en/#listen-key-margin
        :param option: base endpoint used, default 2 is data endpoint, 0 and 1 are the main endpoints
        :type option: int
        :returns: connection key string if successful, False otherwise
        Message Format - see Binance API docs for all types
        """
        return self._get_account_socket('margin', option)

    def futures_socket(self):
        """Start a websocket for futures data
            https://binance-docs.github.io/apidocs/futures/en/#websocket-market-streams
        :returns: connection key string if successful, False otherwise
        Message Format - see Binance API docs for all types
        """
        return self._get_futures_account_socket('futures')

    def coin_futures_socket(self):
        """Start a websocket for coin futures data
            https://binance-docs.github.io/apidocs/delivery/en/#websocket-market-streams
        :returns: connection key string if successful, False otherwise
        Message Format - see Binance API docs for all types
        """
        return self._get_futures_account_socket('coin_futures')

    def isolated_margin_socket(self, symbol: str, option: Optional[int] = None):
        """Start a websocket for isolated margin data
        https://binance-docs.github.io/apidocs/spot/en/#listen-key-isolated-margin
        :param symbol: required - symbol for the isolated margin account
        :type symbol: str
        :param option: base endpoint used, default 2 is data endpoint, 0 and 1 are the main endpoints
        :type option: int
        :returns: connection key string if successful, False otherwise
        Message Format - see Binance API docs for all types
        """
        return self._get_account_socket(symbol, option)

    async def _stop_socket(self, conn_key):
        """Stop a websocket given the connection key
        :param conn_key: Socket connection key
        :type conn_key: string
        :returns: None
        """
        if conn_key not in self._conns:
            return

        del self._conns[conn_key]


class ThreadedWebsocketManager(ThreadedApiManager):

    def __init__(
            self, api_key: Optional[str] = None, api_secret: Optional[str] = None,
            requests_params: Dict[str, str] = None, tld: str = 'com', loop=None
    ):
        super().__init__(api_key, api_secret, requests_params, tld, loop)
        self._bsm: Optional[BinanceSocketManager] = None

    async def _before_socket_listener_start(self):
        assert self._client
        self._bsm = BinanceSocketManager(client=self._client, loop=self._loop)

    def _start_async_socket(
            self, callback: Callable, socket_name: str, params: Dict[str, Any], path: Optional[str] = None
    ) -> str:
        while not self._bsm:
            time.sleep(0.1)
        socket = getattr(self._bsm, socket_name)(**params)
        path = path or socket._path  # noqa
        self._socket_running[path] = True
        self._loop.call_soon_threadsafe(asyncio.create_task, self.start_listener(socket, socket._path, callback))
        return path

    def start_depth_socket(
            self, callback: Callable, symbol: str, depth: Optional[str] = None, interval: Optional[int] = None
    ) -> str:
        return self._start_async_socket(
            callback=callback,
            socket_name='depth_socket',
            params={
                'symbol': symbol,
                'depth': depth,
                'interval': interval,
            }
        )

    def start_kline_socket(self, callback: Callable, symbol: str, interval=KLINE_INTERVAL_1MINUTE) -> str:
        return self._start_async_socket(
            callback=callback,
            socket_name='kline_socket',
            params={
                'symbol': symbol,
                'interval': interval,
            }
        )

    def start_miniticker_socket(self, callback: Callable, update_time: int = 1000) -> str:
        return self._start_async_socket(
            callback=callback,
            socket_name='miniticker_socket',
            params={
                'update_time': update_time,
            }
        )

    def start_trade_socket(self, callback: Callable, symbol: str) -> str:
        return self._start_async_socket(
            callback=callback,
            socket_name='trade_socket',
            params={
                'symbol': symbol,
            }
        )

    def start_aggtrade_socket(self, callback: Callable, symbol: str) -> str:
        return self._start_async_socket(
            callback=callback,
            socket_name='aggtrade_socket',
            params={
                'symbol': symbol,
            }
        )

    def start_aggtrade_futures_socket(
            self, callback: Callable, symbol: str, futures_type: FuturesType = FuturesType.USD_M
    ) -> str:
        return self._start_async_socket(
            callback=callback,
            socket_name='aggtrade_futures_socket',
            params={
                'symbol': symbol,
                'futures_type': futures_type,
            }
        )

    def start_symbol_miniticker_socket(self, callback: Callable, symbol: str) -> str:
        return self._start_async_socket(
            callback=callback,
            socket_name='symbol_miniticker_socket',
            params={
                'symbol': symbol,
            }
        )

    def start_symbol_ticker_socket(self, callback: Callable, symbol: str) -> str:
        return self._start_async_socket(
            callback=callback,
            socket_name='symbol_ticker_socket',
            params={
                'symbol': symbol,
            }
        )

    def start_ticker_socket(self, callback: Callable) -> str:
        return self._start_async_socket(
            callback=callback,
            socket_name='ticker_socket',
            params={}
        )

    def start_index_price_socket(self, callback: Callable, symbol: str, fast: bool = True) -> str:
        return self._start_async_socket(
            callback=callback,
            socket_name='index_price_socket',
            params={
                'symbol': symbol,
                'fast': fast
            }
        )

    def start_symbol_mark_price_socket(
            self, callback: Callable, symbol: str, fast: bool = True, futures_type: FuturesType = FuturesType.USD_M
    ) -> str:
        return self._start_async_socket(
            callback=callback,
            socket_name='symbol_mark_price_socket',
            params={
                'symbol': symbol,
                'fast': fast,
                'futures_type': futures_type
            }
        )

    def start_all_mark_price_socket(
            self, callback: Callable, fast: bool = True, futures_type: FuturesType = FuturesType.USD_M
    ) -> str:
        return self._start_async_socket(
            callback=callback,
            socket_name='all_mark_price_socket',
            params={
                'fast': fast,
                'futures_type': futures_type
            }
        )

    def start_symbol_ticker_futures_socket(
            self, callback: Callable, symbol: str, futures_type: FuturesType = FuturesType.USD_M
    ) -> str:
        return self._start_async_socket(
            callback=callback,
            socket_name='symbol_ticker_futures_socket',
            params={
                'symbol': symbol,
                'futures_type': futures_type
            }
        )

    def start_individual_symbol_ticker_futures_socket(
            self, callback: Callable, symbol: str, futures_type: FuturesType = FuturesType.USD_M
    ) -> str:
        return self._start_async_socket(
            callback=callback,
            socket_name='individual_symbol_ticker_futures_socket',
            params={
                'symbol': symbol,
                'futures_type': futures_type
            }
        )

    def start_all_ticker_futures_socket(self, callback: Callable, futures_type: FuturesType = FuturesType.USD_M) -> str:
        return self._start_async_socket(
            callback=callback,
            socket_name='all_ticker_futures_socket',
            params={
                'futures_type': futures_type
            }
        )

    def start_symbol_book_ticker_socket(self, callback: Callable, symbol: str) -> str:
        return self._start_async_socket(
            callback=callback,
            socket_name='symbol_book_ticker_socket',
            params={
                'symbol': symbol
            }
        )

    def start_book_ticker_socket(self, callback: Callable) -> str:
        return self._start_async_socket(
            callback=callback,
            socket_name='book_ticker_socket',
            params={}
        )

    def start_multiplex_socket(self, callback: Callable, streams: List[str]) -> str:
        return self._start_async_socket(
            callback=callback,
            socket_name='multiplex_socket',
            params={
                'streams': streams
            }
        )

    def start_futures_multiplex_socket(
            self, callback: Callable, streams: List[str], futures_type: FuturesType = FuturesType.USD_M
    ) -> str:
        return self._start_async_socket(
            callback=callback,
            socket_name='futures_multiplex_socket',
            params={
                'streams': streams,
                'futures_type': futures_type
            }
        )

    def start_user_socket(self, callback: Callable) -> str:
        return self._start_async_socket(
            callback=callback,
            socket_name='user_socket',
            params={}
        )

    def start_margin_socket(self, callback: Callable) -> str:
        return self._start_async_socket(
            callback=callback,
            socket_name='margin_socket',
            params={}
        )

    def start_futures_socket(self, callback: Callable) -> str:
        return self._start_async_socket(
            callback=callback,
            socket_name='futures_socket',
            params={}
        )

    def start_coin_futures_socket(self, callback: Callable) -> str:
        return self._start_async_socket(
            callback=callback,
            socket_name='coin_futures_socket',
            params={}
        )

    def start_isolated_margin_socket(self, callback: Callable, symbol: str) -> str:
        return self._start_async_socket(
            callback=callback,
            socket_name='isolated_margin_socket',
            params={
                'symbol': symbol
            }
        )
