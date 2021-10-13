from typing import Dict, Optional, List, Tuple

import aiohttp
import asyncio
import hashlib
import hmac
import requests
import time
from operator import itemgetter
from urllib.parse import urlencode

from .helpers import interval_to_milliseconds, convert_ts_str
from .exceptions import BinanceAPIException, BinanceRequestException, NotImplementedException
from .enums import AGG_ID, HistoricalKlinesType

from yarl import URL


class BaseClient:
    BASE_API_URLS = ['https://api.binance.com',
                     'https://api1.binance.com',
                     'https://api2.binance.com',
                     'https://api3.binance.com']
    API_URL = 'https://api.binance.{}/api'
    API_TESTNET_URL = 'https://testnet.binance.vision/api'
    MARGIN_API_URL = 'https://api.binance.{}/sapi'
    WEBSITE_URL = 'https://www.binance.{}'
    FUTURES_URL = 'https://fapi.binance.{}/fapi'
    FUTURES_COIN_URL = 'https://dapi.binance.{}/dapi'
    PUBLIC_API_VERSION = 'v3'
    PRIVATE_API_VERSION = 'v3'
    MARGIN_API_VERSION = 'v1'
    FUTURES_API_VERSION = 'v1'
    FUTURES_COIN_API_VERSION = 'v1'

    REQUEST_TIMEOUT: float = 5

    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None, timestamp_offset: Optional[int] = None, requests_params: Dict = {}, tld='com'):
        """Binance API Client constructor
        :param api_key: Api Key
        :type api_key: str.
        :param api_secret: Api Secret
        :type api_secret: str.
        :param requests_params: optional - Dictionary of requests params to use for all calls
        :type requests_params: dict.
        """

        self.API_URL = self.API_URL.format(tld)
        self.MARGIN_API_URL = self.MARGIN_API_URL.format(tld)
        self.WEBSITE_URL = self.WEBSITE_URL.format(tld)
        self.FUTURES_URL = self.FUTURES_URL.format(tld)
        self.FUTURES_COIN_URL = self.FUTURES_COIN_URL.format(tld)

        self.API_KEY = api_key
        self.API_SECRET = api_secret
        self.session = self._init_session()
        self._requests_params = requests_params
        self.response = None
        self.timestamp_offset = 0 if timestamp_offset is None else timestamp_offset

        self.N_BASE_API_URLS = len(self.BASE_API_URLS)
        self.GET_EXCHANGE_INFO_URLS = [f'{base_url}/api/v3/exchangeInfo' for base_url in self.BASE_API_URLS]
        self.PING_URLS = [f'{base_url}/api/v3/ping' for base_url in self.BASE_API_URLS]
        self.GET_SERVER_TIME_URLS = [f'{base_url}/api/v3/time' for base_url in self.BASE_API_URLS]
        self.GET_ORDER_BOOK_URLS = [f'{base_url}/api/v3/depth' for base_url in self.BASE_API_URLS]
        self.GET_AGGREGATE_TRADES_URLS = [f'{base_url}/api/v3/aggTrades' for base_url in self.BASE_API_URLS]
        self.GET_AVG_PRICE_URLS = [f'{base_url}/api/v3/avgPrice' for base_url in self.BASE_API_URLS]
        self.GET_TICKER_URLS = [f'{base_url}/api/v3/ticker/24hr' for base_url in self.BASE_API_URLS]
        self.GET_ORDERBOOL_TICKER_URLS = [f'{base_url}/api/v3/ticker/bookTicker' for base_url in self.BASE_API_URLS]
        self.GET_ORDER_URLS = [f'{base_url}/api/v3/order' for base_url in self.BASE_API_URLS]
        self.GET_ALL_ORDERS_URLS = [f'{base_url}/api/v3/allOrders' for base_url in self.BASE_API_URLS]
        self.GET_OPEN_ORDERS_URLS = [f'{base_url}/api/v3/openOrders' for base_url in self.BASE_API_URLS]
        self.GET_ACCOUNT_URLS = [f'{base_url}/api/v3/account' for base_url in self.BASE_API_URLS]
        self.GET_MY_TRADES_URLS = [f'{base_url}/api/v3/myTrades' for base_url in self.BASE_API_URLS]
        self.CREATE_MARGIN_ORDER_URLS = [f'{base_url}/sapi/v1/margin/order' for base_url in self.BASE_API_URLS]

        self.base_api_url_location = 0
        self.base_api_url = self.BASE_API_URLS[0]
        self.get_exchange_info_url = self.GET_EXCHANGE_INFO_URLS[0]
        self.ping_url = self.PING_URLS[0]
        self.get_server_time_url = self.GET_SERVER_TIME_URLS[0]
        self.get_order_book_url = self.GET_ORDER_BOOK_URLS[0]
        self.get_aggregate_trades_url = self.GET_AGGREGATE_TRADES_URLS[0]
        self.get_avg_price_url = self.GET_AVG_PRICE_URLS[0]
        self.get_ticker_url = self.GET_TICKER_URLS[0]
        self.get_orderbook_ticker_url = self.GET_ORDERBOOL_TICKER_URLS[0]
        self.get_orderbook_tickers_url = self.get_orderbook_ticker_url
        self.get_order_url = self.GET_ORDER_URLS[0]
        self.create_order_url = self.get_order_url
        self.cancel_order_url = self.get_order_url
        self.get_all_orders_url = self.GET_ALL_ORDERS_URLS[0]
        self.get_open_orders_url = self.GET_OPEN_ORDERS_URLS[0]
        self.cancel_orders_url = self.get_open_orders_url
        self.get_account_url = self.GET_ACCOUNT_URLS[0]
        self.get_my_trades_url = self.GET_MY_TRADES_URLS[0]
        self.create_margin_order_url = self.CREATE_MARGIN_ORDER_URLS[0]

    def get_best_location(self, n_sample: int, timeout: float = REQUEST_TIMEOUT) -> int:
        total_elapseds = {i: 0 for i in range(self.N_BASE_API_URLS)}
        for _ in range(n_sample):
            for i in range(self.N_BASE_API_URLS):
                try:
                    response = requests.get(self.PING_URLS[i], timeout=timeout)
                except requests.exceptions.Timeout:
                    total_elapseds[i] += timeout
                else:
                    total_elapseds[i] += response.elapsed.total_seconds()
        min_elapsed = total_elapseds[self.N_BASE_API_URLS - 1]
        min_location = self.N_BASE_API_URLS - 1
        for i in range(self.N_BASE_API_URLS - 1):
            if total_elapseds[i] < min_elapsed:
                min_location = i
                min_elapsed = total_elapseds[i]
        return min_location

    async def async_get_best_location(self, n_sample: int, timeout: float = REQUEST_TIMEOUT) -> int:
        total_elapseds = {URL(url): 0 for url in self.PING_URLS}
        request_starts = {URL(url): False for url in self.PING_URLS}

        async def on_request_start(session, trace_config_ctx, params):
            total_elapseds[params.url] -= time.monotonic()
            request_starts[params.url] = True

        async def on_request_end(session, trace_config_ctx, params):
            total_elapseds[params.url] += time.monotonic()
            request_starts[params.url] = False

        async def add_ping_time(session, url):
            try:
                await session.get(url, timeout=timeout)
            except asyncio.TimeoutError:
                if request_starts[URL(url)]:
                    total_elapseds[URL(url)] += time.monotonic()
                    request_starts[URL(url)] = False
                else:
                    total_elapseds[URL(url)] += timeout

        trace_config = aiohttp.TraceConfig()
        trace_config.on_request_start.append(on_request_start)
        trace_config.on_request_end.append(on_request_end)

        async with aiohttp.ClientSession(trace_configs=[trace_config]) as client_session:
            for _ in range(n_sample):
                tasks = [add_ping_time(client_session, url) for url in self.PING_URLS]
                await asyncio.gather(*tasks)

        min_elapsed = total_elapseds[URL(self.PING_URLS[self.N_BASE_API_URLS - 1])]
        min_location = self.N_BASE_API_URLS - 1
        for i in range(self.N_BASE_API_URLS - 1):
            if total_elapseds[URL(self.PING_URLS[i])] < min_elapsed:
                min_location = i
                min_elapsed = total_elapseds[URL(self.PING_URLS[i])]
        return min_location

    def change_location(self, location: int) -> bool:
        if location != self.base_api_url_location:
            self.base_api_url_location = location
            self.base_api_url = self.BASE_API_URLS[location]
            self.get_exchange_info_url = self.GET_EXCHANGE_INFO_URLS[location]
            self.ping_url = self.PING_URLS[location]
            self.get_server_time_url = self.GET_SERVER_TIME_URLS[location]
            self.get_order_book_url = self.GET_ORDER_BOOK_URLS[location]
            self.get_aggregate_trades_url = self.GET_AGGREGATE_TRADES_URLS[location]
            self.get_avg_price_url = self.GET_AVG_PRICE_URLS[location]
            self.get_ticker_url = self.GET_TICKER_URLS[location]
            self.get_orderbook_ticker_url = self.GET_ORDERBOOL_TICKER_URLS[location]
            self.get_orderbook_tickers_url = self.get_orderbook_ticker_url
            self.get_order_url = self.GET_ORDER_URLS[location]
            self.create_order_url = self.get_order_url
            self.cancel_order_url = self.get_order_url
            self.get_all_orders_url = self.GET_ALL_ORDERS_URLS[location]
            self.get_open_orders_url = self.GET_OPEN_ORDERS_URLS[location]
            self.cancel_orders_url = self.get_open_orders_url
            self.get_account_url = self.GET_ACCOUNT_URLS[location]
            self.get_my_trades_url = self.GET_MY_TRADES_URLS[location]
            self.create_margin_order_url = self.CREATE_MARGIN_ORDER_URLS[location]

            self.API_URL = f'{self.base_api_url}/api'
            self.MARGIN_API_URL = f'{self.base_api_url}/sapi'
            return True
        else:
            return False

    def _get_headers(self) -> Dict:
        headers = {
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',  # noqa
        }
        if self.API_KEY:
            assert self.API_KEY
            headers['X-MBX-APIKEY'] = self.API_KEY
        return headers

    def _init_session(self):
        raise NotImplementedError

    def _create_api_uri(self, path, signed=True, version=None) -> str:
        if version is None:
            version = self.PUBLIC_API_VERSION
        v = self.PRIVATE_API_VERSION if signed else version
        return self.API_URL + '/' + v + '/' + path

    def _create_margin_api_uri(self, path):
        return self.MARGIN_API_URL + '/' + self.MARGIN_API_VERSION + '/' + path

    def _create_website_uri(self, path: str) -> str:
        return self.WEBSITE_URL + '/' + path

    def _create_futures_api_uri(self, path):
        return self.FUTURES_URL + '/' + self.FUTURES_API_VERSION + '/' + path

    def _create_tfutures_api_uri(self, path):
        return self.FUTURES_COIN_URL + '/' + self.FUTURES_COIN_API_VERSION + '/' + path

    def _generate_signature(self, data: Dict) -> str:
        ordered_data = self._order_params(data)
        query_string = '&'.join([f"{d[0]}={d[1]}" for d in ordered_data])
        m = hmac.new(self.API_SECRET.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256)
        return m.hexdigest()

    @staticmethod
    def _order_params(data: Dict) -> List[Tuple[str, str]]:
        """Convert params to list with signature as last element
        :param data:
        :return:
        """
        has_signature = False
        params = []
        for key, value in data.items():
            if key == 'signature':
                has_signature = True
            else:
                params.append((key, str(value)))
        # sort parameters by key
        params.sort(key=itemgetter(0))
        if has_signature:
            params.append(('signature', data['signature']))
        return params

    def _get_request_kwargs(self, method, signed: bool, force_params: bool = False, **kwargs) -> Dict:

        # set default requests timeout
        kwargs['timeout'] = self.REQUEST_TIMEOUT

        # add our global requests params
        if self._requests_params:
            kwargs.update(self._requests_params)

        data = kwargs.get('data', None)
        if data and isinstance(data, dict):
            kwargs['data'] = data

            # find any requests params passed and apply them
            if 'requests_params' in kwargs['data']:
                # merge requests params into kwargs
                kwargs.update(kwargs['data']['requests_params'])
                del (kwargs['data']['requests_params'])

        if signed:
            # generate signature
            kwargs['data']['timestamp'] = int(time.time() * 1000 + self.timestamp_offset)
            kwargs['data']['signature'] = self._generate_signature(kwargs['data'])

        # sort get and post params to match signature order
        if data:
            # sort post params and remove any arguments with values of None
            kwargs['data'] = self._order_params(kwargs['data'])
            # Remove any arguments with values of None.
            null_args = [i for i, (key, value) in enumerate(kwargs['data']) if value is None]
            for i in reversed(null_args):
                del kwargs['data'][i]

        # if get request assign data array to params value for requests lib
        if data and (method == 'get' or force_params):
            kwargs['params'] = '&'.join(f'{data[0]}={data[1]}' for data in kwargs['data'])
            del (kwargs['data'])

        return kwargs


class Client(BaseClient):

    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None, timestamp_offset: Optional[int] = None, requests_params: Dict = {}):
        super().__init__(api_key, api_secret, timestamp_offset, requests_params)
        # init DNS and SSL cert
        self.ping_fast()
        if timestamp_offset is None:
            self.reset_timestamp_offset()

    def _init_session(self) -> requests.Session:
        headers = self._get_headers()
        session = requests.session()
        session.headers.update(headers)
        return session

    def _request(self, method, uri: str, signed: bool, force_params: bool = False, **kwargs):
        kwargs = self._get_request_kwargs(method, signed, force_params, **kwargs)
        self.response = getattr(self.session, method)(uri, **kwargs)
        return self._handle_response(self.response)

    def _request_fast(self, method, uri: str, query_string: str, timeout: float = BaseClient.REQUEST_TIMEOUT):
        self.response = getattr(self.session, method)(uri, params=query_string, timeout=timeout)
        return self._handle_response(self.response)

    def _get_signed_fast(self, uri: str, query_string: str, timeout: float = BaseClient.REQUEST_TIMEOUT):
        if query_string:
            query_string += f'&timestamp={time.time() * 1000 + self.timestamp_offset:.0f}'
        else:
            query_string = f'timestamp={time.time() * 1000 + self.timestamp_offset:.0f}'
        m = hmac.new(self.API_SECRET.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256)
        self.response = self.session.get(uri, params=f'{query_string}&signature={m.hexdigest()}', timeout=timeout)
        return self._handle_response(self.response)

    def _other_signed_fast(self, method, uri: str, request_body: List[Tuple[str, str]], timeout: float = BaseClient.REQUEST_TIMEOUT):
        request_body.append(('timestamp', f'{time.time() * 1000 + self.timestamp_offset:.0f}'))
        query_string = '&'.join(f'{data[0]}={data[1]}' for data in request_body)
        m = hmac.new(self.API_SECRET.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256)
        request_body.append(('signature', m.hexdigest()))
        self.response = getattr(self.session, method)(uri, data=request_body, timeout=timeout)
        return self._handle_response(self.response)

    @staticmethod
    def _handle_response(response: requests.Response):
        """Internal helper for handling API responses from the Binance server.
        Raises the appropriate exceptions when necessary; otherwise, returns the
        response.
        """
        if not (200 <= response.status_code < 300):
            raise BinanceAPIException(response, response.status_code, response.text)
        try:
            return response.json()
        except ValueError:
            raise BinanceRequestException(f'Invalid Response: {response.text}')

    def _request_api(self, method, path: str, signed: bool = False, version=None, **kwargs):
        uri = self._create_api_uri(path, signed, version)
        return self._request(method, uri, signed, **kwargs)

    def _request_futures_api(self, method, path, signed=False, **kwargs) -> Dict:
        uri = self._create_futures_api_uri(path)

        return self._request(method, uri, signed, True, **kwargs)

    def _request_tfutures_api(self, method, path, signed=False, **kwargs):
        uri = self._create_tfutures_api_uri(path)
        return self._request(method, uri, signed, True, **kwargs)

    def _request_margin_api(self, method, path, signed=False, **kwargs) -> Dict:
        uri = self._create_margin_api_uri(path)
        return self._request(method, uri, signed, **kwargs)

    def _request_website(self, method, path, signed=False, **kwargs) -> Dict:
        uri = self._create_website_uri(path)
        return self._request(method, uri, signed, **kwargs)

    def _get(self, path, signed=False, version=None, **kwargs):
        return self._request_api('get', path, signed, version, **kwargs)

    def _post(self, path, signed=False, version=None, **kwargs) -> Dict:
        return self._request_api('post', path, signed, version, **kwargs)

    def _put(self, path, signed=False, version=None, **kwargs) -> Dict:
        return self._request_api('put', path, signed, version, **kwargs)

    def _delete(self, path, signed=False, version=None, **kwargs) -> Dict:
        return self._request_api('delete', path, signed, version, **kwargs)

    # Exchange Endpoints

    def get_products(self) -> Dict:
        """Return list of products currently listed on Binance
        Use get_exchange_info() call instead
        :returns: list - List of product dictionaries
        :raises: BinanceRequestException, BinanceAPIException
        """
        products = self._request_website('get', 'exchange-api/v1/public/asset-service/product/get-products')
        return products

    def get_exchange_info(self) -> Dict:
        """Return rate limits and list of symbols
        :returns: list - List of product dictionaries
        .. code-block:: python
            {
                "timezone": "UTC",
                "serverTime": 1508631584636,
                "rateLimits": [
                    {
                        "rateLimitType": "REQUESTS",
                        "interval": "MINUTE",
                        "limit": 1200
                    },
                    {
                        "rateLimitType": "ORDERS",
                        "interval": "SECOND",
                        "limit": 10
                    },
                    {
                        "rateLimitType": "ORDERS",
                        "interval": "DAY",
                        "limit": 100000
                    }
                ],
                "exchangeFilters": [],
                "symbols": [
                    {
                        "symbol": "ETHBTC",
                        "status": "TRADING",
                        "baseAsset": "ETH",
                        "baseAssetPrecision": 8,
                        "quoteAsset": "BTC",
                        "quotePrecision": 8,
                        "orderTypes": ["LIMIT", "MARKET"],
                        "icebergAllowed": false,
                        "filters": [
                            {
                                "filterType": "PRICE_FILTER",
                                "minPrice": "0.00000100",
                                "maxPrice": "100000.00000000",
                                "tickSize": "0.00000100"
                            }, {
                                "filterType": "LOT_SIZE",
                                "minQty": "0.00100000",
                                "maxQty": "100000.00000000",
                                "stepSize": "0.00100000"
                            }, {
                                "filterType": "MIN_NOTIONAL",
                                "minNotional": "0.00100000"
                            }
                        ]
                    }
                ]
            }
        :raises: BinanceRequestException, BinanceAPIException
        """

        return self._get('exchangeInfo')

    def get_exchange_info_fast(self, timeout: float = BaseClient.REQUEST_TIMEOUT) -> Dict:
        return self._request_fast('get', self.get_exchange_info_url, '', timeout)

    def get_symbol_info(self, symbol) -> Dict:
        """Return information about a symbol
        :param symbol: required e.g BNBBTC
        :type symbol: str
        :returns: Dict if found, None if not
        .. code-block:: python
            {
                "symbol": "ETHBTC",
                "status": "TRADING",
                "baseAsset": "ETH",
                "baseAssetPrecision": 8,
                "quoteAsset": "BTC",
                "quotePrecision": 8,
                "orderTypes": ["LIMIT", "MARKET"],
                "icebergAllowed": false,
                "filters": [
                    {
                        "filterType": "PRICE_FILTER",
                        "minPrice": "0.00000100",
                        "maxPrice": "100000.00000000",
                        "tickSize": "0.00000100"
                    }, {
                        "filterType": "LOT_SIZE",
                        "minQty": "0.00100000",
                        "maxQty": "100000.00000000",
                        "stepSize": "0.00100000"
                    }, {
                        "filterType": "MIN_NOTIONAL",
                        "minNotional": "0.00100000"
                    }
                ]
            }
        :raises: BinanceRequestException, BinanceAPIException
        """

        res = self.get_exchange_info()

        for item in res['symbols']:
            if item['symbol'] == symbol:
                return item

        return {}

    def get_symbol_info_fast(self, symbol: str, timeout: float = BaseClient.REQUEST_TIMEOUT) -> Dict:
        res = self.get_exchange_info_fast(timeout)

        for item in res['symbols']:
            if item['symbol'] == symbol:
                return item

        return {}

    # General Endpoints

    def ping(self) -> Dict:
        """Test connectivity to the Rest API.
        https://binance-docs.github.io/apidocs/spot/en/#test-connectivity
        :returns: Empty array
        .. code-block:: python
            {}
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._get('ping')

    def ping_fast(self, timeout: float = BaseClient.REQUEST_TIMEOUT) -> Dict:
        return self._request_fast('get', self.ping_url, '', timeout)

    def get_server_time(self) -> Dict:
        """Test connectivity to the Rest API and get the current server time.
        https://binance-docs.github.io/apidocs/spot/en/#check-server-time
        :returns: Current server time
        .. code-block:: python
            {
                "serverTime": 1499827319559
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._get('time')

    def get_server_time_fast(self, timeout: float = BaseClient.REQUEST_TIMEOUT) -> Dict:
        return self._request_fast('get', self.get_server_time_url, '', timeout)

    def reset_timestamp_offset(self):
        send_time_local = time.time_ns()
        receive_time_server = self.get_server_time_fast()
        self.timestamp_offset = -int((time.time_ns() + send_time_local) / 2000000.0) + receive_time_server['serverTime']

    # Market Data Endpoints

    def get_all_tickers(self) -> List[Dict[str, str]]:
        """Latest price for all symbols.
        https://binance-docs.github.io/apidocs/spot/en/#symbol-price-ticker
        :returns: List of market tickers
        .. code-block:: python
            [
                {
                    "symbol": "LTCBTC",
                    "price": "4.00000200"
                },
                {
                    "symbol": "ETHBTC",
                    "price": "0.07946600"
                }
            ]
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._get('ticker/price')

    def get_orderbook_tickers(self) -> Dict:
        """Best price/qty on the order book for all symbols.
        https://binance-docs.github.io/apidocs/spot/en/#symbol-order-book-ticker
        :param symbol: optional
        :type symbol: str
        :returns: List of order book market entries
        .. code-block:: python
            [
                {
                    "symbol": "LTCBTC",
                    "bidPrice": "4.00000000",
                    "bidQty": "431.00000000",
                    "askPrice": "4.00000200",
                    "askQty": "9.00000000"
                },
                {
                    "symbol": "ETHBTC",
                    "bidPrice": "0.07946700",
                    "bidQty": "9.00000000",
                    "askPrice": "100000.00000000",
                    "askQty": "1000.00000000"
                }
            ]
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._get('ticker/bookTicker')

    def get_orderbook_tickers_fast(self, timeout: float = BaseClient.REQUEST_TIMEOUT):
        return self._request_fast('get', self.get_orderbook_tickers_url, '', timeout)

    def get_order_book(self, **params) -> Dict:
        """Get the Order Book for the market
        https://binance-docs.github.io/apidocs/spot/en/#order-book
        :param symbol: required
        :type symbol: str
        :param limit:  Default 100; max 1000
        :type limit: int
        :returns: API response
        .. code-block:: python
            {
                "lastUpdateId": 1027024,
                "bids": [
                    [
                        "4.00000000",     # PRICE
                        "431.00000000",   # QTY
                        []                # Can be ignored
                    ]
                ],
                "asks": [
                    [
                        "4.00000200",
                        "12.00000000",
                        []
                    ]
                ]
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._get('depth', data=params)

    def get_order_book_fast(self, query_string: str, timeout: float = BaseClient.REQUEST_TIMEOUT) -> Dict:
        return self._request_fast('get', self.get_order_book_url, query_string, timeout)

    def get_recent_trades(self, **params) -> Dict:
        """Get recent trades (up to last 500).
        https://binance-docs.github.io/apidocs/spot/en/#recent-trades-list
        :param symbol: required
        :type symbol: str
        :param limit:  Default 500; max 500.
        :type limit: int
        :returns: API response
        .. code-block:: python
            [
                {
                    "id": 28457,
                    "price": "4.00000100",
                    "qty": "12.00000000",
                    "time": 1499865549590,
                    "isBuyerMaker": true,
                    "isBestMatch": true
                }
            ]
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._get('trades', data=params)

    def get_historical_trades(self, **params) -> Dict:
        """Get older trades.
        https://binance-docs.github.io/apidocs/spot/en/#old-trade-lookup
        :param symbol: required
        :type symbol: str
        :param limit:  Default 500; max 500.
        :type limit: int
        :param fromId:  TradeId to fetch from. Default gets most recent trades.
        :type fromId: str
        :returns: API response
        .. code-block:: python
            [
                {
                    "id": 28457,
                    "price": "4.00000100",
                    "qty": "12.00000000",
                    "time": 1499865549590,
                    "isBuyerMaker": true,
                    "isBestMatch": true
                }
            ]
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._get('historicalTrades', data=params)

    def get_aggregate_trades(self, **params) -> Dict:
        """Get compressed, aggregate trades. Trades that fill at the time,
        from the same order, with the same price will have the quantity aggregated.
        https://binance-docs.github.io/apidocs/spot/en/#compressed-aggregate-trades-list
        :param symbol: required
        :type symbol: str
        :param fromId:  ID to get aggregate trades from INCLUSIVE.
        :type fromId: str
        :param startTime: Timestamp in ms to get aggregate trades from INCLUSIVE.
        :type startTime: int
        :param endTime: Timestamp in ms to get aggregate trades until INCLUSIVE.
        :type endTime: int
        :param limit:  Default 500; max 500.
        :type limit: int
        :returns: API response
        .. code-block:: python
            [
                {
                    "a": 26129,         # Aggregate tradeId
                    "p": "0.01633102",  # Price
                    "q": "4.70443515",  # Quantity
                    "f": 27781,         # First tradeId
                    "l": 27781,         # Last tradeId
                    "T": 1498793709153, # Timestamp
                    "m": true,          # Was the buyer the maker?
                    "M": true           # Was the trade the best price match?
                }
            ]
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._get('aggTrades', data=params)

    def get_aggregate_trades_fast(self, query_string: str, timeout: float = BaseClient.REQUEST_TIMEOUT) -> Dict:
        return self._request_fast('get', self.get_aggregate_trades_url, query_string, timeout)

    def aggregate_trade_iter(self, symbol: str, start_str=None, last_id=None):
        """Iterate over aggregate trade data from (start_time or last_id) to
        the end of the history so far.
        If start_time is specified, start with the first trade after
        start_time. Meant to initialise a local cache of trade data.
        If last_id is specified, start with the trade after it. This is meant
        for updating a pre-existing local trade data cache.
        Only allows start_str or last_id—not both. Not guaranteed to work
        right if you're running more than one of these simultaneously. You
        will probably hit your rate limit.
        See dateparser docs for valid start and end string formats http://dateparser.readthedocs.io/en/latest/
        If using offset strings for dates add "UTC" to date string e.g. "now UTC", "11 hours ago UTC"
        :param symbol: Symbol string e.g. ETHBTC
        :type symbol: str
        :param start_str: Start date string in UTC format or timestamp in milliseconds. The iterator will
        return the first trade occurring later than this time.
        :type start_str: str|int
        :param last_id: aggregate trade ID of the last known aggregate trade.
        Not a regular trade ID. See https://binance-docs.github.io/apidocs/spot/en/#compressed-aggregate-trades-list
        :returns: an iterator of JSON objects, one per trade. The format of
        each object is identical to Client.aggregate_trades().
        :type last_id: int
        """
        if start_str is not None and last_id is not None:
            raise ValueError(
                'start_time and last_id may not be simultaneously specified.')

        # If there's no last_id, get one.
        if last_id is None:
            # Without a last_id, we actually need the first trade.  Normally,
            # we'd get rid of it. See the next loop.
            if start_str is None:
                trades = self.get_aggregate_trades(symbol=symbol, fromId=0)
            else:
                # The difference between startTime and endTime should be less
                # or equal than an hour and the result set should contain at
                # least one trade.
                start_ts = convert_ts_str(start_str)
                # If the resulting set is empty (i.e. no trades in that interval)
                # then we just move forward hour by hour until we find at least one
                # trade or reach present moment
                while True:
                    end_ts = start_ts + (60 * 60 * 1000)
                    trades = self.get_aggregate_trades(
                        symbol=symbol,
                        startTime=start_ts,
                        endTime=end_ts)
                    if len(trades) > 0:
                        break
                    # If we reach present moment and find no trades then there is
                    # nothing to iterate, so we're done
                    if end_ts > int(time.time() * 1000):
                        return
                    start_ts = end_ts
            for t in trades:
                yield t
            last_id = trades[-1][self.AGG_ID]

        while True:
            # There is no need to wait between queries, to avoid hitting the
            # rate limit. We're using blocking IO, and as long as we're the
            # only thread running calls like this, Binance will automatically
            # add the right delay time on their end, forcing us to wait for
            # data. That really simplifies this function's job. Binance is
            # fucking awesome.
            trades = self.get_aggregate_trades(symbol=symbol, fromId=last_id)
            # fromId=n returns a set starting with id n, but we already have
            # that one. So get rid of the first item in the result set.
            trades = trades[1:]
            if len(trades) == 0:
                return
            for t in trades:
                yield t
            last_id = trades[-1][self.AGG_ID]

    def get_klines(self, **params) -> Dict:
        """Kline/candlestick bars for a symbol. Klines are uniquely identified by their open time.
        https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data
        :param symbol: required
        :type symbol: str
        :param interval: -
        :type interval: str
        :param limit: - Default 500; max 500.
        :type limit: int
        :param startTime:
        :type startTime: int
        :param endTime:
        :type endTime: int
        :returns: API response
        .. code-block:: python
            [
                [
                    1499040000000,      # Open time
                    "0.01634790",       # Open
                    "0.80000000",       # High
                    "0.01575800",       # Low
                    "0.01577100",       # Close
                    "148976.11427815",  # Volume
                    1499644799999,      # Close time
                    "2434.19055334",    # Quote asset volume
                    308,                # Number of trades
                    "1756.87402397",    # Taker buy base asset volume
                    "28.46694368",      # Taker buy quote asset volume
                    "17928899.62484339" # Can be ignored
                ]
            ]
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._get('klines', data=params)

    def _klines(self, klines_type: HistoricalKlinesType = HistoricalKlinesType.SPOT, **params) -> Dict:
        """Get klines of spot (get_klines) or futures (futures_klines) endpoints.
        :param klines_type: Historical klines type: SPOT or FUTURES
        :type klines_type: HistoricalKlinesType
        :return: klines, see get_klines
        """
        if 'endTime' in params and not params['endTime']:
            del params['endTime']

        if HistoricalKlinesType.SPOT == klines_type:
            return self.get_klines(**params)
        elif HistoricalKlinesType.FUTURES == klines_type:
            return self.futures_klines(**params)
        else:
            raise NotImplementedException(klines_type)

    def _get_earliest_valid_timestamp(self, symbol, interval, klines_type: HistoricalKlinesType = HistoricalKlinesType.SPOT):
        """Get earliest valid open timestamp from Binance
        :param symbol: Name of symbol pair e.g BNBBTC
        :type symbol: str
        :param interval: Binance Kline interval
        :type interval: str
        :param klines_type: Historical klines type: SPOT or FUTURES
        :type klines_type: HistoricalKlinesType
        :return: first valid timestamp
        """
        kline = self._klines(
            klines_type=klines_type,
            symbol=symbol,
            interval=interval,
            limit=1,
            startTime=0,
            endTime=int(time.time() * 1000)
        )
        return kline[0][0]

    def get_historical_klines(self, symbol, interval, start_str, end_str=None, limit=500,
                              klines_type: HistoricalKlinesType = HistoricalKlinesType.SPOT):
        """Get Historical Klines from Binance
        :param symbol: Name of symbol pair e.g BNBBTC
        :type symbol: str
        :param interval: Binance Kline interval
        :type interval: str
        :param start_str: Start date string in UTC format or timestamp in milliseconds
        :type start_str: str|int
        :param end_str: optional - end date string in UTC format or timestamp in milliseconds (default will fetch everything up to now)
        :type end_str: str|int
        :param limit: Default 500; max 1000.
        :type limit: int
        :param klines_type: Historical klines type: SPOT or FUTURES
        :type klines_type: HistoricalKlinesType
        :return: list of OHLCV values
        """
        return self._historical_klines(symbol, interval, start_str, end_str=end_str, limit=limit, klines_type=klines_type)

    def _historical_klines(self, symbol, interval, start_str, end_str=None, limit=500,
                           klines_type: HistoricalKlinesType = HistoricalKlinesType.SPOT):
        """Get Historical Klines from Binance (spot or futures)
        See dateparser docs for valid start and end string formats http://dateparser.readthedocs.io/en/latest/
        If using offset strings for dates add "UTC" to date string e.g. "now UTC", "11 hours ago UTC"
        :param symbol: Name of symbol pair e.g BNBBTC
        :type symbol: str
        :param interval: Binance Kline interval
        :type interval: str
        :param start_str: Start date string in UTC format or timestamp in milliseconds
        :type start_str: str|int
        :param end_str: optional - end date string in UTC format or timestamp in milliseconds (default will fetch everything up to now)
        :type end_str: None|str|int
        :param limit: Default 500; max 1000.
        :type limit: int
        :param limit: Default 500; max 1000.
        :type limit: int
        :param klines_type: Historical klines type: SPOT or FUTURES
        :type klines_type: HistoricalKlinesType
        :return: list of OHLCV values
        """
        # init our list
        output_data = []

        # convert interval to useful value in seconds
        timeframe = interval_to_milliseconds(interval)

        start_ts = convert_ts_str(start_str)

        # establish first available start timestamp
        first_valid_ts = self._get_earliest_valid_timestamp(symbol, interval, klines_type)
        start_ts = max(start_ts, first_valid_ts)

        # if an end time was passed convert it
        end_ts = convert_ts_str(end_str)

        idx = 0
        while True:
            # fetch the klines from start_ts up to max 500 entries or the end_ts if set
            temp_data = self._klines(
                klines_type=klines_type,
                symbol=symbol,
                interval=interval,
                limit=limit,
                startTime=start_ts,
                endTime=end_ts
            )

            # handle the case where exactly the limit amount of data was returned last loop
            if not len(temp_data):
                break

            # append this loops data to our output data
            output_data += temp_data

            # set our start timestamp using the last value in the array
            start_ts = temp_data[-1][0]

            idx += 1
            # check if we received less than the required limit and exit the loop
            if len(temp_data) < limit:
                # exit the while loop
                break

            # increment next call by our timeframe
            start_ts += timeframe

            # sleep after every 3rd call to be kind to the API
            if idx % 3 == 0:
                time.sleep(1)

        return output_data

    def get_historical_klines_generator(self, symbol, interval, start_str, end_str=None,
                                        klines_type: HistoricalKlinesType = HistoricalKlinesType.SPOT):
        """Get Historical Klines generator from Binance
        :param symbol: Name of symbol pair e.g BNBBTC
        :type symbol: str
        :param interval: Binance Kline interval
        :type interval: str
        :param start_str: Start date string in UTC format or timestamp in milliseconds
        :type start_str: str|int
        :param end_str: optional - end date string in UTC format or timestamp in milliseconds (default will fetch everything up to now)
        :type end_str: str|int
        :param klines_type: Historical klines type: SPOT or FUTURES
        :type klines_type: HistoricalKlinesType
        :return: generator of OHLCV values
        """

        return self._historical_klines_generator(symbol, interval, start_str, end_str=end_str, klines_type=klines_type)

    def _historical_klines_generator(self, symbol, interval, start_str, end_str=None,
                                     klines_type: HistoricalKlinesType = HistoricalKlinesType.SPOT):
        """Get Historical Klines generator from Binance (spot or futures)
        See dateparser docs for valid start and end string formats http://dateparser.readthedocs.io/en/latest/
        If using offset strings for dates add "UTC" to date string e.g. "now UTC", "11 hours ago UTC"
        :param symbol: Name of symbol pair e.g BNBBTC
        :type symbol: str
        :param interval: Binance Kline interval
        :type interval: str
        :param start_str: Start date string in UTC format or timestamp in milliseconds
        :type start_str: str|int
        :param end_str: optional - end date string in UTC format or timestamp in milliseconds (default will fetch everything up to now)
        :type end_str: str|int
        :param klines_type: Historical klines type: SPOT or FUTURES
        :type klines_type: HistoricalKlinesType
        :return: generator of OHLCV values
        """
        # setup the max limit
        limit = 500

        # convert interval to useful value in seconds
        timeframe = interval_to_milliseconds(interval)

        # convert our date strings to milliseconds
        start_ts = convert_ts_str(start_str)

        # establish first available start timestamp
        first_valid_ts = self._get_earliest_valid_timestamp(symbol, interval, klines_type)
        start_ts = max(start_ts, first_valid_ts)

        # if an end time was passed convert it
        end_ts = convert_ts_str(end_str)

        idx = 0
        while True:
            # fetch the klines from start_ts up to max 500 entries or the end_ts if set
            output_data = self._klines(
                klines_type=klines_type,
                symbol=symbol,
                interval=interval,
                limit=limit,
                startTime=start_ts,
                endTime=end_ts
            )

            # handle the case where exactly the limit amount of data was returned last loop
            if not len(output_data):
                break

            # yield data
            for o in output_data:
                yield o

            # set our start timestamp using the last value in the array
            start_ts = output_data[-1][0]

            idx += 1
            # check if we received less than the required limit and exit the loop
            if len(output_data) < limit:
                # exit the while loop
                break

            # increment next call by our timeframe
            start_ts += timeframe

            # sleep after every 3rd call to be kind to the API
            if idx % 3 == 0:
                time.sleep(1)

    def get_avg_price(self, **params) -> Dict:
        """Current average price for a symbol.
        https://binance-docs.github.io/apidocs/spot/en/#current-average-price
        :param symbol:
        :type symbol: str
        :returns: API response
        .. code-block:: python
            {
                "mins": 5,
                "price": "9.35751834"
            }
        """
        return self._get('avgPrice', data=params, version=self.PRIVATE_API_VERSION)

    def get_avg_price_fast(self, query_string: str, timeout: float = BaseClient.REQUEST_TIMEOUT) -> Dict:
        return self._request_fast('get', self.get_avg_price_url, query_string, timeout)

    def get_ticker(self, **params):
        """24 hour price change statistics.
        https://binance-docs.github.io/apidocs/spot/en/#24hr-ticker-price-change-statistics
        :param symbol:
        :type symbol: str
        :returns: API response
        .. code-block:: python
            {
                "priceChange": "-94.99999800",
                "priceChangePercent": "-95.960",
                "weightedAvgPrice": "0.29628482",
                "prevClosePrice": "0.10002000",
                "lastPrice": "4.00000200",
                "bidPrice": "4.00000000",
                "askPrice": "4.00000200",
                "openPrice": "99.00000000",
                "highPrice": "100.00000000",
                "lowPrice": "0.10000000",
                "volume": "8913.30000000",
                "openTime": 1499783499040,
                "closeTime": 1499869899040,
                "fristId": 28385,   # First tradeId
                "lastId": 28460,    # Last tradeId
                "count": 76         # Trade count
            }
        OR
        .. code-block:: python
            [
                {
                    "priceChange": "-94.99999800",
                    "priceChangePercent": "-95.960",
                    "weightedAvgPrice": "0.29628482",
                    "prevClosePrice": "0.10002000",
                    "lastPrice": "4.00000200",
                    "bidPrice": "4.00000000",
                    "askPrice": "4.00000200",
                    "openPrice": "99.00000000",
                    "highPrice": "100.00000000",
                    "lowPrice": "0.10000000",
                    "volume": "8913.30000000",
                    "openTime": 1499783499040,
                    "closeTime": 1499869899040,
                    "fristId": 28385,   # First tradeId
                    "lastId": 28460,    # Last tradeId
                    "count": 76         # Trade count
                }
            ]
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._get('ticker/24hr', data=params)

    def get_ticker_fast(self, query_string: str, timeout: float = BaseClient.REQUEST_TIMEOUT):
        return self._request_fast('get', self.get_ticker_url, query_string, timeout)

    def get_symbol_ticker(self, **params):
        """Latest price for a symbol or symbols.
        https://binance-docs.github.io/apidocs/spot/en/#symbol-price-ticker
        :param symbol:
        :type symbol: str
        :returns: API response
        .. code-block:: python
            {
                "symbol": "LTCBTC",
                "price": "4.00000200"
            }
        OR
        .. code-block:: python
            [
                {
                    "symbol": "LTCBTC",
                    "price": "4.00000200"
                },
                {
                    "symbol": "ETHBTC",
                    "price": "0.07946600"
                }
            ]
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._get('ticker/price', data=params)

    def get_orderbook_ticker(self, **params):
        """Latest price for a symbol or symbols.
        https://binance-docs.github.io/apidocs/spot/en/#symbol-order-book-ticker
        :param symbol:
        :type symbol: str
        :returns: API response
        .. code-block:: python
            {
                "symbol": "LTCBTC",
                "bidPrice": "4.00000000",
                "bidQty": "431.00000000",
                "askPrice": "4.00000200",
                "askQty": "9.00000000"
            }
        OR
        .. code-block:: python
            [
                {
                    "symbol": "LTCBTC",
                    "bidPrice": "4.00000000",
                    "bidQty": "431.00000000",
                    "askPrice": "4.00000200",
                    "askQty": "9.00000000"
                },
                {
                    "symbol": "ETHBTC",
                    "bidPrice": "0.07946700",
                    "bidQty": "9.00000000",
                    "askPrice": "100000.00000000",
                    "askQty": "1000.00000000"
                }
            ]
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._get('ticker/bookTicker', data=params)

    def get_orderbook_ticker_fast(self, query_string: str, timeout: float = BaseClient.REQUEST_TIMEOUT):
        return self._request_fast('get', self.get_orderbook_ticker_url, query_string, timeout)

    # Account Endpoints

    def create_order(self, **params):
        """Send in a new order
        Any order with an icebergQty MUST have timeInForce set to GTC.
        https://binance-docs.github.io/apidocs/spot/en/#new-order-trade
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
                "symbol":"LTCBTC",
                "orderId": 1,
                "clientOrderId": "myOrder1" # Will be newClientOrderId
                "transactTime": 1499827319559
            }
        Response RESULT:
        .. code-block:: python
            {
                "symbol": "BTCUSDT",
                "orderId": 28,
                "clientOrderId": "6gCrw2kRUAF9CvJDGP16IP",
                "transactTime": 1507725176595,
                "price": "0.00000000",
                "origQty": "10.00000000",
                "executedQty": "10.00000000",
                "status": "FILLED",
                "timeInForce": "GTC",
                "type": "MARKET",
                "side": "SELL"
            }
        Response FULL:
        .. code-block:: python
            {
                "symbol": "BTCUSDT",
                "orderId": 28,
                "clientOrderId": "6gCrw2kRUAF9CvJDGP16IP",
                "transactTime": 1507725176595,
                "price": "0.00000000",
                "origQty": "10.00000000",
                "executedQty": "10.00000000",
                "status": "FILLED",
                "timeInForce": "GTC",
                "type": "MARKET",
                "side": "SELL",
                "fills": [
                    {
                        "price": "4000.00000000",
                        "qty": "1.00000000",
                        "commission": "4.00000000",
                        "commissionAsset": "USDT"
                    },
                    {
                        "price": "3999.00000000",
                        "qty": "5.00000000",
                        "commission": "19.99500000",
                        "commissionAsset": "USDT"
                    },
                    {
                        "price": "3998.00000000",
                        "qty": "2.00000000",
                        "commission": "7.99600000",
                        "commissionAsset": "USDT"
                    },
                    {
                        "price": "3997.00000000",
                        "qty": "1.00000000",
                        "commission": "3.99700000",
                        "commissionAsset": "USDT"
                    },
                    {
                        "price": "3995.00000000",
                        "qty": "1.00000000",
                        "commission": "3.99500000",
                        "commissionAsset": "USDT"
                    }
                ]
            }
        :raises: BinanceRequestException, BinanceAPIException, BinanceOrderException, BinanceOrderMinAmountException, BinanceOrderMinPriceException, BinanceOrderMinTotalException, BinanceOrderUnknownSymbolException, BinanceOrderInactiveSymbolException
        """
        return self._post('order', True, data=params)

    def create_order_fast(self, request_body: List[Tuple[str, str]], timeout: float = BaseClient.REQUEST_TIMEOUT):
        return self._other_signed_fast('post', self.create_order_url, request_body, timeout)

    def create_oco_order(self, **params):
        """Send in a new OCO order
        https://binance-docs.github.io/apidocs/spot/en/#new-oco-trade
        :param symbol: required
        :type symbol: str
        :param listClientOrderId: A unique id for the list order. Automatically generated if not sent.
        :type listClientOrderId: str
        :param side: required
        :type side: str
        :param quantity: required
        :type quantity: decimal
        :param limitClientOrderId: A unique id for the limit order. Automatically generated if not sent.
        :type limitClientOrderId: str
        :param price: required
        :type price: str
        :param limitIcebergQty: Used to make the LIMIT_MAKER leg an iceberg order.
        :type limitIcebergQty: decimal
        :param stopClientOrderId: A unique id for the stop order. Automatically generated if not sent.
        :type stopClientOrderId: str
        :param stopPrice: required
        :type stopPrice: str
        :param stopLimitPrice: If provided, stopLimitTimeInForce is required.
        :type stopLimitPrice: str
        :param stopIcebergQty: Used with STOP_LOSS_LIMIT leg to make an iceberg order.
        :type stopIcebergQty: decimal
        :param stopLimitTimeInForce: Valid values are GTC/FOK/IOC.
        :type stopLimitTimeInForce: str
        :param newOrderRespType: Set the response JSON. ACK, RESULT, or FULL; default: RESULT.
        :type newOrderRespType: str
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        Response ACK:
        .. code-block:: python
            {
            }
        Response RESULT:
        .. code-block:: python
            {
            }
        Response FULL:
        .. code-block:: python
            {
            }
        :raises: BinanceRequestException, BinanceAPIException, BinanceOrderException, BinanceOrderMinAmountException, BinanceOrderMinPriceException, BinanceOrderMinTotalException, BinanceOrderUnknownSymbolException, BinanceOrderInactiveSymbolException
        """
        return self._post('order/oco', True, data=params)

    def create_test_order(self, **params):
        """Test new order creation and signature/recvWindow long. Creates and validates a new order but does not send it into the matching engine.
        https://binance-docs.github.io/apidocs/spot/en/#test-new-order-trade
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
        :param price: required
        :type price: str
        :param newClientOrderId: A unique id for the order. Automatically generated if not sent.
        :type newClientOrderId: str
        :param icebergQty: Used with iceberg orders
        :type icebergQty: decimal
        :param newOrderRespType: Set the response JSON. ACK, RESULT, or FULL; default: RESULT.
        :type newOrderRespType: str
        :param recvWindow: The number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            {}
        :raises: BinanceRequestException, BinanceAPIException, BinanceOrderException, BinanceOrderMinAmountException, BinanceOrderMinPriceException, BinanceOrderMinTotalException, BinanceOrderUnknownSymbolException, BinanceOrderInactiveSymbolException
        """
        return self._post('order/test', True, data=params)

    def get_order(self, **params):
        """Check an order's status. Either orderId or origClientOrderId must be sent.
        https://binance-docs.github.io/apidocs/spot/en/#query-order-user_data
        :param symbol: required
        :type symbol: str
        :param orderId: The unique order id
        :type orderId: int
        :param origClientOrderId: optional
        :type origClientOrderId: str
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            {
                "symbol": "LTCBTC",
                "orderId": 1,
                "clientOrderId": "myOrder1",
                "price": "0.1",
                "origQty": "1.0",
                "executedQty": "0.0",
                "status": "NEW",
                "timeInForce": "GTC",
                "type": "LIMIT",
                "side": "BUY",
                "stopPrice": "0.0",
                "icebergQty": "0.0",
                "time": 1499827319559
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._get('order', True, data=params)

    def get_order_fast(self, query_string: str, timeout: float = BaseClient.REQUEST_TIMEOUT):
        return self._get_signed_fast(self.get_order_url, query_string, timeout)

    def get_all_orders(self, **params):
        """Get all account orders; active, canceled, or filled.
        https://binance-docs.github.io/apidocs/spot/en/#all-orders-user_data
        :param symbol: required
        :type symbol: str
        :param orderId: The unique order id
        :type orderId: int
        :param startTime: optional
        :type startTime: int
        :param endTime: optional
        :type endTime: int
        :param limit: Default 500; max 500.
        :type limit: int
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            [
                {
                    "symbol": "LTCBTC",
                    "orderId": 1,
                    "clientOrderId": "myOrder1",
                    "price": "0.1",
                    "origQty": "1.0",
                    "executedQty": "0.0",
                    "status": "NEW",
                    "timeInForce": "GTC",
                    "type": "LIMIT",
                    "side": "BUY",
                    "stopPrice": "0.0",
                    "icebergQty": "0.0",
                    "time": 1499827319559
                }
            ]
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._get('allOrders', True, data=params)

    def get_all_orders_fast(self, query_string: str, timeout: float = BaseClient.REQUEST_TIMEOUT):
        return self._get_signed_fast(self.get_all_orders_url, query_string, timeout)

    def cancel_order(self, **params):
        """Cancel an active order. Either orderId or origClientOrderId must be sent.
        https://binance-docs.github.io/apidocs/spot/en/#cancel-order-trade
        :param symbol: required
        :type symbol: str
        :param orderId: The unique order id
        :type orderId: int
        :param origClientOrderId: optional
        :type origClientOrderId: str
        :param newClientOrderId: Used to uniquely identify this cancel. Automatically generated by default.
        :type newClientOrderId: str
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            {
                "symbol": "LTCBTC",
                "origClientOrderId": "myOrder1",
                "orderId": 1,
                "clientOrderId": "cancelMyOrder1"
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._delete('order', True, data=params)

    def cancel_order_fast(self, request_body: List[Tuple[str, str]], timeout: float = BaseClient.REQUEST_TIMEOUT):
        return self._other_signed_fast('delete', self.cancel_order_url, request_body, timeout)

    def get_open_orders(self, **params):
        """Get all open orders on a symbol.
        https://binance-docs.github.io/apidocs/spot/en/#current-open-orders-user_data
        :param symbol: optional
        :type symbol: str
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            [
                {
                    "symbol": "LTCBTC",
                    "orderId": 1,
                    "clientOrderId": "myOrder1",
                    "price": "0.1",
                    "origQty": "1.0",
                    "executedQty": "0.0",
                    "status": "NEW",
                    "timeInForce": "GTC",
                    "type": "LIMIT",
                    "side": "BUY",
                    "stopPrice": "0.0",
                    "icebergQty": "0.0",
                    "time": 1499827319559
                }
            ]
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._get('openOrders', True, data=params)

    def get_open_orders_fast(self, query_string: str, timeout: float = BaseClient.REQUEST_TIMEOUT):
        return self._get_signed_fast(self.get_open_orders_url, query_string, timeout)

    def cancel_oco(self, **params):
        """Cancel an entire Order List
        https://github.com/binance-exchange/binance-official-api-docs/blob/master/rest-api.md#cancel-oco-trade
        :param symbol: required
        :type symbol: str
        :param orderListId
        :type orderListId: long
        :param listClientOrderId
        :type listClientOrderId: str
        :param newClientOrderId
        :type newClientOrderId: str
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int

        :returns: API response

        .. code-block:: python

            {
              "orderListId": 0,
              "contingencyType": "OCO",
              "listStatusType": "ALL_DONE",
              "listOrderStatus": "ALL_DONE",
              "listClientOrderId": "C3wyj4WVEktd7u9aVBRXcN",
              "transactionTime": 1574040868128,
              "symbol": "LTCBTC",
              "orders": [
                {
                  "symbol": "LTCBTC",
                  "orderId": 2,
                  "clientOrderId": "pO9ufTiFGg3nw2fOdgeOXa"
                },
                {
                  "symbol": "LTCBTC",
                  "orderId": 3,
                  "clientOrderId": "TXOvglzXuaubXAaENpaRCB"
                }
              ],
              "orderReports": [
                {
                  "symbol": "LTCBTC",
                  "origClientOrderId": "pO9ufTiFGg3nw2fOdgeOXa",
                  "orderId": 2,
                  "orderListId": 0,
                  "clientOrderId": "unfWT8ig8i0uj6lPuYLez6",
                  "price": "1.00000000",
                  "origQty": "10.00000000",
                  "executedQty": "0.00000000",
                  "cummulativeQuoteQty": "0.00000000",
                  "status": "CANCELED",
                  "timeInForce": "GTC",
                  "type": "STOP_LOSS_LIMIT",
                  "side": "SELL",
                  "stopPrice": "1.00000000"
                },
                {
                  "symbol": "LTCBTC",
                  "origClientOrderId": "TXOvglzXuaubXAaENpaRCB",
                  "orderId": 3,
                  "orderListId": 0,
                  "clientOrderId": "unfWT8ig8i0uj6lPuYLez6",
                  "price": "3.00000000",
                  "origQty": "10.00000000",
                  "executedQty": "0.00000000",
                  "cummulativeQuoteQty": "0.00000000",
                  "status": "CANCELED",
                  "timeInForce": "GTC",
                  "type": "LIMIT_MAKER",
                  "side": "SELL"
                }
              ]
            }

        :raises: BinanceRequestException, BinanceAPIException

        """
        return self._delete('orderList', True, data=params)

    def get_oco(self, **params):
        """Retrieves a specific OCO based on provided optional parameters
        https://github.com/binance-exchange/binance-official-api-docs/blob/master/rest-api.md#query-oco-user_data
        :param orderListId
        :type orderListId: long
        :param origClientOrderId
        :type origClientOrderId: str
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            {
              "orderListId": 27,
              "contingencyType": "OCO",
              "listStatusType": "EXEC_STARTED",
              "listOrderStatus": "EXECUTING",
              "listClientOrderId": "h2USkA5YQpaXHPIrkd96xE",
              "transactionTime": 1565245656253,
              "symbol": "LTCBTC",
              "orders": [
                {
                  "symbol": "LTCBTC",
                  "orderId": 4,
                  "clientOrderId": "qD1gy3kc3Gx0rihm9Y3xwS"
                },
                {
                  "symbol": "LTCBTC",
                  "orderId": 5,
                  "clientOrderId": "ARzZ9I00CPM8i3NhmU9Ega"
                }
              ]
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._get('orderList', True, data=params)

    def get_all_ocos(self, **params):
        """Retrieves all OCO based on provided optional parameters
        https://github.com/binance-exchange/binance-official-api-docs/blob/master/rest-api.md#query-all-oco-user_data
        :param fromId
        :type fromId: long
        :param startTime
        :type startTime: long
        :param endTime
        :type endTime: long
        :param limit
        :type limit: int
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            [
              {
                "orderListId": 29,
                "contingencyType": "OCO",
                "listStatusType": "EXEC_STARTED",
                "listOrderStatus": "EXECUTING",
                "listClientOrderId": "amEEAXryFzFwYF1FeRpUoZ",
                "transactionTime": 1565245913483,
                "symbol": "LTCBTC",
                "orders": [
                  {
                    "symbol": "LTCBTC",
                    "orderId": 4,
                    "clientOrderId": "oD7aesZqjEGlZrbtRpy5zB"
                  },
                  {
                    "symbol": "LTCBTC",
                    "orderId": 5,
                    "clientOrderId": "Jr1h6xirOxgeJOUuYQS7V3"
                  }
                ]
              },
              {
                "orderListId": 28,
                "contingencyType": "OCO",
                "listStatusType": "EXEC_STARTED",
                "listOrderStatus": "EXECUTING",
                "listClientOrderId": "hG7hFNxJV6cZy3Ze4AUT4d",
                "transactionTime": 1565245913407,
                "symbol": "LTCBTC",
                "orders": [
                  {
                    "symbol": "LTCBTC",
                    "orderId": 2,
                    "clientOrderId": "j6lFOfbmFMRjTYA7rRJ0LP"
                  },
                  {
                    "symbol": "LTCBTC",
                    "orderId": 3,
                    "clientOrderId": "z0KCjOdditiLS5ekAFtK81"
                  }
                ]
              }
            ]
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._get('allOrderList', True, data=params)

    def get_open_ocos(self, **params):
        """Retrieves all OCO based on provided optional parameters
        https://github.com/binance-exchange/binance-official-api-docs/blob/master/rest-api.md#query-open-oco-user_data
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            [
              {
                "orderListId": 31,
                "contingencyType": "OCO",
                "listStatusType": "EXEC_STARTED",
                "listOrderStatus": "EXECUTING",
                "listClientOrderId": "wuB13fmulKj3YjdqWEcsnp",
                "transactionTime": 1565246080644,
                "symbol": "1565246079109",
                "orders": [
                  {
                    "symbol": "LTCBTC",
                    "orderId": 4,
                    "clientOrderId": "r3EH2N76dHfLoSZWIUw1bT"
                  },
                  {
                    "symbol": "LTCBTC",
                    "orderId": 5,
                    "clientOrderId": "Cv1SnyPD3qhqpbjpYEHbd2"
                  }
                ]
              }
            ]
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._get('openOrderList', True, data=params)

    def cancel_orders(self, **params):
        """Cancels all active orders on a symbol. This includes OCO orders.

        https://github.com/binance-exchange/binance-official-api-docs/blob/master/rest-api.md#cancel-all-open-orders-on-a-symbol-trade

        :param symbol: required
        :type symbol: str
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int

        :returns: API response

        .. code-block:: python

            [
              {
                "symbol": "BTCUSDT",
                "origClientOrderId": "E6APeyTJvkMvLMYMqu1KQ4",
                "orderId": 11,
                "orderListId": -1,
                "clientOrderId": "pXLV6Hz6mprAcVYpVMTGgx",
                "price": "0.089853",
                "origQty": "0.178622",
                "executedQty": "0.000000",
                "cummulativeQuoteQty": "0.000000",
                "status": "CANCELED",
                "timeInForce": "GTC",
                "type": "LIMIT",
                "side": "BUY"
              },
              {
                "symbol": "BTCUSDT",
                "origClientOrderId": "A3EF2HCwxgZPFMrfwbgrhv",
                "orderId": 13,
                "orderListId": -1,
                "clientOrderId": "pXLV6Hz6mprAcVYpVMTGgx",
                "price": "0.090430",
                "origQty": "0.178622",
                "executedQty": "0.000000",
                "cummulativeQuoteQty": "0.000000",
                "status": "CANCELED",
                "timeInForce": "GTC",
                "type": "LIMIT",
                "side": "BUY"
              },
              {
                "orderListId": 1929,
                "contingencyType": "OCO",
                "listStatusType": "ALL_DONE",
                "listOrderStatus": "ALL_DONE",
                "listClientOrderId": "2inzWQdDvZLHbbAmAozX2N",
                "transactionTime": 1585230948299,
                "symbol": "BTCUSDT",
                "orders": [
                  {
                    "symbol": "BTCUSDT",
                    "orderId": 20,
                    "clientOrderId": "CwOOIPHSmYywx6jZX77TdL"
                  },
                  {
                    "symbol": "BTCUSDT",
                    "orderId": 21,
                    "clientOrderId": "461cPg51vQjV3zIMOXNz39"
                  }
                ],
                "orderReports": [
                  {
                    "symbol": "BTCUSDT",
                    "origClientOrderId": "CwOOIPHSmYywx6jZX77TdL",
                    "orderId": 20,
                    "orderListId": 1929,
                    "clientOrderId": "pXLV6Hz6mprAcVYpVMTGgx",
                    "price": "0.668611",
                    "origQty": "0.690354",
                    "executedQty": "0.000000",
                    "cummulativeQuoteQty": "0.000000",
                    "status": "CANCELED",
                    "timeInForce": "GTC",
                    "type": "STOP_LOSS_LIMIT",
                    "side": "BUY",
                    "stopPrice": "0.378131",
                    "icebergQty": "0.017083"
                  },
                  {
                    "symbol": "BTCUSDT",
                    "origClientOrderId": "461cPg51vQjV3zIMOXNz39",
                    "orderId": 21,
                    "orderListId": 1929,
                    "clientOrderId": "pXLV6Hz6mprAcVYpVMTGgx",
                    "price": "0.008791",
                    "origQty": "0.690354",
                    "executedQty": "0.000000",
                    "cummulativeQuoteQty": "0.000000",
                    "status": "CANCELED",
                    "timeInForce": "GTC",
                    "type": "LIMIT_MAKER",
                    "side": "BUY",
                    "icebergQty": "0.639962"
                  }
                ]
              }
            ]

        :raises: BinanceRequestException, BinanceAPIException

        """
        return self._delete('openOrders', True, data=params)

    def cancel_orders_fast(self, request_body: List[Tuple[str, str]], timeout: float = BaseClient.REQUEST_TIMEOUT):
        return self._other_signed_fast('delete', self.cancel_orders_url, request_body, timeout)

    # User Stream Endpoints
    def get_account(self, **params):
        """Get current account information.
        https://binance-docs.github.io/apidocs/spot/en/#account-information-user_data
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            {
                "makerCommission": 15,
                "takerCommission": 15,
                "buyerCommission": 0,
                "sellerCommission": 0,
                "canTrade": true,
                "canWithdraw": true,
                "canDeposit": true,
                "balances": [
                    {
                        "asset": "BTC",
                        "free": "4723846.89208129",
                        "locked": "0.00000000"
                    },
                    {
                        "asset": "LTC",
                        "free": "4763368.68006011",
                        "locked": "0.00000000"
                    }
                ]
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._get('account', True, data=params)

    def get_account_fast(self, query_string: str, timeout: float = BaseClient.REQUEST_TIMEOUT):
        return self._get_signed_fast(self.get_account_url, query_string, timeout)

    def get_asset_balance(self, asset, **params):
        """Get current asset balance.
        :param asset: required
        :type asset: str
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: dictionary or None if not found
        .. code-block:: python
            {
                "asset": "BTC",
                "free": "4723846.89208129",
                "locked": "0.00000000"
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        res = self.get_account(**params)
        # find asset balance in list of balances
        if "balances" in res:
            for bal in res['balances']:
                if bal['asset'].lower() == asset.lower():
                    return bal
        return None

    def get_my_trades(self, **params):
        """Get trades for a specific symbol.
        https://binance-docs.github.io/apidocs/spot/en/#account-trade-list-user_data
        :param symbol: required
        :type symbol: str
        :param startTime: optional
        :type startTime: int
        :param endTime: optional
        :type endTime: int
        :param limit: Default 500; max 500.
        :type limit: int
        :param fromId: TradeId to fetch from. Default gets most recent trades.
        :type fromId: int
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            [
                {
                    "id": 28457,
                    "price": "4.00000100",
                    "qty": "12.00000000",
                    "commission": "10.10000000",
                    "commissionAsset": "BNB",
                    "time": 1499865549590,
                    "isBuyer": true,
                    "isMaker": false,
                    "isBestMatch": true
                }
            ]
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._get('myTrades', True, data=params)

    def get_my_trades_fast(self, query_string: str, timeout: float = BaseClient.REQUEST_TIMEOUT):
        return self._get_signed_fast(self.get_my_trades_url, query_string, timeout)

    def get_system_status(self):
        """Get system status detail.
        https://binance-docs.github.io/apidocs/spot/en/#system-status-sapi-system
        :returns: API response
        .. code-block:: python
            {
                "status": 0,        # 0: normal，1：system maintenance
                "msg": "normal"     # normal or System maintenance.
            }
        :raises: BinanceAPIException
        """
        return self._request_margin_api('get', 'system/status')

    def get_account_status(self, **params):
        """Get account status detail.
        https://binance-docs.github.io/apidocs/spot/en/#account-status-sapi-user_data
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            {
                "data": "Normal"
            }
        :raises: BinanceWithdrawException
        """
        return self._request_margin_api('get', 'account/status', True, data=params)

    def get_account_api_trading_status(self, **params):
        """Fetch account api trading status detail.
        https://binance-docs.github.io/apidocs/spot/en/#account-api-trading-status-sapi-user_data
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            {
                "data": {          // API trading status detail
                    "isLocked": false,   // API trading function is locked or not
                    "plannedRecoverTime": 0,  // If API trading function is locked, this is the planned recover time
                    "triggerCondition": {
                            "GCR": 150,  // Number of GTC orders
                            "IFER": 150, // Number of FOK/IOC orders
                            "UFR": 300   // Number of orders
                    },
                    "indicators": {  // The indicators updated every 30 seconds
                         "BTCUSDT": [  // The symbol
                            {
                                "i": "UFR",  // Unfilled Ratio (UFR)
                                "c": 20,     // Count of all orders
                                "v": 0.05,   // Current UFR value
                                "t": 0.995   // Trigger UFR value
                            },
                            {
                                "i": "IFER", // IOC/FOK Expiration Ratio (IFER)
                                "c": 20,     // Count of FOK/IOC orders
                                "v": 0.99,   // Current IFER value
                                "t": 0.99    // Trigger IFER value
                            },
                            {
                                "i": "GCR",  // GTC Cancellation Ratio (GCR)
                                "c": 20,     // Count of GTC orders
                                "v": 0.99,   // Current GCR value
                                "t": 0.99    // Trigger GCR value
                            }
                        ],
                        "ETHUSDT": [
                            {
                                "i": "UFR",
                                "c": 20,
                                "v": 0.05,
                                "t": 0.995
                            },
                            {
                                "i": "IFER",
                                "c": 20,
                                "v": 0.99,
                                "t": 0.99
                            },
                            {
                                "i": "GCR",
                                "c": 20,
                                "v": 0.99,
                                "t": 0.99
                            }
                        ]
                    },
                    "updateTime": 1547630471725
                }
            }
        """
        return self._request_margin_api('get', 'account/apiTradingStatus', True, data=params)

    def get_dust_log(self, **params) -> Dict:
        """Get log of small amounts exchanged for BNB.
        https://binance-docs.github.io/apidocs/spot/en/#dustlog-sapi-user_data
        :param startTime: optional
        :type startTime: long
        :param endTime: optional
        :type endTime: long
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            {
                "total": 8,   //Total counts of exchange
                "userAssetDribblets": [
                    {
                        "totalTransferedAmount": "0.00132256",   // Total transfered BNB amount for this exchange.
                        "totalServiceChargeAmount": "0.00002699",    //Total service charge amount for this exchange.
                        "transId": 45178372831,
                        "userAssetDribbletDetails": [           //Details of  this exchange.
                            {
                                "transId": 4359321,
                                "serviceChargeAmount": "0.000009",
                                "amount": "0.0009",
                                "operateTime": 1615985535000,
                                "transferedAmount": "0.000441",
                                "fromAsset": "USDT"
                            },
                            {
                                "transId": 4359321,
                                "serviceChargeAmount": "0.00001799",
                                "amount": "0.0009",
                                "operateTime": 1615985535000,
                                "transferedAmount": "0.00088156",
                                "fromAsset": "ETH"
                            }
                        ]
                    },
                    {
                        "operateTime":1616203180000,
                        "totalTransferedAmount": "0.00058795",
                        "totalServiceChargeAmount": "0.000012",
                        "transId": 4357015,
                        "userAssetDribbletDetails": [
                            {
                                "transId": 4357015,
                                "serviceChargeAmount": "0.00001"
                                "amount": "0.001",
                                "operateTime": 1616203180000,
                                "transferedAmount": "0.00049",
                                "fromAsset": "USDT"
                            },
                            {
                                "transId": 4357015,
                                "serviceChargeAmount": "0.000002"
                                "amount": "0.0001",
                                "operateTime": 1616203180000,
                                "transferedAmount": "0.00009795",
                                "fromAsset": "ETH"
                            }
                        ]
                    }
                ]
            }
        """
        return self._request_margin_api('get', 'asset/dribblet', True, data=params)

    def transfer_dust(self, **params) -> Dict:
        """Convert dust assets to BNB.
        https://binance-docs.github.io/apidocs/spot/en/#dust-transfer-user_data
        :param asset: The asset being converted. e.g: 'ONE'
        :type asset: str
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        .. code:: python
            result = client.transfer_dust(asset='ONE')
        :returns: API response
        .. code-block:: python
            {
                "totalServiceCharge":"0.02102542",
                "totalTransfered":"1.05127099",
                "transferResult":[
                    {
                        "amount":"0.03000000",
                        "fromAsset":"ETH",
                        "operateTime":1563368549307,
                        "serviceChargeAmount":"0.00500000",
                        "tranId":2970932918,
                        "transferedAmount":"0.25000000"
                    }
                ]
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('post', 'asset/dust', True, data=params)

    def get_asset_dividend_history(self, **params) -> Dict:
        """Query asset dividend record.
        https://binance-docs.github.io/apidocs/spot/en/#asset-dividend-record-user_data
        :param asset: optional
        :type asset: str
        :param startTime: optional
        :type startTime: long
        :param endTime: optional
        :type endTime: long
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        .. code:: python
            result = client.get_asset_dividend_history()
        :returns: API response
        .. code-block:: python
            {
                "rows":[
                    {
                        "amount":"10.00000000",
                        "asset":"BHFT",
                        "divTime":1563189166000,
                        "enInfo":"BHFT distribution",
                        "tranId":2968885920
                    },
                    {
                        "amount":"10.00000000",
                        "asset":"BHFT",
                        "divTime":1563189165000,
                        "enInfo":"BHFT distribution",
                        "tranId":2968885920
                    }
                ],
                "total":2
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'asset/assetDividend', True, data=params)

    def make_universal_transfer(self, **params) -> Dict:
        """User Universal Transfer
        https://binance-docs.github.io/apidocs/spot/en/#user-universal-transfer
        :param type: required
        :type type: str (ENUM)
        :param asset: required
        :type asset: str
        :param amount: required
        :type amount: str
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        .. code:: python
            transfer_status = client.make_universal_transfer(params)
        :returns: API response
        .. code-block:: python
            {
                "tranId":13526853623
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('post', 'asset/transfer', signed=True, data=params)

    def query_universal_transfer_history(self, **params) -> Dict:
        """Query User Universal Transfer History
        https://binance-docs.github.io/apidocs/spot/en/#query-user-universal-transfer-history
        :param type: required
        :type type: str (ENUM)
        :param startTime: optional
        :type startTime: int
        :param endTime: optional
        :type endTime: int
        :param current: optional - Default 1
        :type current: int
        :param size: required - Default 10, Max 100
        :type size: int
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        .. code:: python
            transfer_status = client.query_universal_transfer_history(params)
        :returns: API response
        .. code-block:: python
            {
                "total":2,
                "rows":[
                    {
                        "asset":"USDT",
                        "amount":"1",
                        "type":"MAIN_UMFUTURE"
                        "status": "CONFIRMED",
                        "tranId": 11415955596,
                        "timestamp":1544433328000
                    },
                    {
                        "asset":"USDT",
                        "amount":"2",
                        "type":"MAIN_UMFUTURE",
                        "status": "CONFIRMED",
                        "tranId": 11366865406,
                        "timestamp":1544433328000
                    }
                ]
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'asset/transfer', signed=True, data=params)

    def get_trade_fee(self, **params) -> List[Dict]:
        """Get trade fee.
        https://binance-docs.github.io/apidocs/spot/en/#trade-fee-sapi-user_data
        :param symbol: optional
        :type symbol: str
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            [
                {
                    "symbol": "ADABNB",
                    "makerCommission": "0.001",
                    "takerCommission": "0.001"
                },
                {
                    "symbol": "BNBBTC",
                    "makerCommission": "0.001",
                    "takerCommission": "0.001"
                }
            ]
        """
        return self._request_margin_api('get', 'asset/tradeFee', True, data=params)

    def get_asset_details(self, **params) -> Dict:
        """Fetch details on assets.
        https://binance-docs.github.io/apidocs/spot/en/#asset-detail-sapi-user_data
        :param asset: optional
        :type asset: str
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            {
                    "CTR": {
                        "minWithdrawAmount": "70.00000000", //min withdraw amount
                        "depositStatus": false,//deposit status (false if ALL of networks' are false)
                        "withdrawFee": 35, // withdraw fee
                        "withdrawStatus": true, //withdraw status (false if ALL of networks' are false)
                        "depositTip": "Delisted, Deposit Suspended" //reason
                    },
                    "SKY": {
                        "minWithdrawAmount": "0.02000000",
                        "depositStatus": true,
                        "withdrawFee": 0.01,
                        "withdrawStatus": true
                    }
            }
        """
        return self._request_margin_api('get', 'asset/assetDetail', True, data=params)

    # Withdraw Endpoints

    def withdraw(self, **params) -> Dict:
        """Submit a withdraw request.
        https://binance-docs.github.io/apidocs/spot/en/#withdraw-sapi
        Assumptions:
        - You must have Withdraw permissions enabled on your API key
        - You must have withdrawn to the address specified through the website and approved the transaction via email
        :param coin: required
        :type coin: str
        :param withdrawOrderId: optional - client id for withdraw
        :type withdrawOrderId: str
        :param network: optional
        :type network: str
        :param address: required
        :type address: str
        :para, addressTag: optional - Secondary address identifier for coins like XRP,XMR etc.
        :type addressTag: str
        :param amount: required
        :type amount: decimal
        :param transactionFeeFlag: required - When making internal transfer, true for returning the fee to the destination account; false for returning the fee back to the departure account. Default false.
        :type transactionFeeFlag: bool
        :param name: optional - Description of the address, default asset value passed will be used
        :type name: str
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            {
                "id":"7213fea8e94b4a5593d507237e5a555b"
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('post', 'capital/withdraw/apply', True, data=params)

    def get_deposit_history(self, **params) -> List[Dict]:
        """Fetch deposit history.
        https://binance-docs.github.io/apidocs/spot/en/#deposit-history-supporting-network-user_data
        :param coin: optional
        :type coin: str
        :type status: 0(0:pending, 6: credited but cannot withdraw, 1:success) optional
        :type status: int
        :param startTime: optional, default: 90 days from current timestamp
        :type startTime: long
        :param endTime: optional, default: present timestamp
        :type endTime: long
        :param offset: optional, default: 0
        :type offset: int
        :param limit: optional, default: 1000, max: 1000
        :type limit: int
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            [
                {
                    "amount":"0.00999800",
                    "coin":"PAXG",
                    "network":"ETH",
                    "status":1,
                    "address":"0x788cabe9236ce061e5a892e1a59395a81fc8d62c",
                    "addressTag":"",
                    "txId":"0xaad4654a3234aa6118af9b4b335f5ae81c360b2394721c019b5d1e75328b09f3",
                    "insertTime":1599621997000,
                    "transferType":0,
                    "confirmTimes":"12/12"
                },
                {
                    "amount":"0.50000000",
                    "coin":"IOTA",
                    "network":"IOTA",
                    "status":1,
                    "address":"SIZ9VLMHWATXKV99LH99CIGFJFUMLEHGWVZVNNZXRJJVWBPHYWPPBOSDORZ9EQSHCZAMPVAPGFYQAUUV9DROOXJLNW",
                    "addressTag":"",
                    "txId":"ESBFVQUTPIWQNJSPXFNHNYHSQNTGKRVKPRABQWTAXCDWOAKDKYWPTVG9BGXNVNKTLEJGESAVXIKIZ9999",
                    "insertTime":1599620082000,
                    "transferType":0,
                    "confirmTimes":"1/1"
                }
            ]
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'capital/deposit/hisrec', True, data=params)

    def get_withdraw_history(self, **params) -> List[Dict]:
        """Fetch withdraw history.
        https://binance-docs.github.io/apidocs/spot/en/#withdraw-history-supporting-network-user_data
        :param coin: optional
        :type coin: str
        :type status: 0(0:Email Sent,1:Cancelled 2:Awaiting Approval 3:Rejected 4:Processing 5:Failure 6Completed) optional
        :type status: int
        :param startTime: optional, default: 90 days from current timestamp
        :type startTime: long
        :param endTime: optional, default: present timestamp
        :type endTime: long
        :param offset: optional, default: 0
        :type offset: int
        :param limit: optional, default: 1000, max: 1000
        :type limit: int
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            [
                {
                    "address": "0x94df8b352de7f46f64b01d3666bf6e936e44ce60",
                    "amount": "8.91000000",
                    "applyTime": "2019-10-12 11:12:02",
                    "coin": "USDT",
                    "id": "b6ae22b3aa844210a7041aee7589627c",
                    "withdrawOrderId": "WITHDRAWtest123", // will not be returned if there's no withdrawOrderId for this withdraw.
                    "network": "ETH",
                    "transferType": 0,   // 1 for internal transfer, 0 for external transfer
                    "status": 6,
                    "transactionFee": "0.004",
                    "txId": "0xb5ef8c13b968a406cc62a93a8bd80f9e9a906ef1b3fcf20a2e48573c17659268"
                },
                {
                    "address": "1FZdVHtiBqMrWdjPyRPULCUceZPJ2WLCsB",
                    "amount": "0.00150000",
                    "applyTime": "2019-09-24 12:43:45",
                    "coin": "BTC",
                    "id": "156ec387f49b41df8724fa744fa82719",
                    "network": "BTC",
                    "status": 6,
                    "transactionFee": "0.004",
                    "transferType": 0,   // 1 for internal transfer, 0 for external transfer
                    "txId": "60fd9007ebfddc753455f95fafa808c4302c836e4d1eebc5a132c36c1d8ac354"
                }
            ]
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'capital/withdraw/history', True, data=params)

    def get_withdraw_history_id(self, withdraw_id, **params) -> Dict:
        """Fetch withdraw history.
        https://binance-docs.github.io/apidocs/spot/en/#withdraw-history-supporting-network-user_data
        :param withdraw_id: required
        :type withdraw_id: str
        :param asset: optional
        :type asset: str
        :type status: 0(0:Email Sent,1:Cancelled 2:Awaiting Approval 3:Rejected 4:Processing 5:Failure 6Completed) optional
        :type status: int
        :param startTime: optional
        :type startTime: long
        :param endTime: optional
        :type endTime: long
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            {
                "id":"7213fea8e94b4a5593d507237e5a555b",
                "withdrawOrderId": None,
                "amount": 0.99,
                "transactionFee": 0.01,
                "address": "0x6915f16f8791d0a1cc2bf47c13a6b2a92000504b",
                "asset": "ETH",
                "txId": "0xdf33b22bdb2b28b1f75ccd201a4a4m6e7g83jy5fc5d5a9d1340961598cfcb0a1",
                "applyTime": 1508198532000,
                "status": 4
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        result = self.get_withdraw_history(**params)

        for entry in result:
            if 'id' in entry and entry['id'] == withdraw_id:
                return entry

        raise Exception("There is no entry with withdraw id", result)

    def get_deposit_address(self, **params) -> Dict:
        """Fetch a deposit address for a symbol
        https://binance-docs.github.io/apidocs/spot/en/#deposit-address-supporting-network-user_data
        :param coin: required
        :type coin: str
        :param network: optional
        :type network: str
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            {
                "address": "1HPn8Rx2y6nNSfagQBKy27GB99Vbzg89wv",
                "coin": "BTC",
                "tag": "",
                "url": "https://btc.com/1HPn8Rx2y6nNSfagQBKy27GB99Vbzg89wv"
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'capital/deposit/address', True, data=params)

    # User Stream Endpoints

    def stream_get_listen_key(self) -> Dict:
        """Start a new user data stream and return the listen key
        If a stream already exists it should return the same key.
        If the stream becomes invalid a new key is returned.
        Can be used to keep the user stream alive.
        https://binance-docs.github.io/apidocs/spot/en/#listen-key-spot
        :returns: API response
        .. code-block:: python
            {
                "listenKey": "pqia91ma19a5s61cv6a81va65sdf19v8a65a1a5s61cv6a81va65sdf19v8a65a1"
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        res = self._post('userDataStream', False, data={})
        return res['listenKey']

    def stream_keepalive(self, listenKey) -> Dict:
        """PING a user data stream to prevent a time out.
        https://binance-docs.github.io/apidocs/spot/en/#listen-key-spot
        :param listenKey: required
        :type listenKey: str
        :returns: API response
        .. code-block:: python
            {}
        :raises: BinanceRequestException, BinanceAPIException
        """
        params = {
            'listenKey': listenKey
        }
        return self._put('userDataStream', False, data=params)

    def stream_close(self, listenKey) -> Dict:
        """Close out a user data stream.
        https://binance-docs.github.io/apidocs/spot/en/#listen-key-spot
        :param listenKey: required
        :type listenKey: str
        :returns: API response
        .. code-block:: python
            {}
        :raises: BinanceRequestException, BinanceAPIException
        """
        params = {
            'listenKey': listenKey
        }
        return self._delete('userDataStream', False, data=params)

    # Margin Trading Endpoints

    def get_margin_account(self, **params) -> Dict:
        """Query cross-margin account details
        https://binance-docs.github.io/apidocs/spot/en/#query-cross-margin-account-details-user_data
        :returns: API response
        .. code-block:: python
            {
                "borrowEnabled": true,
                "marginLevel": "11.64405625",
                "totalAssetOfBtc": "6.82728457",
                "totalLiabilityOfBtc": "0.58633215",
                "totalNetAssetOfBtc": "6.24095242",
                "tradeEnabled": true,
                "transferEnabled": true,
                "userAssets": [
                    {
                        "asset": "BTC",
                        "borrowed": "0.00000000",
                        "free": "0.00499500",
                        "interest": "0.00000000",
                        "locked": "0.00000000",
                        "netAsset": "0.00499500"
                    },
                    {
                        "asset": "BNB",
                        "borrowed": "201.66666672",
                        "free": "2346.50000000",
                        "interest": "0.00000000",
                        "locked": "0.00000000",
                        "netAsset": "2144.83333328"
                    },
                    {
                        "asset": "ETH",
                        "borrowed": "0.00000000",
                        "free": "0.00000000",
                        "interest": "0.00000000",
                        "locked": "0.00000000",
                        "netAsset": "0.00000000"
                    },
                    {
                        "asset": "USDT",
                        "borrowed": "0.00000000",
                        "free": "0.00000000",
                        "interest": "0.00000000",
                        "locked": "0.00000000",
                        "netAsset": "0.00000000"
                    }
                ]
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'margin/account', True, data=params)

    def get_isolated_margin_account(self, **params) -> Dict:
        """Query isolated margin account details
        https://binance-docs.github.io/apidocs/spot/en/#query-isolated-margin-account-info-user_data
        :param symbols: optional up to 5 margin pairs as a comma separated string
        :type asset: str
        .. code:: python
            account_info = client.get_isolated_margin_account()
            account_info = client.get_isolated_margin_account(symbols="BTCUSDT,ETHUSDT")
        :returns: API response
        .. code-block:: python
            If "symbols" is not sent:
                {
                "assets":[
                    {
                        "baseAsset":
                        {
                        "asset": "BTC",
                        "borrowEnabled": true,
                        "borrowed": "0.00000000",
                        "free": "0.00000000",
                        "interest": "0.00000000",
                        "locked": "0.00000000",
                        "netAsset": "0.00000000",
                        "netAssetOfBtc": "0.00000000",
                        "repayEnabled": true,
                        "totalAsset": "0.00000000"
                        },
                        "quoteAsset":
                        {
                        "asset": "USDT",
                        "borrowEnabled": true,
                        "borrowed": "0.00000000",
                        "free": "0.00000000",
                        "interest": "0.00000000",
                        "locked": "0.00000000",
                        "netAsset": "0.00000000",
                        "netAssetOfBtc": "0.00000000",
                        "repayEnabled": true,
                        "totalAsset": "0.00000000"
                        },
                        "symbol": "BTCUSDT"
                        "isolatedCreated": true,
                        "marginLevel": "0.00000000",
                        "marginLevelStatus": "EXCESSIVE", // "EXCESSIVE", "NORMAL", "MARGIN_CALL", "PRE_LIQUIDATION", "FORCE_LIQUIDATION"
                        "marginRatio": "0.00000000",
                        "indexPrice": "10000.00000000"
                        "liquidatePrice": "1000.00000000",
                        "liquidateRate": "1.00000000"
                        "tradeEnabled": true
                    }
                    ],
                    "totalAssetOfBtc": "0.00000000",
                    "totalLiabilityOfBtc": "0.00000000",
                    "totalNetAssetOfBtc": "0.00000000"
                }
            If "symbols" is sent:
                {
                "assets":[
                    {
                        "baseAsset":
                        {
                        "asset": "BTC",
                        "borrowEnabled": true,
                        "borrowed": "0.00000000",
                        "free": "0.00000000",
                        "interest": "0.00000000",
                        "locked": "0.00000000",
                        "netAsset": "0.00000000",
                        "netAssetOfBtc": "0.00000000",
                        "repayEnabled": true,
                        "totalAsset": "0.00000000"
                        },
                        "quoteAsset":
                        {
                        "asset": "USDT",
                        "borrowEnabled": true,
                        "borrowed": "0.00000000",
                        "free": "0.00000000",
                        "interest": "0.00000000",
                        "locked": "0.00000000",
                        "netAsset": "0.00000000",
                        "netAssetOfBtc": "0.00000000",
                        "repayEnabled": true,
                        "totalAsset": "0.00000000"
                        },
                        "symbol": "BTCUSDT"
                        "isolatedCreated": true,
                        "marginLevel": "0.00000000",
                        "marginLevelStatus": "EXCESSIVE", // "EXCESSIVE", "NORMAL", "MARGIN_CALL", "PRE_LIQUIDATION", "FORCE_LIQUIDATION"
                        "marginRatio": "0.00000000",
                        "indexPrice": "10000.00000000"
                        "liquidatePrice": "1000.00000000",
                        "liquidateRate": "1.00000000"
                        "tradeEnabled": true
                    }
                    ]
                }
        """
        return self._request_margin_api('get', 'margin/isolated/account', True, data=params)

    def get_margin_asset(self, **params) -> Dict:
        """Query cross-margin asset
        https://binance-docs.github.io/apidocs/spot/en/#query-margin-asset-market_data
        :param asset: name of the asset
        :type asset: str
        .. code:: python
            asset_details = client.get_margin_asset(asset='BNB')
        :returns: API response
        .. code-block:: python
            {
                "assetFullName": "Binance Coin",
                "assetName": "BNB",
                "isBorrowable": false,
                "isMortgageable": true,
                "userMinBorrow": "0.00000000",
                "userMinRepay": "0.00000000"
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'margin/asset', data=params)

    def get_margin_symbol(self, **params) -> Dict:
        """Query cross-margin symbol info
        https://binance-docs.github.io/apidocs/spot/en/#query-cross-margin-pair-market_data
        :param symbol: name of the symbol pair
        :type symbol: str
        .. code:: python
            pair_details = client.get_margin_symbol(symbol='BTCUSDT')
        :returns: API response
        .. code-block:: python
            {
                "id":323355778339572400,
                "symbol":"BTCUSDT",
                "base":"BTC",
                "quote":"USDT",
                "isMarginTrade":true,
                "isBuyAllowed":true,
                "isSellAllowed":true
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'margin/pair', data=params)

    def create_isolated_margin_account(self, **params) -> Dict:
        """Create isolated margin account for symbol
        https://binance-docs.github.io/apidocs/spot/en/#create-isolated-margin-account-margin
        :param base: Base asset of symbol
        :type base: str
        :param quote: Quote asset of symbol
        :type quote: str
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            {
                "success": true,
                "symbol": "BTCUSDT"
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('post', 'margin/isolated/create', signed=True, data=params)

    def get_isolated_margin_symbol(self, **params) -> Dict:
        """Query isolated margin symbol info
        https://binance-docs.github.io/apidocs/spot/en/#query-isolated-margin-symbol-user_data
        :param symbol: name of the symbol pair
        :type symbol: str
        :param recvWindow: optional, the number of milliseconds the request is valid for, no more than 60000
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            {
            "symbol":"BTCUSDT",
            "base":"BTC",
            "quote":"USDT",
            "isMarginTrade":true,
            "isBuyAllowed":true,
            "isSellAllowed":true
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'margin/isolated/pair', signed=True, data=params)

    def get_all_isolated_margin_symbols(self, **params) -> List[Dict]:
        """Query isolated margin symbol info for all pairs
        https://binance-docs.github.io/apidocs/spot/en/#get-all-isolated-margin-symbol-user_data
        :param recvWindow: optional, the number of milliseconds the request is valid for, no more than 60000
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            [
                {
                    "base": "BNB",
                    "isBuyAllowed": true,
                    "isMarginTrade": true,
                    "isSellAllowed": true,
                    "quote": "BTC",
                    "symbol": "BNBBTC"
                },
                {
                    "base": "TRX",
                    "isBuyAllowed": true,
                    "isMarginTrade": true,
                    "isSellAllowed": true,
                    "quote": "BTC",
                    "symbol": "TRXBTC"
                }
            ]
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'margin/isolated/allPairs', signed=True, data=params)

    def toggle_bnb_burn_spot_margin(self, **params) -> Dict:
        """Toggle BNB Burn On Spot Trade And Margin Interest
        https://binance-docs.github.io/apidocs/spot/en/#toggle-bnb-burn-on-spot-trade-and-margin-interest-user_data
        :param spotBNBBurn: Determines whether to use BNB to pay for trading fees on SPOT
        :type spotBNBBurn: bool
        :param interestBNBBurn: Determines whether to use BNB to pay for margin loan's interest
        :type interestBNBBurn: bool
        .. code:: python
            response = client.toggle_bnb_burn_spot_margin()
        :returns: API response
        .. code-block:: python
            {
               "spotBNBBurn":true,
               "interestBNBBurn": false
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('post', 'bnbBurn', signed=True, data=params)

    def get_bnb_burn_spot_margin(self, **params) -> Dict:
        """Get BNB Burn Status
        https://binance-docs.github.io/apidocs/spot/en/#get-bnb-burn-status-user_data
        .. code:: python
            status = client.get_bnb_burn_spot_margin()
        :returns: API response
        .. code-block:: python
            {
               "spotBNBBurn":true,
               "interestBNBBurn": false
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'bnbBurn', signed=True, data=params)

    def get_margin_price_index(self, **params) -> Dict:
        """Query margin priceIndex
        https://binance-docs.github.io/apidocs/spot/en/#query-margin-priceindex-market_data
        :param symbol: name of the symbol pair
        :type symbol: str
        .. code:: python
            price_index_details = client.get_margin_price_index(symbol='BTCUSDT')
        :returns: API response
        .. code-block:: python
            {
                "calcTime": 1562046418000,
                "price": "0.00333930",
                "symbol": "BNBBTC"
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'margin/priceIndex', data=params)

    def transfer_margin_to_spot(self, **params) -> Dict:
        """Execute transfer between cross-margin account and spot account.
        https://binance-docs.github.io/apidocs/spot/en/#cross-margin-account-transfer-margin
        :param asset: name of the asset
        :type asset: str
        :param amount: amount to transfer
        :type amount: str
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        .. code:: python
            transfer = client.transfer_margin_to_spot(asset='BTC', amount='1.1')
        :returns: API response
        .. code-block:: python
            {
                "tranId": 100000001
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        params['type'] = 2
        return self._request_margin_api('post', 'margin/transfer', signed=True, data=params)

    def transfer_spot_to_margin(self, **params) -> Dict:
        """Execute transfer between spot account and cross-margin account.
        https://binance-docs.github.io/apidocs/spot/en/#cross-margin-account-transfer-margin
        :param asset: name of the asset
        :type asset: str
        :param amount: amount to transfer
        :type amount: str
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        .. code:: python
            transfer = client.transfer_spot_to_margin(asset='BTC', amount='1.1')
        :returns: API response
        .. code-block:: python
            {
                "tranId": 100000001
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        params['type'] = 1
        return self._request_margin_api('post', 'margin/transfer', signed=True, data=params)

    def transfer_isolated_margin_account(self, **params) -> Dict:
        """Transfer isolated margin
        https://binance-docs.github.io/apidocs/spot/en/#create-isolated-margin-account-margin
        :param asset: required, asset, such as BTC
        :type asset: str
        :param symbol: required
        :type symbol: str
        :param transFrom: required, "SPOT", "ISOLATED_MARGIN"
        :type transFrom: str
        :param transTo: required, "SPOT", "ISOLATED_MARGIN"
        :type transTo: str
        :param amount: required
        :type amount: decimal
        :param recvWindow: the number of milliseconds the request is valid for, no more than 60000
        :type recvWindow: int
        :param timestamp: required
        :type timestamp: LONG

        :returns: API response
        .. code-block:: python
            {
                // transaction id
                "tranId": 100000001
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('post', 'margin/isolated/transfer', signed=True, data=params)

    def create_margin_loan(self, **params) -> Dict:
        """Apply for a loan in cross-margin or isolated-margin account.
        https://binance-docs.github.io/apidocs/spot/en/#margin-account-borrow-margin
        :param asset: name of the asset
        :type asset: str
        :param isIsolated: optional, for isolated margin or not, "TRUE", "FALSE", default "FALSE"
        :type isIsolated: str
        :param symbol: optional, isolated symbol
        :type symbol: str
        :param amount: amount to transfer
        :type amount: decimal
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        .. code:: python
            transaction = client.margin_create_loan(asset='BTC', amount='1.1')
            transaction = client.margin_create_loan(asset='BTC', amount='1.1',
                                                    isIsolated='TRUE', symbol='ETHBTC')
        :returns: API response
        .. code-block:: python
            {
                "tranId": 100000001
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('post', 'margin/loan', signed=True, data=params)

    def repay_margin_loan(self, **params) -> Dict:
        """Repay loan in cross-margin or isolated-margin account.
        If amount is more than the amount borrowed, the full loan will be repaid.
        https://binance-docs.github.io/apidocs/spot/en/#margin-account-repay-margin
        :param asset: name of the asset
        :type asset: str
        :param isIsolated: optional, for isolated margin or not, "TRUE", "FALSE", default "FALSE"
        :type isIsolated: str
        :param symbol: optional, isolated symbol
        :type symbol: str
        :param amount: amount to transfer
        :type amount: decimal
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        .. code:: python
            transaction = client.margin_repay_loan(asset='BTC', amount='1.1')
            transaction = client.margin_repay_loan(asset='BTC', amount='1.1',
                                                    isIsolated='TRUE', symbol='ETHBTC')
        :returns: API response
        .. code-block:: python
            {
                "tranId": 100000001
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('post', 'margin/repay', signed=True, data=params)

    def create_margin_order(self, **params) -> Dict:
        """Post a new order for margin account.
        https://binance-docs.github.io/apidocs/spot/en/#margin-account-new-order-trade
        :param symbol: required
        :type symbol: str
        :param isIsolated: optional, for isolated margin or not, "TRUE", "FALSE", default "FALSE"
        :type isIsolated: str
        :param side: required
        :type side: str
        :param type: required
        :type type: str
        :param quantity: required
        :type quantity: decimal
        :param price: required
        :type price: str
        :param stopPrice: Used with STOP_LOSS, STOP_LOSS_LIMIT, TAKE_PROFIT, and TAKE_PROFIT_LIMIT orders.
        :type stopPrice: str
        :param timeInForce: required if limit order GTC,IOC,FOK
        :type timeInForce: str
        :param newClientOrderId: A unique id for the order. Automatically generated if not sent.
        :type newClientOrderId: str
        :param icebergQty: Used with LIMIT, STOP_LOSS_LIMIT, and TAKE_PROFIT_LIMIT to create an iceberg order.
        :type icebergQty: str
        :param newOrderRespType: Set the response JSON. ACK, RESULT, or FULL; MARKET and LIMIT order types default to
            FULL, all other orders default to ACK.
        :type newOrderRespType: str
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        Response ACK:
        .. code-block:: python
            {
                "symbol": "BTCUSDT",
                "orderId": 28,
                "clientOrderId": "6gCrw2kRUAF9CvJDGP16IP",
                "isIsolated": true,
                "transactTime": 1507725176595
            }
        Response RESULT:
        .. code-block:: python
            {
                "symbol": "BTCUSDT",
                "orderId": 28,
                "clientOrderId": "6gCrw2kRUAF9CvJDGP16IP",
                "transactTime": 1507725176595,
                "price": "1.00000000",
                "origQty": "10.00000000",
                "executedQty": "10.00000000",
                "cummulativeQuoteQty": "10.00000000",
                "status": "FILLED",
                "timeInForce": "GTC",
                "type": "MARKET",
                "isIsolated": true,
                "side": "SELL"
            }
        Response FULL:
        .. code-block:: python
            {
                "symbol": "BTCUSDT",
                "orderId": 28,
                "clientOrderId": "6gCrw2kRUAF9CvJDGP16IP",
                "transactTime": 1507725176595,
                "price": "1.00000000",
                "origQty": "10.00000000",
                "executedQty": "10.00000000",
                "cummulativeQuoteQty": "10.00000000",
                "status": "FILLED",
                "timeInForce": "GTC",
                "type": "MARKET",
                "side": "SELL",
                "marginBuyBorrowAmount": 5,       // will not returen if no margin trade happens
                "marginBuyBorrowAsset": "BTC",    // will not returen if no margin trade happens
                "isIsolated": true,       // if isolated margin
                "fills": [
                    {
                        "price": "4000.00000000",
                        "qty": "1.00000000",
                        "commission": "4.00000000",
                        "commissionAsset": "USDT"
                    },
                    {
                        "price": "3999.00000000",
                        "qty": "5.00000000",
                        "commission": "19.99500000",
                        "commissionAsset": "USDT"
                    },
                    {
                        "price": "3998.00000000",
                        "qty": "2.00000000",
                        "commission": "7.99600000",
                        "commissionAsset": "USDT"
                    },
                    {
                        "price": "3997.00000000",
                        "qty": "1.00000000",
                        "commission": "3.99700000",
                        "commissionAsset": "USDT"
                    },
                    {
                        "price": "3995.00000000",
                        "qty": "1.00000000",
                        "commission": "3.99500000",
                        "commissionAsset": "USDT"
                    }
                ]
            }
        :raises: BinanceRequestException, BinanceAPIException, BinanceOrderException, BinanceOrderMinAmountException,
            BinanceOrderMinPriceException, BinanceOrderMinTotalException, BinanceOrderUnknownSymbolException,
            BinanceOrderInactiveSymbolException
        """
        return self._request_margin_api('post', 'margin/order', signed=True, data=params)

    def create_margin_order_fast(self, request_body: List[Tuple[str, str]], timeout: float = BaseClient.REQUEST_TIMEOUT) -> Dict:
        return self._other_signed_fast('post', self.create_margin_order_url, request_body, timeout)

    def cancel_margin_order(self, **params) -> Dict:
        """Cancel an active order for margin account.
        Either orderId or origClientOrderId must be sent.
        https://binance-docs.github.io/apidocs/spot/en/#margin-account-cancel-order-trade
        :param symbol: required
        :type symbol: str
        :param isIsolated: optional, for isolated margin or not, "TRUE", "FALSE", default "FALSE"
        :type isIsolated: str
        :param orderId:
        :type orderId: str
        :param origClientOrderId:
        :type origClientOrderId: str
        :param newClientOrderId: Used to uniquely identify this cancel. Automatically generated by default.
        :type newClientOrderId: str
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
            {
                "symbol": "LTCBTC",
                "isIsolated": true,
                "orderId": 28,
                "origClientOrderId": "myOrder1",
                "clientOrderId": "cancelMyOrder1",
                "transactTime": 1507725176595,
                "price": "1.00000000",
                "origQty": "10.00000000",
                "executedQty": "8.00000000",
                "cummulativeQuoteQty": "8.00000000",
                "status": "CANCELED",
                "timeInForce": "GTC",
                "type": "LIMIT",
                "side": "SELL"
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('delete', 'margin/order', signed=True, data=params)

    def get_margin_loan_details(self, **params) -> Dict:
        """Query loan record
        txId or startTime must be sent. txId takes precedence.
        https://binance-docs.github.io/apidocs/spot/en/#query-loan-record-user_data
        :param asset: required
        :type asset: str
        :param isIsolated: optional, for isolated margin or not, "TRUE", "FALSE", default "FALSE", if isIsolated = "TRUE", symbol must be sent.
        :type isIsolated: str
        :param txId: the tranId in of the created loan
        :type txId: str
        :param startTime: earliest timestamp to filter transactions
        :type startTime: str
        :param endTime: Used to uniquely identify this cancel. Automatically generated by default.
        :type endTime: str
        :param current: Currently querying page. Start from 1. Default:1
        :type current: str
        :param size: Default:10 Max:100
        :type size: int
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
            {
                "rows": [
                    {
                        "isolatedSymbol": "BNBUSDT", // isolated symbol, return "" for crossed margin
                        "asset": "BNB",
                        "principal": "0.84624403",
                        "timestamp": 1555056425000,
                        "status": "CONFIRMED"  // one of PENDING (pending to execution), CONFIRMED (successfully loaned), FAILED (execution failed, nothing happened to your account);
                    }
                ],
                "total": 1
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'margin/loan', signed=True, data=params)

    def get_margin_repay_details(self, **params) -> Dict:
        """Query repay record
        txId or startTime must be sent. txId takes precedence.
        https://binance-docs.github.io/apidocs/spot/en/#query-repay-record-user_data
        :param asset: required
        :type asset: str
        :param isIsolated: optional, for isolated margin or not, "TRUE", "FALSE", default "FALSE"
        :type isIsolated: str
        :param txId: the tranId in of the created loan
        :type txId: str
        :param startTime:
        :type startTime: str
        :param endTime: Used to uniquely identify this cancel. Automatically generated by default.
        :type endTime: str
        :param current: Currently querying page. Start from 1. Default:1
        :type current: str
        :param size: Default:10 Max:100
        :type size: int
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
            {
                "rows": [
                    {
                        "isolatedSymbol": "BNBUSDT",  // isolated symbol, return "" for crossed margin
                        "amount": "14.00000000",  // Total amount repaid
                        "asset": "BNB",
                        "interest": "0.01866667",  // Interest repaid
                        "principal": "13.98133333",  // Principal repaid
                        "status": "CONFIRMED",  // one of PENDING (pending to execution), CONFIRMED (successfully loaned), FAILED (execution failed, nothing happened to your account);
                        "timestamp": 1563438204000,
                        "txId": 2970933056
                    }
                ],
                "total": 1
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'margin/repay', signed=True, data=params)

    def get_margin_order(self, **params) -> Dict:
        """Query margin accounts order
        Either orderId or origClientOrderId must be sent.
        For some historical orders cummulativeQuoteQty will be < 0, meaning the data is not available at this time.
        https://binance-docs.github.io/apidocs/spot/en/#query-margin-account-39-s-order-user_data
        :param symbol: required
        :type symbol: str
        :param isIsolated: optional, for isolated margin or not, "TRUE", "FALSE", default "FALSE"
        :type isIsolated: str
        :param orderId:
        :type orderId: str
        :param origClientOrderId:
        :type origClientOrderId: str
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
            {
                "clientOrderId": "ZwfQzuDIGpceVhKW5DvCmO",
                "cummulativeQuoteQty": "0.00000000",
                "executedQty": "0.00000000",
                "icebergQty": "0.00000000",
                "isWorking": true,
                "orderId": 213205622,
                "origQty": "0.30000000",
                "price": "0.00493630",
                "side": "SELL",
                "status": "NEW",
                "stopPrice": "0.00000000",
                "symbol": "BNBBTC",
                "isIsolated": true,
                "time": 1562133008725,
                "timeInForce": "GTC",
                "type": "LIMIT",
                "updateTime": 1562133008725
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'margin/order', signed=True, data=params)

    def get_open_margin_orders(self, **params) -> List[Dict]:
        """Query margin accounts open orders
        If the symbol is not sent, orders for all symbols will be returned in an array (cross-margin only).
        If querying isolated margin orders, both the isIsolated='TRUE' and symbol=symbol_name must be set.
        When all symbols are returned, the number of requests counted against the rate limiter is equal to the number
        of symbols currently trading on the exchange.
        https://binance-docs.github.io/apidocs/spot/en/#query-margin-account-39-s-open-order-user_data
        :param symbol: optional
        :type symbol: str
        :param isIsolated: optional, for isolated margin or not, "TRUE", "FALSE", default "FALSE", if isIsolated = "TRUE", symbol must be sent.
        :type isIsolated: str
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
            [
                {
                    "clientOrderId": "qhcZw71gAkCCTv0t0k8LUK",
                    "cummulativeQuoteQty": "0.00000000",
                    "executedQty": "0.00000000",
                    "icebergQty": "0.00000000",
                    "isWorking": true,
                    "orderId": 211842552,
                    "origQty": "0.30000000",
                    "price": "0.00475010",
                    "side": "SELL",
                    "status": "NEW",
                    "stopPrice": "0.00000000",
                    "symbol": "BNBBTC",
                    "isIsolated": true,
                    "time": 1562040170089,
                    "timeInForce": "GTC",
                    "type": "LIMIT",
                    "updateTime": 1562040170089
                }
            ]
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'margin/openOrders', signed=True, data=params)

    def get_all_margin_orders(self, **params) -> List[Dict]:
        """Query all margin accounts orders
        If orderId is set, it will get orders >= that orderId. Otherwise most recent orders are returned.
        For some historical orders cummulativeQuoteQty will be < 0, meaning the data is not available at this time.
        https://binance-docs.github.io/apidocs/spot/en/#query-margin-account-39-s-all-order-user_data
        :param symbol: required
        :type symbol: str
        :param isIsolated: optional, for isolated margin or not, "TRUE", "FALSE", default "FALSE", if isIsolated = "TRUE", symbol must be sent.
        :type isIsolated: str
        :param orderId: optional
        :type orderId: str
        :param startTime: optional
        :type startTime: str
        :param endTime: optional
        :type endTime: str
        :param limit: Default 500; max 1000
        :type limit: int
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
            [
                {
                    "clientOrderId": "D2KDy4DIeS56PvkM13f8cP",
                    "cummulativeQuoteQty": "0.00000000",
                    "executedQty": "0.00000000",
                    "icebergQty": "0.00000000",
                    "isWorking": false,
                    "orderId": 41295,
                    "origQty": "5.31000000",
                    "price": "0.22500000",
                    "side": "SELL",
                    "status": "CANCELED",
                    "stopPrice": "0.18000000",
                    "symbol": "BNBBTC",
                    "isIsolated": false,
                    "time": 1565769338806,
                    "timeInForce": "GTC",
                    "type": "TAKE_PROFIT_LIMIT",
                    "updateTime": 1565769342148
                },
                {
                    "clientOrderId": "gXYtqhcEAs2Rn9SUD9nRKx",
                    "cummulativeQuoteQty": "0.00000000",
                    "executedQty": "0.00000000",
                    "icebergQty": "1.00000000",
                    "isWorking": true,
                    "orderId": 41296,
                    "origQty": "6.65000000",
                    "price": "0.18000000",
                    "side": "SELL",
                    "status": "CANCELED",
                    "stopPrice": "0.00000000",
                    "symbol": "BNBBTC",
                    "isIsolated": false,
                    "time": 1565769348687,
                    "timeInForce": "GTC",
                    "type": "LIMIT",
                    "updateTime": 1565769352226
                },
                {
                    "clientOrderId": "duDq1BqohhcMmdMs9FSuDy",
                    "cummulativeQuoteQty": "0.39450000",
                    "executedQty": "2.63000000",
                    "icebergQty": "0.00000000",
                    "isWorking": true,
                    "orderId": 41297,
                    "origQty": "2.63000000",
                    "price": "0.00000000",
                    "side": "SELL",
                    "status": "FILLED",
                    "stopPrice": "0.00000000",
                    "symbol": "BNBBTC",
                    "isIsolated": false,
                    "time": 1565769358139,
                    "timeInForce": "GTC",
                    "type": "MARKET",
                    "updateTime": 1565769358139
                }
            ]
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'margin/allOrders', signed=True, data=params)

    def get_margin_trades(self, **params) -> List[Dict]:
        """Query margin accounts trades
        If fromId is set, it will get orders >= that fromId. Otherwise most recent orders are returned.
        https://binance-docs.github.io/apidocs/spot/en/#query-margin-account-39-s-trade-list-user_data
        :param symbol: required
        :type symbol: str
        :param isIsolated: optional, for isolated margin or not, "TRUE", "FALSE", default "FALSE", if isIsolated = "TRUE", symbol must be sent.
        :type isIsolated: str
        :param fromId: optional
        :type fromId: str
        :param startTime: optional
        :type startTime: str
        :param endTime: optional
        :type endTime: str
        :param limit: Default 500; max 1000
        :type limit: int
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
            [
                {
                    "commission": "0.00006000",
                    "commissionAsset": "BTC",
                    "id": 34,
                    "isBestMatch": true,
                    "isBuyer": false,
                    "isMaker": false,
                    "orderId": 39324,
                    "price": "0.02000000",
                    "qty": "3.00000000",
                    "symbol": "BNBBTC",
                    "isIsolated": false,
                    "time": 1561973357171
                }, {
                    "commission": "0.00002950",
                    "commissionAsset": "BTC",
                    "id": 32,
                    "isBestMatch": true,
                    "isBuyer": false,
                    "isMaker": true,
                    "orderId": 39319,
                    "price": "0.00590000",
                    "qty": "5.00000000",
                    "symbol": "BNBBTC",
                    "isIsolated": false,
                    "time": 1561964645345
                }
            ]
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'margin/myTrades', signed=True, data=params)

    def get_max_margin_loan(self, **params) -> Dict:
        """Query max borrow amount for an asset
        https://binance-docs.github.io/apidocs/spot/en/#query-max-borrow-user_data
        :param asset: required
        :type asset: str
        :param isolatedSymbol: optional, if isolatedSymbol is not sent, crossed margin data will be sent.
        :type isolatedSymbol: str
        :param recvWindow: the number of milliseconds the request is valid for, the value cannot be greater than 60000
        :type recvWindow: int
        :returns: API response
            {
                "amount": "1.69248805"
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'margin/maxBorrowable', signed=True, data=params)

    def get_max_margin_transfer(self, **params) -> Dict:
        """Query max transfer-out amount
        https://binance-docs.github.io/apidocs/spot/en/#query-max-transfer-out-amount-user_data
        :param asset: required
        :type asset: str
        :param isolatedSymbol: optional, if isolatedSymbol is not sent, crossed margin data will be sent.
        :type isolatedSymbol: str
        :param recvWindow: the number of milliseconds the request is valid for, the value cannot be greater than 60000
        :type recvWindow: int
        :returns: API response
            {
                "amount": "3.59498107"
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'margin/maxTransferable', signed=True, data=params)

    def margin_stream_get_listen_key(self):
        """Start a new cross-margin data stream and return the listen key
        If a stream already exists it should return the same key.
        If the stream becomes invalid a new key is returned.
        Can be used to keep the stream alive.
        https://binance-docs.github.io/apidocs/spot/en/#listen-key-margin
        :returns: API response
        .. code-block:: python
            {
                "listenKey": "pqia91ma19a5s61cv6a81va65sdf19v8a65a1a5s61cv6a81va65sdf19v8a65a1"
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        res = self._request_margin_api('post', 'userDataStream', signed=False, data={})
        return res['listenKey']

    def margin_stream_keepalive(self, listenKey):
        """PING a cross-margin data stream to prevent a time out.
        https://binance-docs.github.io/apidocs/spot/en/#listen-key-margin
        :param listenKey: required
        :type listenKey: str
        :returns: API response
        .. code-block:: python
            {}
        :raises: BinanceRequestException, BinanceAPIException
        """
        params = {
            'listenKey': listenKey
        }
        return self._request_margin_api('put', 'userDataStream', signed=False, data=params)

    def margin_stream_close(self, listenKey):
        """Close out a cross-margin data stream.
        https://binance-docs.github.io/apidocs/spot/en/#listen-key-margin
        :param listenKey: required
        :type listenKey: str
        :returns: API response
        .. code-block:: python
            {}
        :raises: BinanceRequestException, BinanceAPIException
        """
        params = {
            'listenKey': listenKey
        }
        return self._request_margin_api('delete', 'userDataStream', signed=False, data=params)

    def isolated_stream_get_listen_key(self, symbol):
        """Start a new isolated margin data stream and return the listen key
        If a stream already exists it should return the same key.
        If the stream becomes invalid a new key is returned.
        Can be used to keep the stream alive.
        https://binance-docs.github.io/apidocs/spot/en/#listen-key-isolated-margin
        :param symbol: required - symbol for the isolated margin account
        :type symbol: str
        :returns: API response
        .. code-block:: python
            {
                "listenKey":  "T3ee22BIYuWqmvne0HNq2A2WsFlEtLhvWCtItw6ffhhdmjifQ2tRbuKkTHhr"
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        params = {
            'symbol': symbol
        }
        res = self._request_margin_api('post', 'userDataStream/isolated', signed=False, data=params)
        return res['listenKey']

    def isolated_stream_keepalive(self, listenKey, symbol):
        """PING an isolated margin data stream to prevent a time out.
        https://binance-docs.github.io/apidocs/spot/en/#listen-key-isolated-margin
        :param symbol: required - symbol for the isolated margin account
        :type symbol: str
        :param listenKey: required
        :type listenKey: str
        :returns: API response
        .. code-block:: python
            {}
        :raises: BinanceRequestException, BinanceAPIException
        """
        params = {
            'symbol': symbol,
            'listenKey': listenKey
        }
        return self._request_margin_api('put', 'userDataStream/isolated', signed=False, data=params)

    def isolated_stream_close(self, listenKey, symbol):
        """Close out an isolated margin data stream.
        https://binance-docs.github.io/apidocs/spot/en/#listen-key-isolated-margin
        :param symbol: required - symbol for the isolated margin account
        :type symbol: str
        :param listenKey: required
        :type listenKey: str
        :returns: API response
        .. code-block:: python
            {}
        :raises: BinanceRequestException, BinanceAPIException
        """
        params = {
            'symbol': symbol,
            'listenKey': listenKey
        }
        return self._request_margin_api('delete', 'userDataStream/isolated', signed=False, data=params)

    def get_all_margin_assets(self, **params):
        """Query all margin assets
        https://github.com/binance-exchange/binance-official-api-docs/blob/master/margin-api.md#get-all-margin-assets-market_data
        :returns: API response

        .. code-block:: python

            [
              {
                  "assetFullName": "USD coin",
                  "assetName": "USDC",
                  "isBorrowable": true,
                  "isMortgageable": true,
                  "userMinBorrow": "0.00000000",
                  "userMinRepay": "0.00000000"
              },
              {
                  "assetFullName": "BNB-coin",
                  "assetName": "BNB",
                  "isBorrowable": true,
                  "isMortgageable": true,
                  "userMinBorrow": "1.00000000",
                  "userMinRepay": "0.00000000"
              },
              {
                  "assetFullName": "Tether",
                  "assetName": "USDT",
                  "isBorrowable": true,
                  "isMortgageable": true,
                  "userMinBorrow": "1.00000000",
                  "userMinRepay": "0.00000000"
              },
              {
                  "assetFullName": "etherum",
                  "assetName": "ETH",
                  "isBorrowable": true,
                  "isMortgageable": true,
                  "userMinBorrow": "0.00000000",
                  "userMinRepay": "0.00000000"
              },
              {
                  "assetFullName": "Bitcoin",
                  "assetName": "BTC",
                  "isBorrowable": true,
                  "isMortgageable": true,
                  "userMinBorrow": "0.00000000",
                  "userMinRepay": "0.00000000"
              }
            ]

        :raises: BinanceRequestException, BinanceAPIException

        """
        return self._request_margin_api('get', 'margin/allAssets', data=params)

    def get_all_margin_symbols(self, **params):
        """Query all margin symbol info
        https://github.com/binance-exchange/binance-official-api-docs/blob/master/margin-api.md#get-all-margin-pairs-market_data
        :returns: API response
        .. code-block:: python
            [
                {
                    "base": "BNB",
                    "id": 351637150141315861,
                    "isBuyAllowed": True,
                    "isMarginTrade": True,
                    "isSellAllowed": True,
                    "quote": "BTC",
                    "symbol": "BNBBTC"
                },
                {
                    "base": "TRX",
                    "id": 351637923235429141,
                    "isBuyAllowed": True,
                    "isMarginTrade": True,
                    "isSellAllowed": True,
                    "quote": "BTC",
                    "symbol": "TRXBTC"
                },
                {
                    "base": "XRP",
                    "id": 351638112213990165,
                    "isBuyAllowed": True,
                    "isMarginTrade": True,
                    "isSellAllowed": True,
                    "quote": "BTC",
                    "symbol": "XRPBTC"
                },
                {
                    "base": "ETH",
                    "id": 351638524530850581,
                    "isBuyAllowed": True,
                    "isMarginTrade": True,
                    "isSellAllowed": True,
                    "quote": "BTC",
                    "symbol": "ETHBTC"
                },
                {
                    "base": "BNB",
                    "id": 376870400832855109,
                    "isBuyAllowed": True,
                    "isMarginTrade": True,
                    "isSellAllowed": True,
                    "quote": "USDT",
                    "symbol": "BNBUSDT"
                },
            ]
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'margin/allPairs', data=params)

    def get_margin_transfer_history(self, **params):
        """Get transfer between margin account and spot account.
        https://github.com/binance-exchange/binance-official-api-docs/blob/master/margin-api.md#get-transfer-history-user_data
        :param asset: name of the asset
        :type asset: str
        :param type: required
        :type type: 'ROLL_IN' or 'ROLL_OUT'
        :param startTime
        :type startTime: long
        :param endTime
        :type endTime: long
        :param current
        :type current: long
        :param size
        :type size: long
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            {
                "rows": [
                {
                    "amount: "0.10000000",
                    "asset": "BNB",
                    "status": "CONFIRMED",
                    "timestamp": 1566898617,
                    "txId": 5240372201,
                    "type": "ROLL_IN"
                },
                {
                    "amount": "5.00000000",
                    "asset": "USDT",
                    "status": "CONFIRMED",
                    "timestamp": 1566888436,
                    "txId": 5239810406,
                    "type": "ROLL_OUT"
                },
                {
                    "amount": "1.00000000",
                    "asset": "EOS,
                    "status": "CONFIRMED",
                    "timestamp": 1566888403,
                    "txId": 5239808703,
                    "type": "ROLL_IN"
                }
                "total": 3
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'margin/transfer', signed=True, data=params)

    def get_margin_interest_history(self, **params):
        """Get margin interest history
        https://github.com/binance-exchange/binance-official-api-docs/blob/master/margin-api.md#get-interest-history-user_data
        :param asset: name of the asset
        :type asset: str
        :param isIsolated: optional, for isolated margin or not, "TRUE", "FALSE", default "FALSE", if isIsolated = "TRUE", symbol must be sent.
        :type isIsolated: str
        :param startTime
        :type startTime: long
        :param endTime
        :type endTime: long
        :param current
        :type current: long
        :param size
        :type size: long
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :param timestamp: required
        :type timestamp: LONG

        :returns: API response

        .. code-block:: python
            {
                "rows": [
                    {
                        "isolatedSymbol": "BNBUSDT", // isolated symbol, return "" for crossed margin
                        "asset": "BNB",
                        "interest": "0.02414667",
                        "interestAccuredTime": 1566813600,
                        "interestRate": "0.01600000",
                        "principal": "36.22000000",
                        "type": "ON_BORROW"
                    },
                    {
                        "asset": "BNB",
                        "interest": "0.02019334",
                        "interestAccuredTime": 1566813600,
                        "interestRate": "0.01600000",
                        "principal": "30.29000000",
                        "type": "ON_BORROW"
                    }
                ],
                "total": 2
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'margin/interestHistory', signed=True, data=params)

    def get_force_liquidation_records(self, **params):
        """Get force liquidation records, response in descending order
        https://github.com/binance-exchange/binance-official-api-docs/blob/master/margin-api.md#get-force-liquidation-record-user_data
        :param startTime
        :type startTime: long
        :param endTime
        :type endTime: long
        :param isolatedSymbol: optional, if isolatedSymbol is not sent, crossed margin data will be sent.
        :type isolatedSymbol: str
        :param current
        :type current: long
        :param size
        :type size: long
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :param timestamp: required
        :type timestamp: LONG

        :returns: API response
        .. code-block:: python
            {
              "rows": [
                  {
                      "avgPrice": "0.00388359",
                      "executedQty": "31.39000000",
                      "orderId": 180015097,
                      "price": "0.00388110",
                      "qty": "31.39000000",
                      "side": "SELL",
                      "symbol": "BNBBTC",
                      "timeInForce": "GTC",
                      "isIsolated": true,
                      "updatedTime": 1558941374745
                  }
              ],
              "total": 1
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'margin/forceLiquidationRec', signed=True, data=params)

    def get_isolated_margin_transfer_history(self, **params):
        """Get isolated margin transfer history
        https://binance-docs.github.io/apidocs/spot/en/#get-isolated-margin-transfer-history-user_data
        :param asset: optional, asset, such as BTC
        :type asset: str
        :param symbol: required
        :type symbol: str
        :param transFrom: optional, "SPOT", "ISOLATED_MARGIN"
        :type transFrom: str
        :param transTo: optional, "SPOT", "ISOLATED_MARGIN"
        :type transTo: str
        :param startTime: optional
        :type startTime: LONG
        :param endTime: optional
        :type endTime: LONG
        :param current: optional, current page, default 1
        :type current: LONG
        :param size: optional, default 10, max 100
        :type size: LONG
        :param recvWindow: the number of milliseconds the request is valid for, no more than 60000
        :type recvWindow: int
        :param timestamp: required
        :type timestamp: LONG

        :returns: API response

        .. code-block:: python
            {
                "rows": [
                    {
                        "amount": "0.10000000",
                        "asset": "BNB",
                        "status": "CONFIRMED",
                        "timestamp": 1566898617000,
                        "txId": 5240372201,
                        "transFrom": "SPOT",
                        "transTo": "ISOLATED_MARGIN"
                    },
                    {
                        "amount": "5.00000000",
                        "asset": "USDT",
                        "status": "CONFIRMED",
                        "timestamp": 1566888436123,
                        "txId": 5239810406,
                        "transFrom": "ISOLATED_MARGIN",
                        "transTo": "SPOT"
                    }
                ],
              "total": 2
            }

        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'margin/isolated/transfer', signed=True, data=params)

    # Lending Endpoints

    def get_lending_product_list(self, **params):
        """Get Lending Product List
        https://binance-docs.github.io/apidocs/spot/en/#get-flexible-product-list-user_data
        """
        return self._request_margin_api('get', 'lending/daily/product/list', signed=True, data=params)

    def get_lending_daily_quota_left(self, **params):
        """Get Left Daily Purchase Quota of Flexible Product.
        https://binance-docs.github.io/apidocs/spot/en/#get-left-daily-purchase-quota-of-flexible-product-user_data
        """
        return self._request_margin_api('get', 'lending/daily/userLeftQuota', signed=True, data=params)

    def purchase_lending_product(self, **params):
        """Purchase Flexible Product
        https://binance-docs.github.io/apidocs/spot/en/#purchase-flexible-product-user_data
        """
        return self._request_margin_api('post', 'lending/daily/purchase', signed=True, data=params)

    def get_lending_daily_redemption_quota(self, **params):
        """Get Left Daily Redemption Quota of Flexible Product
        https://binance-docs.github.io/apidocs/spot/en/#get-left-daily-redemption-quota-of-flexible-product-user_data
        """
        return self._request_margin_api('get', 'lending/daily/userRedemptionQuota', signed=True, data=params)

    def redeem_lending_product(self, **params):
        """Redeem Flexible Product
        https://binance-docs.github.io/apidocs/spot/en/#redeem-flexible-product-user_data
        """
        return self._request_margin_api('post', 'lending/daily/redeem', signed=True, data=params)

    def get_lending_position(self, **params):
        """Get Flexible Product Position
        https://binance-docs.github.io/apidocs/spot/en/#get-flexible-product-position-user_data
        """
        return self._request_margin_api('get', 'lending/daily/token/position', signed=True, data=params)

    def get_fixed_activity_project_list(self, **params):
        """Get Fixed and Activity Project List
        https://binance-docs.github.io/apidocs/spot/en/#get-fixed-and-activity-project-list-user_data
        :param asset: optional
        :type asset: str
        :param type: required - "ACTIVITY", "CUSTOMIZED_FIXED"
        :type type: str
        :param status: optional - "ALL", "SUBSCRIBABLE", "UNSUBSCRIBABLE"; default "ALL"
        :type status: str
        :param sortBy: optional - "START_TIME", "LOT_SIZE", "INTEREST_RATE", "DURATION"; default "START_TIME"
        :type sortBy: str
        :param current: optional - Currently querying page. Start from 1. Default:1
        :type current: int
        :param size: optional - Default:10, Max:100
        :type size: int
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            [
                {
                    "asset": "USDT",
                    "displayPriority": 1,
                    "duration": 90,
                    "interestPerLot": "1.35810000",
                    "interestRate": "0.05510000",
                    "lotSize": "100.00000000",
                    "lotsLowLimit": 1,
                    "lotsPurchased": 74155,
                    "lotsUpLimit": 80000,
                    "maxLotsPerUser": 2000,
                    "needKyc": False,
                    "projectId": "CUSDT90DAYSS001",
                    "projectName": "USDT",
                    "status": "PURCHASING",
                    "type": "CUSTOMIZED_FIXED",
                    "withAreaLimitation": False
                }
            ]
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'lending/project/list', signed=True, data=params)

    def get_lending_account(self, **params):
        """Get Lending Account Details
        https://binance-docs.github.io/apidocs/spot/en/#lending-account-user_data
        """
        return self._request_margin_api('get', 'lending/union/account', signed=True, data=params)

    def get_lending_purchase_history(self, **params):
        """Get Lending Purchase History
        https://binance-docs.github.io/apidocs/spot/en/#get-purchase-record-user_data
        """
        return self._request_margin_api('get', 'lending/union/purchaseRecord', signed=True, data=params)

    def get_lending_redemption_history(self, **params):
        """Get Lending Redemption History
        https://binance-docs.github.io/apidocs/spot/en/#get-redemption-record-user_data
        """
        return self._request_margin_api('get', 'lending/union/redemptionRecord', signed=True, data=params)

    def get_lending_interest_history(self, **params):
        """Get Lending Interest History
        https://binance-docs.github.io/apidocs/spot/en/#get-interest-history-user_data-2
        """
        return self._request_margin_api('get', 'lending/union/interestHistory', signed=True, data=params)

    def change_fixed_activity_to_daily_position(self, **params):
        """Change Fixed/Activity Position to Daily Position
        https://binance-docs.github.io/apidocs/spot/en/#change-fixed-activity-position-to-daily-position-user_data
        """
        return self._request_margin_api('post', 'lending/positionChanged', signed=True, data=params)

    # Sub Accounts

    def get_sub_account_list(self, **params) -> Dict:
        """Query Sub-account List.
        https://binance-docs.github.io/apidocs/spot/en/#query-sub-account-list-sapi-for-master-account
        :param email: optional - Sub-account email
        :type email: str
        :param isFreeze: optional
        :type isFreeze: str
        :param page: optional - Default value: 1
        :type page: int
        :param limit: optional - Default value: 1, Max value: 200
        :type limit: int
        :param recvWindow: optional
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            {
                "subAccounts":[
                    {
                        "email":"testsub@gmail.com",
                        "isFreeze":false,
                        "createTime":1544433328000
                    },
                    {
                        "email":"virtual@oxebmvfonoemail.com",
                        "isFreeze":false,
                        "createTime":1544433328000
                    }
                ]
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'sub-account/list', True, data=params)

    def get_sub_account_transfer_history(self, **params):
        """Query Sub-account Transfer History.
        https://binance-docs.github.io/apidocs/spot/en/#query-sub-account-spot-asset-transfer-history-sapi-for-master-account
        :param fromEmail: optional
        :type fromEmail: str
        :param toEmail: optional
        :type toEmail: str
        :param startTime: optional
        :type startTime: long
        :param endTime: optional
        :type endTime: long
        :param page: optional - Default value: 1
        :type page: int
        :param limit: optional - Default value: 500
        :type limit: int
        :param recvWindow: optional
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            [
                {
                    "from":"aaa@test.com",
                    "to":"bbb@test.com",
                    "asset":"BTC",
                    "qty":"10",
                    "status": "SUCCESS",
                    "tranId": 6489943656,
                    "time":1544433328000
                },
                {
                    "from":"bbb@test.com",
                    "to":"ccc@test.com",
                    "asset":"ETH",
                    "qty":"2",
                    "status": "SUCCESS",
                    "tranId": 6489938713,
                    "time":1544433328000
                }
            ]
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'sub-account/sub/transfer/history', True, data=params)

    def create_sub_account_transfer(self, **params):
        """Execute sub-account transfer

        https://binance-docs.github.io/apidocs/spot/en/#universal-transfer-for-master-account

        :param fromEmail: optional, default master account email
        :type fromEmail: str
        :param toEmail: optional, default master account email
        :type toEmail: str
        :param fromAccountType: required ("SPOT","USDT_FUTURE","COIN_FUTURE")
        :type fromAccountType: str
        :param toAccountType: required ("SPOT","USDT_FUTURE","COIN_FUTURE")
        :type toAccountType: str
        :param asset: required
        :type asset: str
        :param amount: required
        :type amount: decimal
        :param recvWindow: optional
        :type recvWindow: long

        :returns: API response
        .. code-block:: python

            {
                "tranId":11945860693
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('post', 'sub-account/universalTransfer', True, data=params)

    def get_sub_account_assets(self, **params):
        """Fetch sub-account assets
        https://binance-docs.github.io/apidocs/spot/en/#query-sub-account-assets-sapi-for-master-account
        :param email: required
        :type email: str
        :param recvWindow: optional
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            {
                "balances":[
                    {
                        "asset":"ADA",
                        "free":10000,
                        "locked":0
                    },
                    {
                        "asset":"BNB",
                        "free":10003,
                        "locked":0
                    },
                    {
                        "asset":"BTC",
                        "free":11467.6399,
                        "locked":0
                    },
                    {
                        "asset":"ETH",
                        "free":10004.995,
                        "locked":0
                    },
                    {
                        "asset":"USDT",
                        "free":11652.14213,
                        "locked":0
                    }
                ],
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request('get', self.MARGIN_API_URL + '/v3/sub-account/assets', True, data=params)

    # Futures API

    def futures_ping(self):
        """Test connectivity to the Rest API
        https://binance-docs.github.io/apidocs/futures/en/#test-connectivity
        """
        return self._request_futures_api('get', 'ping')

    def futures_time(self):
        """Test connectivity to the Rest API and get the current server time.
        https://binance-docs.github.io/apidocs/futures/en/#check-server-time
        """
        return self._request_futures_api('get', 'time')

    def futures_exchange_info(self):
        """Current exchange trading rules and symbol information
        https://binance-docs.github.io/apidocs/futures/en/#exchange-information-market_data
        """
        return self._request_futures_api('get', 'exchangeInfo')

    def futures_order_book(self, **params):
        """Get the Order Book for the market
        https://binance-docs.github.io/apidocs/futures/en/#order-book-market_data
        """
        return self._request_futures_api('get', 'depth', data=params)

    def futures_recent_trades(self, **params):
        """Get recent trades (up to last 500).
        https://binance-docs.github.io/apidocs/futures/en/#recent-trades-list-market_data
        """
        return self._request_futures_api('get', 'trades', data=params)

    def futures_historical_trades(self, **params):
        """Get older market historical trades.
        https://binance-docs.github.io/apidocs/futures/en/#old-trades-lookup-market_data
        """
        return self._request_futures_api('get', 'historicalTrades', data=params)

    def futures_aggregate_trades(self, **params):
        """Get compressed, aggregate trades. Trades that fill at the time, from the same order, with the same
        price will have the quantity aggregated.
        https://binance-docs.github.io/apidocs/futures/en/#compressed-aggregate-trades-list-market_data
        """
        return self._request_futures_api('get', 'aggTrades', data=params)

    def futures_klines(self, **params):
        """Kline/candlestick bars for a symbol. Klines are uniquely identified by their open time.
        https://binance-docs.github.io/apidocs/futures/en/#kline-candlestick-data-market_data
        """
        return self._request_futures_api('get', 'klines', data=params)

    def futures_continous_klines(self, **params):
        """Kline/candlestick bars for a specific contract type. Klines are uniquely identified by their open time.
        https://binance-docs.github.io/apidocs/futures/en/#continuous-contract-kline-candlestick-data
        """
        return self._request_futures_api('get', 'continuousKlines', data=params)

    def futures_historical_klines(self, symbol, interval, start_str, end_str=None, limit=500):
        """Get historical futures klines from Binance
        :param symbol: Name of symbol pair e.g BNBBTC
        :type symbol: str
        :param interval: Binance Kline interval
        :type interval: str
        :param start_str: Start date string in UTC format or timestamp in milliseconds
        :type start_str: str|int
        :param end_str: optional - end date string in UTC format or timestamp in milliseconds (default will fetch everything up to now)
        :type end_str: str|int
        :param limit: Default 500; max 1000.
        :type limit: int
        :return: list of OHLCV values
        """
        return self._historical_klines(symbol, interval, start_str, end_str=end_str, limit=limit, klines_type=HistoricalKlinesType.FUTURES)

    def futures_historical_klines_generator(self, symbol, interval, start_str, end_str=None):
        """Get historical futures klines generator from Binance
        :param symbol: Name of symbol pair e.g BNBBTC
        :type symbol: str
        :param interval: Binance Kline interval
        :type interval: str
        :param start_str: Start date string in UTC format or timestamp in milliseconds
        :type start_str: str|int
        :param end_str: optional - end date string in UTC format or timestamp in milliseconds (default will fetch everything up to now)
        :type end_str: str|int
        :return: generator of OHLCV values
        """

        return self._historical_klines_generator(symbol, interval, start_str, end_str=end_str, klines_type=HistoricalKlinesType.FUTURES)

    def futures_mark_price(self, **params):
        """Get Mark Price and Funding Rate
        https://binance-docs.github.io/apidocs/futures/en/#mark-price-market_data
        """
        return self._request_futures_api('get', 'premiumIndex', data=params)

    def futures_funding_rate(self, **params):
        """Get funding rate history
        https://binance-docs.github.io/apidocs/futures/en/#get-funding-rate-history-market_data
        """
        return self._request_futures_api('get', 'fundingRate', data=params)

    def futures_ticker(self, **params):
        """24 hour rolling window price change statistics.
        https://binance-docs.github.io/apidocs/futures/en/#24hr-ticker-price-change-statistics-market_data
        """
        return self._request_futures_api('get', 'ticker/24hr', data=params)

    def futures_symbol_ticker(self, **params):
        """Latest price for a symbol or symbols.
        https://binance-docs.github.io/apidocs/futures/en/#symbol-price-ticker-market_data
        """
        return self._request_futures_api('get', 'ticker/price', data=params)

    def futures_orderbook_ticker(self, **params):
        """Best price/qty on the order book for a symbol or symbols.
        https://binance-docs.github.io/apidocs/futures/en/#symbol-order-book-ticker-market_data
        """
        return self._request_futures_api('get', 'ticker/bookTicker', data=params)

    def futures_liquidation_orders(self, **params):
        """Get all liquidation orders
        https://binance-docs.github.io/apidocs/futures/en/#get-all-liquidation-orders-market_data
        """
        return self._request_futures_api('get', 'forceOrders', signed=True, data=params)

    def futures_adl_quantile_estimate(self, **params):
        """Get Position ADL Quantile Estimate
        https://binance-docs.github.io/apidocs/futures/en/#position-adl-quantile-estimation-user_data
        """
        return self._request_futures_api('get', 'adlQuantile', signed=True, data=params)

    def futures_open_interest(self, **params):
        """Get present open interest of a specific symbol.
        https://binance-docs.github.io/apidocs/futures/en/#open-interest
        """
        return self._request_futures_api('get', 'openInterest', data=params)

    def futures_leverage_bracket(self, **params):
        """Notional and Leverage Brackets
        https://binance-docs.github.io/apidocs/futures/en/#notional-and-leverage-brackets-market_data
        """
        return self._request_futures_api('get', 'leverageBracket', True, data=params)

    def futures_account_transfer(self, **params):
        """Execute transfer between spot account and futures account.
        https://binance-docs.github.io/apidocs/futures/en/#new-future-account-transfer
        """
        return self._request_margin_api('post', 'futures/transfer', True, data=params)

    def transfer_history(self, **params):
        """Get future account transaction history list
        https://binance-docs.github.io/apidocs/futures/en/#get-future-account-transaction-history-list-user_data
        """
        return self._request_margin_api('get', 'futures/transfer', True, data=params)

    def futures_create_order(self, **params):
        """Send in a new order.
        https://binance-docs.github.io/apidocs/futures/en/#new-order-trade
        """
        return self._request_futures_api('post', 'order', True, data=params)

    def futures_place_batch_order(self, **params):
        """Send in new orders.
        https://binance-docs.github.io/apidocs/futures/en/#place-multiple-orders-trade
        To avoid modifying the existing signature generation and parameter order logic,
        the url encoding is done on the special query param, batchOrders, in the early stage.
        """
        query_string = urlencode(params)
        query_string = query_string.replace('%27', '%22')
        params['batchOrders'] = query_string[12:]
        return self._request_futures_api('post', 'batchOrders', True, data=params)

    def futures_get_order(self, **params):
        """Check an order's status.
        https://binance-docs.github.io/apidocs/futures/en/#query-order-user_data
        """
        return self._request_futures_api('get', 'order', True, data=params)

    def futures_get_open_order(self, **params):
        """Get open order given orderId or origClientOrderId
        https://binance-docs.github.io/apidocs/futures/en/#query-current-open-order-user_data
        """
        return self._request_futures_api('get', 'openOrder', True, data=params)

    def futures_get_open_orders(self, **params):
        """Get all open orders on a symbol.
        https://binance-docs.github.io/apidocs/futures/en/#current-open-orders-user_data
        """
        return self._request_futures_api('get', 'openOrders', True, data=params)

    def futures_get_all_orders(self, **params):
        """Get all futures account orders; active, canceled, or filled.
        https://binance-docs.github.io/apidocs/futures/en/#all-orders-user_data
        """
        return self._request_futures_api('get', 'allOrders', True, data=params)

    def futures_cancel_order(self, **params):
        """Cancel an active futures order.
        https://binance-docs.github.io/apidocs/futures/en/#cancel-order-trade
        """
        return self._request_futures_api('delete', 'order', True, data=params)

    def futures_cancel_all_open_orders(self, **params):
        """Cancel all open futures orders
        https://binance-docs.github.io/apidocs/futures/en/#cancel-all-open-orders-trade
        """
        return self._request_futures_api('delete', 'allOpenOrders', True, data=params)

    def futures_cancel_orders(self, **params):
        """Cancel multiple futures orders
        https://binance-docs.github.io/apidocs/futures/en/#cancel-multiple-orders-trade
        """
        return self._request_futures_api('delete', 'batchOrders', True, data=params)

    def futures_account_balance(self, **params):
        """Get futures account balance
        https://binance-docs.github.io/apidocs/futures/en/#future-account-balance-user_data
        """
        return self._request_futures_api('get', 'balance', True, data=params)

    def futures_account(self, **params):
        """Get current account information.
        https://binance-docs.github.io/apidocs/futures/en/#account-information-user_data
        """
        return self._request_futures_api('get', 'account', True, data=params)

    def futures_change_leverage(self, **params):
        """Change user's initial leverage of specific symbol market
        https://binance-docs.github.io/apidocs/futures/en/#change-initial-leverage-trade
        """
        return self._request_futures_api('post', 'leverage', True, data=params)

    def futures_change_margin_type(self, **params):
        """Change the margin type for a symbol
        https://binance-docs.github.io/apidocs/futures/en/#change-margin-type-trade
        """
        return self._request_futures_api('post', 'marginType', True, data=params)

    def futures_change_position_margin(self, **params):
        """Change the position margin for a symbol
        https://binance-docs.github.io/apidocs/futures/en/#modify-isolated-position-margin-trade
        """
        return self._request_futures_api('post', 'positionMargin', True, data=params)

    def futures_position_margin_history(self, **params):
        """Get position margin change history
        https://binance-docs.github.io/apidocs/futures/en/#get-postion-margin-change-history-trade
        """
        return self._request_futures_api('get', 'positionMargin/history', True, data=params)

    def futures_position_information(self, **params):
        """Get position information
        https://binance-docs.github.io/apidocs/futures/en/#position-information-user_data
        """
        return self._request_futures_api('get', 'positionRisk', True, data=params)

    def futures_account_trades(self, **params):
        """Get trades for the authenticated account and symbol.
        https://binance-docs.github.io/apidocs/futures/en/#account-trade-list-user_data
        """
        return self._request_futures_api('get', 'userTrades', True, data=params)

    def futures_income_history(self, **params):
        """Get income history for authenticated account
        https://binance-docs.github.io/apidocs/futures/en/#get-income-history-user_data
        """
        return self._request_futures_api('get', 'income', True, data=params)

    def transfer_futures_to_spot(self, **params):
        """Execute transfer between spot account and futures account.
        https://binance-docs.github.io/apidocs/futures/en/#new-future-account-transfer
        :param asset: required
        :type asset: str
        :param amount: required
        :type amount: str
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            {
                "tranId": 100000001
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        params['type'] = 2
        return self._request_margin_api('post', 'futures/transfer', signed=True, data=params)

    def transfer_spot_to_futures(self, **params):
        """Execute transfer between spot account and futures account.
        https://binance-docs.github.io/apidocs/futures/en/#new-future-account-transfer
        :param asset: required
        :type asset: str
        :param amount: required
        :type amount: str
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            {
                "tranId": 100000001
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        params['type'] = 1
        return self._request_margin_api('post', 'futures/transfer', signed=True, data=params)

    def futures_change_position_mode(self, **params):
        """Change position mode for authenticated account
        https://binance-docs.github.io/apidocs/futures/en/#change-position-mode-trade
        """
        return self._request_futures_api('post', 'positionSide/dual', True, data=params)

    def futures_get_position_mode(self, **params):
        """Get position mode for authenticated account
        https://binance-docs.github.io/apidocs/futures/en/#get-current-position-mode-user_data
        """
        return self._request_futures_api('get', 'positionSide/dual', True, data=params)

    def futures_change_multi_assets_mode(self, multiAssetsMargin: bool):
        """Change user's Multi-Assets mode (Multi-Assets Mode or Single-Asset Mode) on Every symbol
        https://binance-docs.github.io/apidocs/futures/en/#change-multi-assets-mode-trade
        """
        params = {
            'true' if multiAssetsMargin else 'false'
        }
        return self._request_futures_api('post', 'multiAssetsMargin', True, data=params)

    def futures_get_multi_assets_mode(self):
        """Get user's Multi-Assets mode (Multi-Assets Mode or Single-Asset Mode) on Every symbol
        https://binance-docs.github.io/apidocs/futures/en/#get-current-multi-assets-mode-user_data
        """
        return self._request_futures_api('get', 'multiAssetsMargin', True)

    def futures_stream_get_listen_key(self):
        res = self._request_futures_api('post', 'listenKey', signed=False, data={})
        return res['listenKey']

    def futures_stream_keepalive(self, listenKey):
        params = {
            'listenKey': listenKey
        }
        return self._request_futures_api('put', 'listenKey', signed=False, data=params)

    def futures_stream_close(self, listenKey):
        params = {
            'listenKey': listenKey
        }
        return self._request_futures_api('delete', 'listenKey', signed=False, data=params)

    # Traditional Futures API (Manual Update)

    def tfutures_ping(self):
        """Test connectivity to the Rest API
        https://binance-docs.github.io/apidocs/delivery/en/#test-connectivity
        """
        return self._request_tfutures_api('get', 'ping')

    def tfutures_time(self):
        """Test connectivity to the Rest API and get the current server time.
        https://binance-docs.github.io/apidocs/delivery/en/#check-server-time
        """
        return self._request_tfutures_api('get', 'time')

    def tfutures_exchange_info(self):
        """Current exchange trading rules and symbol information
        https://binance-docs.github.io/apidocs/delivery/en/#exchange-information
        """
        return self._request_tfutures_api('get', 'exchangeInfo')

    def tfutures_order_book(self, **params):
        """Get the Order Book for the market
        https://binance-docs.github.io/apidocs/delivery/en/#order-book
        """
        return self._request_tfutures_api('get', 'depth', data=params)

    def tfutures_recent_trades(self, **params):
        """Get recent trades (up to last 500).
        https://binance-docs.github.io/apidocs/delivery/en/#recent-trades-list
        """
        return self._request_tfutures_api('get', 'trades', data=params)

    def tfutures_historical_trades(self, **params):
        """Get older market historical trades.
        https://binance-docs.github.io/apidocs/delivery/en/#old-trades-lookup-market_data
        """
        return self._request_tfutures_api('get', 'historicalTrades', data=params)

    def tfutures_aggregate_trades(self, **params):
        """Get compressed, aggregate trades. Trades that fill at the time, from the same order, with the same
        price will have the quantity aggregated.
        https://binance-docs.github.io/apidocs/delivery/en/#compressed-aggregate-trades-list
        """
        return self._request_tfutures_api('get', 'aggTrades', data=params)

    def tfutures_klines(self, **params):
        """Kline/candlestick bars for a symbol. Klines are uniquely identified by their open time.
        https://binance-docs.github.io/apidocs/delivery/en/#kline-candlestick-data
        """
        return self._request_tfutures_api('get', 'klines', data=params)

    def tfutures_continuous_klines(self, **params):
        """Kline/candlestick bars for a specific contract type. Klines are uniquely identified by their open time.
        https://binance-docs.github.io/apidocs/delivery/en/#continues-contract-kline-candlestick-data
        """
        return self._request_tfutures_api('get', 'continuousKlines', data=params)

    def tfutures_index_price_klines(self, **params):
        """Kline/candlestick bars for the index price of a pair. Klines are uniquely identified by their open time.
        https://binance-docs.github.io/apidocs/delivery/en/#index-price-kline-candlestick-data
        """
        return self._request_tfutures_api('get', 'indexPriceKlines', data=params)

    def tfutures_mark_price_klines(self, **params):
        """Kline/candlestick bars for the mark price of a symbol. Klines are uniquely identified by their open time.
        https://binance-docs.github.io/apidocs/delivery/en/#mark-price-kline-candlestick-data
        """
        return self._request_tfutures_api('get', 'markPriceKlines', data=params)

    def tfutures_mark_price(self, **params):
        """Get Index Price, Mark Price and Estimated Settlement Price.
        https://binance-docs.github.io/apidocs/delivery/en/#index-price-and-mark-price
        """
        return self._request_tfutures_api('get', 'premiumIndex', data=params)

    def tfutures_ticker(self, **params):
        """24 hour rolling window price change statistics.
        https://binance-docs.github.io/apidocs/delivery/en/#24hr-ticker-price-change-statistics
        """
        return self._request_tfutures_api('get', 'ticker/24hr', data=params)

    def tfutures_symbol_ticker(self, **params):
        """Latest price for a symbol or symbols.
        https://binance-docs.github.io/apidocs/delivery/en/#symbol-price-ticker
        """
        return self._request_tfutures_api('get', 'ticker/price', data=params)

    def tfutures_orderbook_ticker(self, **params):
        """Best price/qty on the order book for a symbol or symbols.
        https://binance-docs.github.io/apidocs/delivery/en/#symbol-order-book-ticker
        """
        return self._request_tfutures_api('get', 'ticker/bookTicker', data=params)

    def tfutures_liquidation_orders(self, **params):
        """Get all liquidation orders
        https://binance-docs.github.io/apidocs/delivery/en/#get-all-liquidation-orders
        """
        return self._request_tfutures_api('get', 'allForceOrders', data=params)

    def tfutures_open_interest(self, **params):
        """Get present open interest of a specific symbol.
        https://binance-docs.github.io/apidocs/delivery/en/#open-interest
        """
        return self._request_tfutures_api('get', 'openInterest', data=params)

    def tfutures_leverage_bracket(self, **params):
        """Notional and Leverage Brackets
        https://binance-docs.github.io/apidocs/delivery/en/#notional-bracket-user_data
        """
        return self._request_tfutures_api('get', 'leverageBracket', data=params)

    def transfer_tfutures_to_spot(self, **params):
        """Execute transfer between spot account and futures account.
        https://binance-docs.github.io/apidocs/delivery/en/#new-future-account-transfer
        :param asset: required
        :type asset: str
        :param amount: required
        :type amount: str
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            {
                "tranId": 100000001
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        params['type'] = 4
        return self._request_margin_api('post', 'futures/transfer', signed=True, data=params)

    def transfer_spot_to_tfutures(self, **params):
        """Execute transfer between spot account and futures account.
        https://binance-docs.github.io/apidocs/delivery/en/#new-future-account-transfer
        :param asset: required
        :type asset: str
        :param amount: required
        :type amount: str
        :param recvWindow: the number of milliseconds the request is valid for
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            {
                "tranId": 100000001
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        params['type'] = 3
        return self._request_margin_api('post', 'futures/transfer', signed=True, data=params)

    def tfutures_create_order(self, **params):
        """Send in a new order.
        https://binance-docs.github.io/apidocs/delivery/en/#new-order-trade
        """
        return self._request_tfutures_api('post', 'order', True, data=params)

    def tfutures_batch_order(self, **params):
        """Place multiple orders concurrently.
        https://binance-docs.github.io/apidocs/delivery/en/#place-multiple-orders-trade
        To avoid modifying the existing signature generation and parameter order logic,
        the url encoding is done on the special query param, batchOrders, in the early stage.
        """
        query_string = urlencode(params)
        query_string = query_string.replace('%27', '%22')
        params['batchOrders'] = query_string[12:]

        return self._request_tfutures_api('post', 'batchOrders', True, data=params)

    def tfutures_get_order(self, **params):
        """Check an order's status.
        https://binance-docs.github.io/apidocs/delivery/en/#query-order-user_data
        """
        return self._request_tfutures_api('get', 'order', True, data=params)

    def tfutures_get_open_order(self, **params):
        """Get open order given orderId or origClientOrderId
        https://binance-docs.github.io/apidocs/delivery/en/#query-current-open-order-user_data
        """
        return self._request_tfutures_api('get', 'openOrder', True, data=params)

    def tfutures_get_open_orders(self, **params):
        """Get all open orders on a symbol.
        https://binance-docs.github.io/apidocs/delivery/en/#current-all-open-orders-user_data
        """
        return self._request_tfutures_api('get', 'openOrders', True, data=params)

    def tfutures_get_all_orders(self, **params):
        """Get all futures account orders; active, canceled, or filled.
        https://binance-docs.github.io/apidocs/futures/en/#all-orders-user_data
        """
        return self._request_tfutures_api('get', 'allOrders', True, data=params)

    def tfutures_cancel_order(self, **params):
        """Cancel an active order.
        https://binance-docs.github.io/apidocs/delivery/en/#cancel-order-trade
        """
        return self._request_tfutures_api('delete', 'order', True, data=params)

    def tfutures_cancel_all_open_orders(self, **params):
        """Cancel all open futures orders
        https://binance-docs.github.io/apidocs/delivery/en/#cancel-all-open-orders-trade
        """
        return self._request_tfutures_api('delete', 'allOpenOrders', True, data=params)

    def tfutures_cancel_orders(self, **params):
        """Cancel multiple futures orders
        https://binance-docs.github.io/apidocs/delivery/en/#cancel-multiple-orders-trade
        """
        return self._request_tfutures_api('delete', 'batchOrders', True, data=params)

    def tfutures_auto_cancel_all_open_orders(self, **params):
        """Cancel all open orders of the specified symbol at the end of the specified countdown.
        https://binance-docs.github.io/apidocs/delivery/en/#auto-cancel-all-open-orders-trade
        """
        return self._request_tfutures_api('post', 'countdownCancelAll', True, data=params)

    def tfutures_account_balance(self, **params):
        """Get futures account balance
        https://binance-docs.github.io/apidocs/delivery/en/#futures-account-balance-user_data
        """
        return self._request_tfutures_api('get', 'balance', True, data=params)

    def tfutures_account(self, **params):
        """Get futures account information
        https://binance-docs.github.io/apidocs/delivery/en/#account-information-user_data
        """
        return self._request_tfutures_api('get', 'account', True, data=params)

    def tfutures_change_leverage(self, **params):
        """Change user's initial leverage of specific symbol market
        https://binance-docs.github.io/apidocs/delivery/en/#change-initial-leverage-trade
        """
        return self._request_tfutures_api('post', 'leverage', True, data=params)

    def tfutures_change_margin_type(self, **params):
        """Change the margin type for a symbol
        https://binance-docs.github.io/apidocs/delivery/en/#change-margin-type-trade
        """
        return self._request_tfutures_api('post', 'marginType', True, data=params)

    def tfutures_change_position_margin(self, **params):
        """Change the position margin for a symbol
        https://binance-docs.github.io/apidocs/delivery/en/#modify-isolated-position-margin-trade
        """
        return self._request_tfutures_api('post', 'positionMargin', True, data=params)

    def tfutures_position_margin_history(self, **params):
        """Get position margin change history
        https://binance-docs.github.io/apidocs/delivery/en/#get-position-margin-change-history-trade
        """
        return self._request_tfutures_api('get', 'positionMargin/history', True, data=params)

    def tfutures_position_information(self, **params):
        """Get position information
        https://binance-docs.github.io/apidocs/delivery/en/#position-information-user_data
        """
        return self._request_tfutures_api('get', 'positionRisk', True, data=params)

    def tfutures_account_trades(self, **params):
        """Get trades for the authenticated account and symbol.
        https://binance-docs.github.io/apidocs/delivery/en/#account-trade-list-user_data
        """
        return self._request_tfutures_api('get', 'userTrades', True, data=params)

    def tfutures_income_history(self, **params):
        """Get income history for authenticated account
        https://binance-docs.github.io/apidocs/delivery/en/#get-income-history-user_data
        """
        return self._request_tfutures_api('get', 'income', True, data=params)

    def tfutures_change_position_mode(self, **params):
        """Change user's position mode (Hedge Mode or One-way Mode) on EVERY symbol
        https://binance-docs.github.io/apidocs/delivery/en/#change-position-mode-trade
        """
        return self._request_tfutures_api('post', 'positionSide/dual', True, data=params)

    def tfutures_get_position_mode(self, **params):
        """Get user's position mode (Hedge Mode or One-way Mode) on EVERY symbol
        https://binance-docs.github.io/apidocs/delivery/en/#get-current-position-mode-user_data
        """
        return self._request_tfutures_api('get', 'positionSide/dual', True, data=params)

    def tfuture_stream_get_listen_key(self):
        res = self._request_tfutures_api('post', 'listenKey', signed=False, data={})
        return res['listenKey']

    def tfuture_stream_keepalive(self, listenKey):
        params = {'listenKey': listenKey}
        return self._request_tfutures_api('put', 'listenKey', signed=False, data=params)

    def tfuture_stream_close(self, listenKey):
        params = {'listenKey': listenKey}
        return self._request_tfutures_api('delete', 'listenKey', signed=False, data=params)

    def get_all_coins_info(self, **params):
        """Get information of coins (available for deposit and withdraw) for user.
        https://binance-docs.github.io/apidocs/spot/en/#all-coins-39-information-user_data
        :param recvWindow: optional
        :type recvWindow: int
        :returns: API response
        .. code-block:: python
            {
                "coin": "BTC",
                "depositAllEnable": true,
                "withdrawAllEnable": true,
                "name": "Bitcoin",
                "free": "0",
                "locked": "0",
                "freeze": "0",
                "withdrawing": "0",
                "ipoing": "0",
                "ipoable": "0",
                "storage": "0",
                "isLegalMoney": false,
                "trading": true,
                "networkList": [
                    {
                        "network": "BNB",
                        "coin": "BTC",
                        "withdrawIntegerMultiple": "0.00000001",
                        "isDefault": false,
                        "depositEnable": true,
                        "withdrawEnable": true,
                        "depositDesc": "",
                        "withdrawDesc": "",
                        "specialTips": "Both a MEMO and an Address are required to successfully deposit your BEP2-BTCB tokens to Binance.",
                        "name": "BEP2",
                        "resetAddressStatus": false,
                        "addressRegex": "^(bnb1)[0-9a-z]{38}$",
                        "memoRegex": "^[0-9A-Za-z-_]{1,120}$",
                        "withdrawFee": "0.0000026",
                        "withdrawMin": "0.0000052",
                        "withdrawMax": "0",
                        "minConfirm": 1,
                        "unLockConfirm": 0
                    },
                    {
                        "network": "BTC",
                        "coin": "BTC",
                        "withdrawIntegerMultiple": "0.00000001",
                        "isDefault": true,
                        "depositEnable": true,
                        "withdrawEnable": true,
                        "depositDesc": "",
                        "withdrawDesc": "",
                        "specialTips": "",
                        "name": "BTC",
                        "resetAddressStatus": false,
                        "addressRegex": "^[13][a-km-zA-HJ-NP-Z1-9]{25,34}$|^(bc1)[0-9A-Za-z]{39,59}$",
                        "memoRegex": "",
                        "withdrawFee": "0.0005",
                        "withdrawMin": "0.001",
                        "withdrawMax": "0",
                        "minConfirm": 1,
                        "unLockConfirm": 2
                    }
                ]
            }
        :raises: BinanceRequestException, BinanceAPIException
        """
        return self._request_margin_api('get', 'capital/config/getall', signed=True, data=params)

    def close_connection(self):
        if self.session:
            self.session.close()

    def __del__(self):
        self.close_connection()


class AsyncClient(BaseClient):

    def __init__(
            self, api_key: Optional[str] = None, api_secret: Optional[str] = None, timestamp_offset: Optional[int] = None,
            requests_params: Dict = {}, tld: str = 'com', loop=None
    ):

        self.loop = loop or asyncio.get_event_loop()
        super().__init__(api_key, api_secret, timestamp_offset, requests_params, tld)

    @classmethod
    async def create(cls, api_key='', api_secret='', timestamp_offset=None, requests_params=None, tld='com', loop=None):
        self = cls(api_key, api_secret, timestamp_offset, requests_params, tld, loop)
        await self.ping_fast()
        return self

    def _init_session(self) -> aiohttp.ClientSession:
        session = aiohttp.ClientSession(
            loop=self.loop,
            headers=self._get_headers()
        )
        return session

    async def close_connection(self):
        if self.session:
            assert self.session
            await self.session.close()

    async def _request(self, method, uri: str, signed: bool, force_params: bool = False, **kwargs):
        kwargs = self._get_request_kwargs(method, signed, force_params, **kwargs)

        async with getattr(self.session, method)(uri, **kwargs) as response:
            self.response = response
            return await self._handle_response(response)

    async def _request_fast(self, method, uri: str, query_string: str, timeout: float = BaseClient.REQUEST_TIMEOUT):
        async with getattr(self.session, method)(uri, params=query_string, timeout=timeout) as response:
            self.response = response
            return await self._handle_response(self.response)

    async def _get_signed_fast(self, uri: str, query_string: str, timeout: float = BaseClient.REQUEST_TIMEOUT):
        if query_string:
            query_string += f'&timestamp={time.time() * 1000 + self.timestamp_offset:.0f}'
        else:
            query_string = f'timestamp={time.time() * 1000 + self.timestamp_offset:.0f}'
        m = hmac.new(self.API_SECRET.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256)
        async with self.session.get(uri, params=f'{query_string}&signature={m.hexdigest()}', timeout=timeout) as response:
            self.response = response
            return await self._handle_response(self.response)

    async def _other_signed_fast(self, method, uri: str, request_body: List[Tuple[str, str]], timeout: float = BaseClient.REQUEST_TIMEOUT):
        request_body.append(('timestamp', f'{time.time() * 1000 + self.timestamp_offset:.0f}'))
        query_string = '&'.join(f'{data[0]}={data[1]}' for data in request_body)
        m = hmac.new(self.API_SECRET.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256)
        request_body.append(('signature', m.hexdigest()))
        async with getattr(self.session, method)(uri, data=request_body, timeout=timeout) as response:
            self.response = response
            return await self._handle_response(self.response)

    async def _handle_response(self, response: aiohttp.ClientResponse):
        """Internal helper for handling API responses from the Binance server.
        Raises the appropriate exceptions when necessary; otherwise, returns the
        response.
        """
        if not (200 <= response.status < 300):
            raise BinanceAPIException(response, response.status, await response.text())
        try:
            return await response.json()
        except ValueError:
            txt = await response.text()
            raise BinanceRequestException(f'Invalid Response: {txt}')

    async def _request_api(self, method, path, signed=False, version=None, **kwargs):
        uri = self._create_api_uri(path, signed, version)
        return await self._request(method, uri, signed, **kwargs)

    async def _request_futures_api(self, method, path, signed=False, **kwargs) -> Dict:
        uri = self._create_futures_api_uri(path)
        return await self._request(method, uri, signed, True, **kwargs)

    async def _request_tfutures_api(self, method, path, signed=False, **kwargs) -> Dict:
        uri = self._create_tfutures_api_uri(path)
        return await self._request(method, uri, signed, True, **kwargs)

    async def _request_margin_api(self, method, path, signed=False, **kwargs) -> Dict:
        uri = self._create_margin_api_uri(path)
        return await self._request(method, uri, signed, **kwargs)

    async def _request_website(self, method, path, signed=False, **kwargs) -> Dict:
        uri = self._create_website_uri(path)
        return await self._request(method, uri, signed, **kwargs)

    async def _get(self, path, signed=False, version=None, **kwargs):
        return await self._request_api('get', path, signed, version, **kwargs)

    async def _post(self, path, signed=False, version=None, **kwargs):
        return await self._request_api('post', path, signed, version, **kwargs)

    async def _put(self, path, signed=False, version=None, **kwargs):
        return await self._request_api('put', path, signed, version, **kwargs)

    async def _delete(self, path, signed=False, version=None, **kwargs):
        return await self._request_api('delete', path, signed, version, **kwargs)

    # Exchange Endpoints

    async def get_products(self) -> Dict:
        products = await self._request_website('get', 'exchange-api/v1/public/asset-service/product/get-products')
        return products

    get_products.__doc__ = Client.get_products.__doc__

    async def get_exchange_info(self) -> Dict:
        return await self._get('exchangeInfo')

    get_exchange_info.__doc__ = Client.get_exchange_info.__doc__

    async def get_exchange_info_fast(self, timeout: float = BaseClient.REQUEST_TIMEOUT) -> Dict:
        return await self._request_fast('get', self.get_exchange_info_url, '', timeout)

    async def get_symbol_info(self, symbol: str) -> Dict:
        res = await self.get_exchange_info()

        for item in res['symbols']:
            if item['symbol'] == symbol:
                return item

        return {}

    get_symbol_info.__doc__ = Client.get_symbol_info.__doc__

    async def get_symbol_info_fast(self, symbol: str, timeout: float = BaseClient.REQUEST_TIMEOUT) -> Dict:
        res = await self.get_exchange_info_fast(timeout)

        for item in res['symbols']:
            if item['symbol'] == symbol:
                return item

        return {}

    # General Endpoints

    async def ping(self) -> Dict:
        return await self._get('ping')

    ping.__doc__ = Client.ping.__doc__

    async def ping_fast(self, timeout: float = BaseClient.REQUEST_TIMEOUT) -> Dict:
        return await self._request_fast('get', self.ping_url, '', timeout)

    async def get_server_time(self) -> Dict:
        return await self._get('time')

    get_server_time.__doc__ = Client.get_server_time.__doc__

    async def get_server_time_fast(self, timeout: float = BaseClient.REQUEST_TIMEOUT) -> Dict:
        return await self._request_fast('get', self.get_server_time_url, '', timeout)

    # Market Data Endpoints

    async def get_all_tickers(self):
        return await self._get('ticker/price')

    get_all_tickers.__doc__ = Client.get_all_tickers.__doc__

    async def get_orderbook_tickers(self):
        return await self._get('ticker/bookTicker')

    get_orderbook_tickers.__doc__ = Client.get_orderbook_tickers.__doc__

    async def get_orderbook_tickers_fast(self, timeout: float = BaseClient.REQUEST_TIMEOUT):
        return await self._request_fast('get', self.get_orderbook_tickers_url, '', timeout)

    async def get_order_book(self, **params) -> Dict:
        return await self._get('depth', data=params)

    get_order_book.__doc__ = Client.get_order_book.__doc__

    async def get_order_book_fast(self, query_string: str, timeout: float = BaseClient.REQUEST_TIMEOUT) -> Dict:
        return await self._request_fast('get', self.get_order_book_url, query_string, timeout)

    async def get_recent_trades(self, **params) -> Dict:
        return await self._get('trades', data=params)

    get_recent_trades.__doc__ = Client.get_recent_trades.__doc__

    async def get_historical_trades(self, **params) -> Dict:
        return await self._get('historicalTrades', data=params)

    get_historical_trades.__doc__ = Client.get_historical_trades.__doc__

    async def get_aggregate_trades(self, **params) -> Dict:
        return await self._get('aggTrades', data=params)

    get_aggregate_trades.__doc__ = Client.get_aggregate_trades.__doc__

    async def get_aggregate_trades_fast(self, query_string: str, timeout: float = BaseClient.REQUEST_TIMEOUT) -> Dict:
        return await self._request_fast('get', self.get_aggregate_trades_url, query_string, timeout)

    async def aggregate_trade_iter(self, symbol, start_str=None, last_id=None):
        if start_str is not None and last_id is not None:
            raise ValueError(
                'start_time and last_id may not be simultaneously specified.')

        # If there's no last_id, get one.
        if last_id is None:
            # Without a last_id, we actually need the first trade.  Normally,
            # we'd get rid of it. See the next loop.
            if start_str is None:
                trades = await self.get_aggregate_trades(symbol=symbol, fromId=0)
            else:
                # The difference between startTime and endTime should be less
                # or equal than an hour and the result set should contain at
                # least one trade.
                start_ts = convert_ts_str(start_str)
                # If the resulting set is empty (i.e. no trades in that interval)
                # then we just move forward hour by hour until we find at least one
                # trade or reach present moment
                while True:
                    end_ts = start_ts + (60 * 60 * 1000)
                    trades = await self.get_aggregate_trades(
                        symbol=symbol,
                        startTime=start_ts,
                        endTime=end_ts)
                    if len(trades) > 0:
                        break
                    # If we reach present moment and find no trades then there is
                    # nothing to iterate, so we're done
                    if end_ts > int(time.time() * 1000):
                        return
                    start_ts = end_ts
            for t in trades:
                yield t
            last_id = trades[-1][self.AGG_ID]

        while True:
            # There is no need to wait between queries, to avoid hitting the
            # rate limit. We're using blocking IO, and as long as we're the
            # only thread running calls like this, Binance will automatically
            # add the right delay time on their end, forcing us to wait for
            # data. That really simplifies this function's job. Binance is
            # fucking awesome.
            trades = await self.get_aggregate_trades(symbol=symbol, fromId=last_id)
            # fromId=n returns a set starting with id n, but we already have
            # that one. So get rid of the first item in the result set.
            trades = trades[1:]
            if len(trades) == 0:
                return
            for t in trades:
                yield t
            last_id = trades[-1][self.AGG_ID]

    aggregate_trade_iter.__doc__ = Client.aggregate_trade_iter.__doc__

    async def get_klines(self, **params) -> Dict:
        return await self._get('klines', data=params)

    get_klines.__doc__ = Client.get_klines.__doc__

    async def _klines(self, klines_type: HistoricalKlinesType = HistoricalKlinesType.SPOT, **params) -> Dict:
        if 'endTime' in params and not params['endTime']:
            del params['endTime']
        if HistoricalKlinesType.SPOT == klines_type:
            return await self.get_klines(**params)
        elif HistoricalKlinesType.FUTURES == klines_type:
            return await self.futures_klines(**params)
        else:
            raise NotImplementedException(klines_type)

    _klines.__doc__ = Client._klines.__doc__

    async def _get_earliest_valid_timestamp(self, symbol, interval,
                                            klines_type: HistoricalKlinesType = HistoricalKlinesType.SPOT):
        kline = await self._klines(
            klines_type=klines_type,
            symbol=symbol,
            interval=interval,
            limit=1,
            startTime=0,
            endTime=int(time.time() * 1000)
        )
        return kline[0][0]

    _get_earliest_valid_timestamp.__doc__ = Client._get_earliest_valid_timestamp.__doc__

    async def get_historical_klines(self, symbol, interval, start_str, end_str=None, limit=500,
                                    klines_type: HistoricalKlinesType = HistoricalKlinesType.SPOT):
        return await self._historical_klines(symbol, interval, start_str, end_str=end_str, limit=limit, klines_type=klines_type)

    get_historical_klines.__doc__ = Client.get_historical_klines.__doc__

    async def _historical_klines(self, symbol, interval, start_str, end_str=None, limit=500,
                                 klines_type: HistoricalKlinesType = HistoricalKlinesType.SPOT):

        # init our list
        output_data = []

        # convert interval to useful value in seconds
        timeframe = interval_to_milliseconds(interval)

        # convert our date strings to milliseconds
        start_ts = convert_ts_str(start_str)

        # establish first available start timestamp
        first_valid_ts = await self._get_earliest_valid_timestamp(symbol, interval, klines_type)
        start_ts = max(start_ts, first_valid_ts)

        # if an end time was passed convert it
        end_ts = convert_ts_str(end_str)

        idx = 0
        while True:
            # fetch the klines from start_ts up to max 500 entries or the end_ts if set
            temp_data = await self._klines(
                klines_type=klines_type,
                symbol=symbol,
                interval=interval,
                limit=limit,
                startTime=start_ts,
                endTime=end_ts
            )

            # handle the case where exactly the limit amount of data was returned last loop
            if not len(temp_data):
                break

            # append this loops data to our output data
            output_data += temp_data

            # set our start timestamp using the last value in the array
            start_ts = temp_data[-1][0]

            idx += 1
            # check if we received less than the required limit and exit the loop
            if len(temp_data) < limit:
                # exit the while loop
                break

            # increment next call by our timeframe
            start_ts += timeframe

            # sleep after every 3rd call to be kind to the API
            if idx % 3 == 0:
                await asyncio.sleep(1)

        return output_data

    _historical_klines.__doc__ = Client._historical_klines.__doc__

    async def get_historical_klines_generator(self, symbol, interval, start_str, end_str=None,
                                              klines_type: HistoricalKlinesType = HistoricalKlinesType.SPOT):
        return self._historical_klines_generator(symbol, interval, start_str, end_str=end_str, klines_type=klines_type)

    get_historical_klines_generator.__doc__ = Client.get_historical_klines_generator.__doc__

    async def _historical_klines_generator(self, symbol, interval, start_str, end_str=None,
                                           klines_type: HistoricalKlinesType = HistoricalKlinesType.SPOT):

        # setup the max limit
        limit = 500

        # convert interval to useful value in seconds
        timeframe = interval_to_milliseconds(interval)

        # convert our date strings to milliseconds
        start_ts = convert_ts_str(start_str)

        # establish first available start timestamp
        first_valid_ts = await self._get_earliest_valid_timestamp(symbol, interval, klines_type)
        start_ts = max(start_ts, first_valid_ts)

        # if an end time was passed convert it
        end_ts = convert_ts_str(end_str)

        idx = 0
        while True:
            # fetch the klines from start_ts up to max 500 entries or the end_ts if set
            output_data = await self._klines(
                klines_type=klines_type,
                symbol=symbol,
                interval=interval,
                limit=limit,
                startTime=start_ts,
                endTime=end_ts
            )

            # handle the case where exactly the limit amount of data was returned last loop
            if not len(output_data):
                break

            # yield data
            for o in output_data:
                yield o

            # set our start timestamp using the last value in the array
            start_ts = output_data[-1][0]

            idx += 1
            # check if we received less than the required limit and exit the loop
            if len(output_data) < limit:
                # exit the while loop
                break

            # increment next call by our timeframe
            start_ts += timeframe

            # sleep after every 3rd call to be kind to the API
            if idx % 3 == 0:
                await asyncio.sleep(1)

    _historical_klines_generator.__doc__ = Client._historical_klines_generator.__doc__

    async def get_avg_price(self, **params):
        return await self._get('avgPrice', data=params, version=self.PRIVATE_API_VERSION)

    get_avg_price.__doc__ = Client.get_avg_price.__doc__

    async def get_avg_price_fast(self, query_string: str, timeout: float = BaseClient.REQUEST_TIMEOUT) -> Dict:
        return await self._request_fast('get', self.get_avg_price_url, query_string, timeout)

    async def get_ticker(self, **params):
        return await self._get('ticker/24hr', data=params)

    get_ticker.__doc__ = Client.get_ticker.__doc__

    async def get_ticker_fast(self, query_string: str, timeout: float = BaseClient.REQUEST_TIMEOUT):
        return await self._request_fast('get', self.get_ticker_url, query_string, timeout)

    async def get_symbol_ticker(self, **params):
        return await self._get('ticker/price', data=params, version=self.PRIVATE_API_VERSION)

    get_symbol_ticker.__doc__ = Client.get_symbol_ticker.__doc__

    async def get_orderbook_ticker(self, **params):
        return await self._get('ticker/bookTicker', data=params, version=self.PRIVATE_API_VERSION)

    get_orderbook_ticker.__doc__ = Client.get_orderbook_ticker.__doc__

    async def get_orderbook_ticker_fast(self, query_string: str, timeout: float = BaseClient.REQUEST_TIMEOUT):
        return await self._request_fast('get', self.get_orderbook_ticker_url, query_string, timeout)

    # Account Endpoints

    async def create_order(self, **params):
        return await self._post('order', True, data=params)

    create_order.__doc__ = Client.create_order.__doc__

    async def create_order_fast(self, request_body: List[Tuple[str, str]], timeout: float = BaseClient.REQUEST_TIMEOUT):
        return await self._other_signed_fast('post', self.create_order_url, request_body, timeout)

    async def create_oco_order(self, **params):
        return await self._post('order/oco', True, data=params)

    create_oco_order.__doc__ = Client.create_oco_order.__doc__

    async def create_test_order(self, **params):
        return await self._post('order/test', True, data=params)

    create_test_order.__doc__ = Client.create_test_order.__doc__

    async def get_order(self, **params):
        return await self._get('order', True, data=params)

    get_order.__doc__ = Client.get_order.__doc__

    async def get_order_fast(self, query_string: str, timeout: float = BaseClient.REQUEST_TIMEOUT):
        return await self._get_signed_fast(self.get_order_url, query_string, timeout)

    async def get_all_orders(self, **params):
        return await self._get('allOrders', True, data=params)

    get_all_orders.__doc__ = Client.get_all_orders.__doc__

    async def get_all_orders_fast(self, query_string: str, timeout: float = BaseClient.REQUEST_TIMEOUT):
        return await self._get_signed_fast(self.get_all_orders_url, query_string, timeout)

    async def cancel_order(self, **params):
        return await self._delete('order', True, data=params)

    cancel_order.__doc__ = Client.cancel_order.__doc__

    async def cancel_order_fast(self, request_body: List[Tuple[str, str]], timeout: float = BaseClient.REQUEST_TIMEOUT):
        return await self._other_signed_fast('delete', self.cancel_order_url, request_body, timeout)

    async def get_open_orders(self, **params):
        return await self._get('openOrders', True, data=params)

    get_open_orders.__doc__ = Client.get_open_orders.__doc__

    async def get_open_orders_fast(self, query_string: str, timeout: float = BaseClient.REQUEST_TIMEOUT):
        return await self._get_signed_fast(self.get_open_orders_url, query_string, timeout)

    async def cancel_oco(self, **params):
        return await self._delete('orderList', True, data=params)

    cancel_oco.__doc__ = Client.cancel_oco.__doc__

    async def get_oco(self, **params):
        return await self._get('orderList', True, data=params)

    get_oco.__doc__ = Client.get_oco.__doc__

    async def get_all_ocos(self, **params):
        return await self._get('allOrderList', True, data=params)

    get_all_ocos.__doc__ = Client.get_all_ocos.__doc__

    async def get_open_ocos(self, **params):
        return await self._get('openOrderList', True, data=params)

    get_open_ocos.__doc__ = Client.get_open_ocos.__doc__

    async def cancel_orders(self, **params):
        return await self._delete('openOrders', True, data=params)

    cancel_orders.__doc__ = Client.cancel_orders.__doc__

    async def cancel_orders_fast(self, request_body: List[Tuple[str, str]], timeout: float = BaseClient.REQUEST_TIMEOUT):
        return await self._other_signed_fast('delete', self.cancel_orders_url, request_body, timeout)

    # User Stream Endpoints
    async def get_account(self, **params):
        return await self._get('account', True, data=params)

    get_account.__doc__ = Client.get_account.__doc__

    async def get_account_fast(self, query_string: str, timeout: float = BaseClient.REQUEST_TIMEOUT):
        return await self._get_signed_fast(self.get_account_url, query_string, timeout)

    async def get_asset_balance(self, asset, **params):
        res = await self.get_account(**params)
        # find asset balance in list of balances
        if "balances" in res:
            for bal in res['balances']:
                if bal['asset'].lower() == asset.lower():
                    return bal
        return None

    get_asset_balance.__doc__ = Client.get_asset_balance.__doc__

    async def get_my_trades(self, **params):
        return await self._get('myTrades', True, data=params)

    get_my_trades.__doc__ = Client.get_my_trades.__doc__

    async def get_my_trades_fast(self, query_string: str, timeout: float = BaseClient.REQUEST_TIMEOUT):
        return await self._get_signed_fast(self.get_my_trades_url, query_string, timeout)

    async def get_system_status(self):
        return await self._request_margin_api('get', 'system/status')

    get_system_status.__doc__ = Client.get_system_status.__doc__

    async def get_account_status(self, **params):
        return await self._request_margin_api('get', 'account/status', True, data=params)

    get_account_status.__doc__ = Client.get_account_status.__doc__

    async def get_account_api_trading_status(self, **params):
        return await self._request_margin_api('get', 'account/apiTradingStatus', True, data=params)

    get_account_api_trading_status.__doc__ = Client.get_account_api_trading_status.__doc__

    async def get_dust_log(self, **params):
        return await self._request_margin_api('get', 'asset/dribblet', True, data=params)

    get_dust_log.__doc__ = Client.get_dust_log.__doc__

    async def transfer_dust(self, **params):
        return await self._request_margin_api('post', 'asset/dust', True, data=params)

    transfer_dust.__doc__ = Client.transfer_dust.__doc__

    async def get_asset_dividend_history(self, **params):
        return await self._request_margin_api('get', 'asset/assetDividend', True, data=params)

    get_asset_dividend_history.__doc__ = Client.get_asset_dividend_history.__doc__

    async def make_universal_transfer(self, **params):
        return await self._request_margin_api('post', 'asset/transfer', signed=True, data=params)

    make_universal_transfer.__doc__ = Client.make_universal_transfer.__doc__

    async def query_universal_transfer_history(self, **params):
        return await self._request_margin_api('get', 'asset/transfer', signed=True, data=params)

    query_universal_transfer_history.__doc__ = Client.query_universal_transfer_history.__doc__

    async def get_trade_fee(self, **params):
        return await self._request_margin_api('get', 'asset/tradeFee', True, data=params)

    get_trade_fee.__doc__ = Client.get_trade_fee.__doc__

    async def get_asset_details(self, **params):
        return await self._request_margin_api('get', 'asset/assetDetail', True, data=params)

    get_asset_details.__doc__ = Client.get_asset_details.__doc__

    # Withdraw Endpoints

    async def withdraw(self, **params):
        return await self._request_margin_api('post', 'capital/withdraw/apply', True, data=params)

    withdraw.__doc__ = Client.withdraw.__doc__

    async def get_deposit_history(self, **params):
        return await self._request_margin_api('get', 'capital/deposit/hisrec', True, data=params)

    get_deposit_history.__doc__ = Client.get_deposit_history.__doc__

    async def get_withdraw_history(self, **params):
        return await self._request_margin_api('get', 'capital/withdraw/history', True, data=params)

    get_withdraw_history.__doc__ = Client.get_withdraw_history.__doc__

    async def get_withdraw_history_id(self, withdraw_id, **params):
        result = await self.get_withdraw_history(**params)

        for entry in result:
            if 'id' in entry and entry['id'] == withdraw_id:
                return entry

        raise Exception("There is no entry with withdraw id", result)

    get_withdraw_history_id.__doc__ = Client.get_withdraw_history_id.__doc__

    async def get_deposit_address(self, **params):
        return await self._request_margin_api('get', 'capital/deposit/address', True, data=params)

    get_deposit_address.__doc__ = Client.get_deposit_address.__doc__

    # User Stream Endpoints

    async def stream_get_listen_key(self):
        res = await self._post('userDataStream', False, data={})
        return res['listenKey']

    stream_get_listen_key.__doc__ = Client.stream_get_listen_key.__doc__

    async def stream_keepalive(self, listenKey):
        params = {
            'listenKey': listenKey
        }
        return await self._put('userDataStream', False, data=params)

    stream_keepalive.__doc__ = Client.stream_keepalive.__doc__

    async def stream_close(self, listenKey):
        params = {
            'listenKey': listenKey
        }
        return await self._delete('userDataStream', False, data=params)

    stream_close.__doc__ = Client.stream_close.__doc__

    # Margin Trading Endpoints
    async def get_margin_account(self, **params):
        return await self._request_margin_api('get', 'margin/account', True, data=params)

    get_margin_account.__doc__ = Client.get_margin_account.__doc__

    async def get_margin_asset(self, **params):
        return await self._request_margin_api('get', 'margin/asset', data=params)

    async def get_margin_symbol(self, **params):
        return await self._request_margin_api('get', 'margin/pair', data=params)

    async def get_margin_price_index(self, **params):
        return await self._request_margin_api('get', 'margin/priceIndex', data=params)

    async def transfer_margin_to_spot(self, **params):
        params['type'] = 2
        return await self._request_margin_api('post', 'margin/transfer', signed=True, data=params)

    async def transfer_spot_to_margin(self, **params):
        params['type'] = 1
        return await self._request_margin_api('post', 'margin/transfer', signed=True, data=params)

    async def create_margin_loan(self, **params):
        return await self._request_margin_api('post', 'margin/loan', signed=True, data=params)

    async def repay_margin_loan(self, **params):
        return await self._request_margin_api('post', 'margin/repay', signed=True, data=params)

    async def create_margin_order(self, **params):
        return await self._request_margin_api('post', 'margin/order', signed=True, data=params)

    async def create_margin_order_fast(self, request_body: List[Tuple[str, str]], timeout: float = BaseClient.REQUEST_TIMEOUT):
        return await self._other_signed_fast('post', self.create_margin_order_url, request_body, timeout)

    async def cancel_margin_order(self, **params):
        return await self._request_margin_api('delete', 'margin/order', signed=True, data=params)

    async def get_margin_loan_details(self, **params):
        return await self._request_margin_api('get', 'margin/loan', signed=True, data=params)

    async def get_margin_repay_details(self, **params):
        return await self._request_margin_api('get', 'margin/repay', signed=True, data=params)

    async def get_margin_order(self, **params):
        return await self._request_margin_api('get', 'margin/order', signed=True, data=params)

    async def get_open_margin_orders(self, **params):
        return await self._request_margin_api('get', 'margin/openOrders', signed=True, data=params)

    async def get_all_margin_orders(self, **params):
        return await self._request_margin_api('get', 'margin/allOrders', signed=True, data=params)

    async def get_margin_trades(self, **params):
        return await self._request_margin_api('get', 'margin/myTrades', signed=True, data=params)

    async def get_max_margin_loan(self, **params):
        return await self._request_margin_api('get', 'margin/maxBorrowable', signed=True, data=params)

    async def get_max_margin_transfer(self, **params):
        return await self._request_margin_api('get', 'margin/maxTransferable', signed=True, data=params)

    async def margin_stream_get_listen_key(self):
        res = await self._request_margin_api('post', 'userDataStream', signed=False, data={})
        return res['listenKey']

    async def margin_stream_keepalive(self, listenKey):
        params = {
            'listenKey': listenKey
        }
        return await self._request_margin_api('put', 'userDataStream', signed=False, data=params)

    async def margin_stream_close(self, listenKey):
        params = {
            'listenKey': listenKey
        }
        return await self._request_margin_api('delete', 'userDataStream', signed=False, data=params)

    async def isolated_stream_get_listen_key(self, symbol):
        res = await self._request_margin_api('post', 'userDataStream/isolated', signed=False, data={'symbol': symbol})
        return res['listenKey']

    async def isolated_stream_keepalive(self, listenKey, symbol):
        params = {'listenKey': listenKey, 'symbol': symbol}
        return await self._request_margin_api('put', 'userDataStream/isolated', signed=False, data=params)

    async def isolated_stream_close(self, listenKey, symbol):
        params = {'listenKey': listenKey, 'symbol': symbol}
        return await self._request_margin_api('delete', 'userDataStream/isolated', signed=False, data=params)

    async def get_all_margin_assets(self, **params):
        return await self._request_margin_api('get', 'margin/allAssets', data=params)

    async def get_all_margin_symbols(self, **params):
        return await self._request_margin_api('get', 'margin/allPairs', data=params)

    async def get_margin_transfer_history(self, **params):
        return await self._request_margin_api('get', 'margin/transfer', signed=True, data=params)

    async def get_margin_interest_history(self, **params):
        return await self._request_margin_api('get', 'margin/interestHistory', signed=True, data=params)

    async def get_force_liquidation_records(self, **params):
        return await self._request_margin_api('get', 'margin/forceLiquidationRec', signed=True, data=params)

    async def create_isolated_margin_account(self, **params):
        return await self._request_margin_api('post', 'margin/isolated/create', signed=True, data=params)

    async def transfer_isolated_margin_account(self, **params):
        return await self._request_margin_api('post', 'margin/isolated/transfer', signed=True, data=params)

    async def get_isolated_margin_transfer_history(self, **params):
        return await self._request_margin_api('get', 'margin/isolated/transfer', signed=True, data=params)

    async def get_all_coins_info(self, **params):
        return await self._request_margin_api('get', 'capital/config/getall', signed=True, data=params)

    async def get_isolated_margin_account_info(self, **params):
        return await self._request_margin_api('get', 'margin/isolated/account', signed=True, data=params)

    async def get_isolated_margin_symbol(self, **params):
        return await self._request_margin_api('get', 'margin/isolated/pair', signed=True, data=params)

    async def get_all_isolated_margin_symbols(self, **params):
        return await self._request_margin_api('get', 'margin/isolated/allPairs', signed=True, data=params)

    # Lending Endpoints

    async def get_lending_product_list(self, **params):
        return await self._request_margin_api('get', 'lending/daily/product/list', signed=True, data=params)

    async def get_lending_daily_quota_left(self, **params):
        return await self._request_margin_api('get', 'lending/daily/userLeftQuota', signed=True, data=params)

    async def purchase_lending_product(self, **params):
        return await self._request_margin_api('post', 'lending/daily/purchase', signed=True, data=params)

    async def get_lending_daily_redemption_quota(self, **params):
        return await self._request_margin_api('get', 'lending/daily/userRedemptionQuota', signed=True, data=params)

    async def redeem_lending_product(self, **params):
        return await self._request_margin_api('post', 'lending/daily/redeem', signed=True, data=params)

    async def get_lending_position(self, **params):
        return await self._request_margin_api('get', 'lending/daily/token/position', signed=True, data=params)

    async def get_fixed_activity_project_list(self, **params):
        return await self._request_margin_api('get', 'lending/project/list', signed=True, data=params)

    async def get_lending_account(self, **params):
        return await self._request_margin_api('get', 'lending/union/account', signed=True, data=params)

    async def get_lending_purchase_history(self, **params):
        return await self._request_margin_api('get', 'lending/union/purchaseRecord', signed=True, data=params)

    async def get_lending_redemption_history(self, **params):
        return await self._request_margin_api('get', 'lending/union/redemptionRecord', signed=True, data=params)

    async def get_lending_interest_history(self, **params):
        return await self._request_margin_api('get', 'lending/union/interestHistory', signed=True, data=params)

    async def change_fixed_activity_to_daily_position(self, **params):
        return await self._request_margin_api('post', 'lending/positionChanged', signed=True, data=params)

    # Sub Accounts

    async def get_sub_account_list(self, **params):
        return await self._request_margin_api('get', 'sub-account/list', True, data=params)

    async def get_sub_account_transfer_history(self, **params):
        return await self._request_margin_api('get', 'sub-account/sub/transfer/history', True, data=params)

    async def create_sub_account_transfer(self, **params):
        return await self._request_margin_api('post', 'sub-account/universalTransfer', True, data=params)

    async def get_sub_account_assets(self, **params):
        return await self._request('get', self.MARGIN_API_URL + '/v3/sub-account/assets', True, data=params)

    # Futures API

    async def futures_ping(self):
        return await self._request_futures_api('get', 'ping')

    async def futures_time(self):
        return await self._request_futures_api('get', 'time')

    async def futures_exchange_info(self):
        return await self._request_futures_api('get', 'exchangeInfo')

    async def futures_order_book(self, **params):
        return await self._request_futures_api('get', 'depth', data=params)

    async def futures_recent_trades(self, **params):
        return await self._request_futures_api('get', 'trades', data=params)

    async def futures_historical_trades(self, **params):
        return await self._request_futures_api('get', 'historicalTrades', data=params)

    async def futures_aggregate_trades(self, **params):
        return await self._request_futures_api('get', 'aggTrades', data=params)

    async def futures_klines(self, **params):
        return await self._request_futures_api('get', 'klines', data=params)

    async def futures_continous_klines(self, **params):
        return await self._request_futures_api('get', 'continuousKlines', data=params)

    async def futures_historical_klines(self, symbol, interval, start_str, end_str=None, limit=500):
        return self._historical_klines(symbol, interval, start_str, end_str=end_str, limit=limit, klines_type=HistoricalKlinesType.FUTURES)

    async def futures_historical_klines_generator(self, symbol, interval, start_str, end_str=None):
        return self._historical_klines_generator(symbol, interval, start_str, end_str=end_str, klines_type=HistoricalKlinesType.FUTURES)

    async def futures_mark_price(self, **params):
        return await self._request_futures_api('get', 'premiumIndex', data=params)

    async def futures_funding_rate(self, **params):
        return await self._request_futures_api('get', 'fundingRate', data=params)

    async def futures_ticker(self, **params):
        return await self._request_futures_api('get', 'ticker/24hr', data=params)

    async def futures_symbol_ticker(self, **params):
        return await self._request_futures_api('get', 'ticker/price', data=params)

    async def futures_orderbook_ticker(self, **params):
        return await self._request_futures_api('get', 'ticker/bookTicker', data=params)

    async def futures_liquidation_orders(self, **params):
        return await self._request_futures_api('get', 'forceOrders', signed=True, data=params)

    async def futures_adl_quantile_estimate(self, **params):
        return await self._request_futures_api('get', 'adlQuantile', signed=True, data=params)

    async def futures_open_interest(self, **params):
        return await self._request_futures_api('get', 'openInterest', data=params)

    async def futures_leverage_bracket(self, **params):
        return await self._request_futures_api('get', 'leverageBracket', True, data=params)

    async def futures_account_transfer(self, **params):
        return await self._request_margin_api('post', 'futures/transfer', True, data=params)

    async def transfer_history(self, **params):
        return await self._request_margin_api('get', 'futures/transfer', True, data=params)

    async def futures_create_order(self, **params):
        return await self._request_futures_api('post', 'order', True, data=params)

    async def futures_place_batch_order(self, **params):
        query_string = urlencode(params)
        query_string = query_string.replace('%27', '%22')
        params['batchOrders'] = query_string[12:]
        return await self._request_futures_api('post', 'batchOrders', True, data=params)

    async def futures_get_order(self, **params):
        return await self._request_futures_api('get', 'order', True, data=params)

    async def futures_get_open_order(self, **params):
        return await self._request_futures_api('get', 'openOrder', True, data=params)

    async def futures_get_open_orders(self, **params):
        return await self._request_futures_api('get', 'openOrders', True, data=params)

    async def futures_get_all_orders(self, **params):
        return await self._request_futures_api('get', 'allOrders', True, data=params)

    async def futures_cancel_order(self, **params):
        return await self._request_futures_api('delete', 'order', True, data=params)

    async def futures_cancel_all_open_orders(self, **params):
        return await self._request_futures_api('delete', 'allOpenOrders', True, data=params)

    async def futures_cancel_orders(self, **params):
        return await self._request_futures_api('delete', 'batchOrders', True, data=params)

    async def futures_account_balance(self, **params):
        return await self._request_futures_api('get', 'balance', True, data=params)

    async def futures_account(self, **params):
        return await self._request_futures_api('get', 'account', True, data=params)

    async def futures_change_leverage(self, **params):
        return await self._request_futures_api('post', 'leverage', True, data=params)

    async def futures_change_margin_type(self, **params):
        return await self._request_futures_api('post', 'marginType', True, data=params)

    async def futures_change_position_margin(self, **params):
        return await self._request_futures_api('post', 'positionMargin', True, data=params)

    async def futures_position_margin_history(self, **params):
        return await self._request_futures_api('get', 'positionMargin/history', True, data=params)

    async def futures_position_information(self, **params):
        return await self._request_futures_api('get', 'positionRisk', True, data=params)

    async def futures_account_trades(self, **params):
        return await self._request_futures_api('get', 'userTrades', True, data=params)

    async def futures_income_history(self, **params):
        return await self._request_futures_api('get', 'income', True, data=params)

    async def transfer_futures_to_spot(self, **params):
        params['type'] = 2
        return await self._request_margin_api('post', 'futures/transfer', signed=True, data=params)

    async def transfer_spot_to_futures(self, **params):
        params['type'] = 1
        return await self._request_margin_api('post', 'futures/transfer', signed=True, data=params)

    async def futures_change_position_mode(self, **params):
        return await self._request_futures_api('post', 'positionSide/dual', True, data=params)

    async def futures_get_position_mode(self, **params):
        return await self._request_futures_api('get', 'positionSide/dual', True, data=params)

    async def futures_change_multi_assets_mode(self, multiAssetsMargin: bool):
        params = {
            'true' if multiAssetsMargin else 'false'
        }
        return await self._request_futures_api('post', 'multiAssetsMargin', True, data=params)

    async def futures_get_multi_assets_mode(self):
        return await self._request_futures_api('get', 'multiAssetsMargin', True)

    async def futures_stream_get_listen_key(self):
        res = await self._request_futures_api('post', 'listenKey', signed=False, data={})
        return res['listenKey']

    async def futures_stream_keepalive(self, listenKey):
        params = {
            'listenKey': listenKey
        }
        return await self._request_futures_api('put', 'listenKey', signed=False, data=params)

    async def futures_stream_close(self, listenKey):
        params = {
            'listenKey': listenKey
        }
        return await self._request_futures_api('delete', 'listenKey', signed=False, data=params)

    # Traditional Futures API (Manual Update)

    async def tfutures_ping(self):
        return await self._request_tfutures_api('get', 'ping')

    async def tfutures_time(self):
        return await self._request_tfutures_api('get', 'time')

    async def tfutures_exchange_info(self):
        return await self._request_tfutures_api('get', 'exchangeInfo')

    async def tfutures_order_book(self, **params):
        return await self._request_tfutures_api('get', 'depth', data=params)

    async def tfutures_recent_trades(self, **params):
        return await self._request_tfutures_api('get', 'trades', data=params)

    async def tfutures_historical_trades(self, **params):
        return await self._request_tfutures_api('get', 'historicalTrades', data=params)

    async def tfutures_aggregate_trades(self, **params):
        return await self._request_tfutures_api('get', 'aggTrades', data=params)

    async def tfutures_mark_price(self, **params):
        return await self._request_tfutures_api('get', 'premiumIndex', data=params)

    async def tfutures_klines(self, **params):
        return await self._request_tfutures_api('get', 'klines', data=params)

    async def tfutures_continuous_klines(self, **params):
        return await self._request_tfutures_api('get', 'continuousKlines', data=params)

    async def tfutures_index_price_klines(self, **params):
        return await self._request_tfutures_api('get', 'indexPriceKlines', data=params)

    async def tfutures_mark_price_klines(self, **params):
        return await self._request_tfutures_api('get', 'markPriceKlines', data=params)

    async def tfutures_ticker(self, **params):
        return await self._request_tfutures_api('get', 'ticker/24hr', data=params)

    async def tfutures_symbol_ticker(self, **params):
        return await self._request_tfutures_api('get', 'ticker/price', data=params)

    async def tfutures_orderbook_ticker(self, **params):
        return await self._request_tfutures_api('get', 'ticker/bookTicker', data=params)

    async def tfutures_liquidation_orders(self, **params):
        return await self._request_tfutures_api('get', 'allForceOrders', data=params)

    async def tfutures_open_interest(self, **params):
        return await self._request_tfutures_api('get', 'openInterest', data=params)

    async def tfutures_leverage_bracket(self, **params):
        return await self._request_tfutures_api('get', 'leverageBracket', data=params)

    async def transfer_tfutures_to_spot(self, **params):
        params['type'] = 4
        return await self._request_margin_api('post', 'futures/transfer', signed=True, data=params)

    async def transfer_spot_to_tfutures(self, **params):
        params['type'] = 3
        return await self._request_margin_api('post', 'futures/transfer', signed=True, data=params)

    async def tfutures_change_position_mode(self, **params):
        return await self._request_tfutures_api('post', 'positionSide/dual', True, data=params)

    async def tfutures_get_position_mode(self, **params):
        return await self._request_tfutures_api('get', 'positionSide/dual', True, data=params)

    async def tfutures_create_order(self, **params):
        return await self._request_tfutures_api('post', 'order', True, data=params)

    async def tfutures_batch_order(self, **params):
        return await self._request_tfutures_api('post', 'batchOrders', True, data=params)

    async def tfutures_get_order(self, **params):
        return await self._request_tfutures_api('get', 'order', True, data=params)

    async def tfutures_cancel_order(self, **params):
        return await self._request_tfutures_api('delete', 'order', True, data=params)

    async def tfutures_cancel_all_open_orders(self, **params):
        return await self._request_tfutures_api('delete', 'allOpenOrders', True, data=params)

    async def tfutures_cancel_orders(self, **params):
        return await self._request_tfutures_api('delete', 'batchOrders', True, data=params)

    async def tfutures_auto_cancel_all_open_orders(self, **params):
        return await self._request_tfutures_api('post', 'countdownCancelAll', True, data=params)

    async def tfutures_get_open_order(self, **params):
        return await self._request_tfutures_api('get', 'openOrder', True, data=params)

    async def tfutures_get_open_orders(self, **params):
        return await self._request_tfutures_api('get', 'openOrders', True, data=params)

    async def tfutures_get_all_orders(self, **params):
        return await self._request_tfutures_api('get', 'allOrders', True, data=params)

    async def tfutures_account_balance(self, **params):
        return await self._request_tfutures_api('get', 'balance', True, data=params)

    async def tfutures_account(self, **params):
        return await self._request_tfutures_api('get', 'account', True, data=params)

    async def tfutures_change_leverage(self, **params):
        return await self._request_tfutures_api('post', 'leverage', True, data=params)

    async def tfutures_change_margin_type(self, **params):
        return await self._request_tfutures_api('post', 'marginType', True, data=params)

    async def tfutures_change_position_margin(self, **params):
        return await self._request_tfutures_api('post', 'positionMargin', True, data=params)

    async def tfutures_position_margin_history(self, **params):
        return await self._request_tfutures_api('get', 'positionMargin/history', True, data=params)

    async def tfutures_position_information(self, **params):
        return await self._request_tfutures_api('get', 'positionRisk', True, data=params)

    async def tfutures_account_trades(self, **params):
        return await self._request_tfutures_api('get', 'userTrades', True, data=params)

    async def tfutures_income_history(self, **params):
        return await self._request_tfutures_api('get', 'income', True, data=params)

    async def tfuture_stream_get_listen_key(self):
        res = await self._request_tfutures_api('post', 'listenKey', signed=False, data={})
        return res['listenKey']

    async def tfuture_stream_keepalive(self, listenKey):
        params = {'listenKey': listenKey}
        return await self._request_tfutures_api('put', 'listenKey', signed=False, data=params)

    async def tfuture_stream_close(self, listenKey):
        params = {'listenKey': listenKey}
        return await self._request_tfutures_api('delete', 'listenKey', signed=False, data=params)
