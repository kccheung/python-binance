# coding=utf-8
import json


class BinanceAPIException(Exception):

    def __init__(self, response, status_code, text):
        self.code = 0
        self.message = ''
        try:
            json_res = json.loads(text)
        except json.JSONDecodeError:
            self.message = 'Invalid JSON error message from Binance: {}'.format(text)
        else:
            self.code = json_res['code']
            self.message = json_res['msg']
        self.status_code = status_code
        self.response = response
        self.request = getattr(response, 'request', None)

    def __str__(self):  # pragma: no cover
        return 'APIError(code=%s): %s' % (self.code, self.message)


class BinanceAPIException2(Exception):

    def __init__(self, response, status_code, text):
        self.code = 0
        self.message = ''
        self.data = {}
        try:
            json_res = json.loads(text)
        except json.JSONDecodeError:
            self.message = 'Invalid JSON error message from Binance: {}'.format(text)
        else:
            self.code = json_res['code']
            self.message = json_res['msg']
            if 'data' in json_res:
                self.data = json_res['data']
        self.status_code = status_code
        self.response = response
        self.request = getattr(response, 'request', None)

    def __str__(self):  # pragma: no cover
        return 'APIError(code=%s): %s' % (self.code, self.message)


class BinanceAPIException3(Exception):

    def __init__(self, response, status_code, text):
        self.code = 0
        self.message = ''
        self.message_detail = {}
        self.data = {}
        try:
            json_res = json.loads(text)
        except json.JSONDecodeError:
            self.message = 'Invalid JSON error message from Binance: {}'.format(text)
        else:
            if 'code' in json_res:
                self.code = json_res['code']
            if 'message' in json_res:
                self.message = json_res['message']
            if 'messageDetail' in json_res:
                self.message_detail = json_res['messageDetail']
            if 'data' in json_res:
                self.data = json_res['data']
        self.status_code = status_code
        self.response = response
        self.request = getattr(response, 'request', None)

    def __str__(self):  # pragma: no cover
        message_detail_str = ''
        if self.message_detail:
            message_detail_str = ' {' + ', '.join([str(kv[0]) + ': ' + str(kv[1]) for kv in self.message_detail.items()]) + '}'
        return 'APIError(code=%s): %s' % (self.code, self.message) + message_detail_str


class BinanceRequestException(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return 'BinanceRequestException: %s' % self.message


class BinanceOrderException(Exception):

    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return 'BinanceOrderException(code=%s): %s' % (self.code, self.message)


class BinanceOrderMinAmountException(BinanceOrderException):

    def __init__(self, value):
        message = "Amount must be a multiple of %s" % value
        super().__init__(-1013, message)


class BinanceOrderMinPriceException(BinanceOrderException):

    def __init__(self, value):
        message = "Price must be at least %s" % value
        super().__init__(-1013, message)


class BinanceOrderMinTotalException(BinanceOrderException):

    def __init__(self, value):
        message = "Total must be at least %s" % value
        super().__init__(-1013, message)


class BinanceOrderUnknownSymbolException(BinanceOrderException):

    def __init__(self, value):
        message = "Unknown symbol %s" % value
        super().__init__(-1013, message)


class BinanceOrderInactiveSymbolException(BinanceOrderException):

    def __init__(self, value):
        message = "Attempting to trade an inactive symbol %s" % value
        super().__init__(-1013, message)


class BinanceWebsocketUnableToConnect(Exception):
    pass


class NotImplementedException(Exception):
    def __init__(self, value):
        message = f'Not implemented: {value}'
        super().__init__(message)
