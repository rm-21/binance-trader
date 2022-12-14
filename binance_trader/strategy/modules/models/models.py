from binance.enums import *


class Side:
    Buy = SIDE_BUY
    Sell = SIDE_SELL


class FutureOrder:
    Limit = FUTURE_ORDER_TYPE_LIMIT
    Market = FUTURE_ORDER_TYPE_MARKET
    Stop = FUTURE_ORDER_TYPE_STOP
    StopMarket = FUTURE_ORDER_TYPE_STOP_MARKET
    TP = FUTURE_ORDER_TYPE_TAKE_PROFIT
    TPMarket = FUTURE_ORDER_TYPE_TAKE_PROFIT_MARKET
    LimitMaker = FUTURE_ORDER_TYPE_LIMIT_MAKER


class OrderResponse:
    Ack = ORDER_RESP_TYPE_ACK
    Res = ORDER_RESP_TYPE_RESULT
    Full = ORDER_RESP_TYPE_FULL


class OrderFillType:
    GTC = TIME_IN_FORCE_GTC
    IOC = TIME_IN_FORCE_IOC
    FOK = TIME_IN_FORCE_FOK
    GTC = TIME_IN_FORCE_GTX


class OrderStatus:
    New = ORDER_STATUS_NEW
    PartialFill = ORDER_STATUS_PARTIALLY_FILLED
    Filled = ORDER_STATUS_FILLED
    Cancelled = ORDER_STATUS_CANCELED
    PendingCancel = ORDER_STATUS_PENDING_CANCEL
    Rejected = ORDER_STATUS_REJECTED
    Expired = ORDER_STATUS_EXPIRED


class ContractType:
    Perpetual = "PERPETUAL"
    CurrentQtr = "CURRENT_QUARTER"
    NextQtr = "NEXT_QUARTER"
