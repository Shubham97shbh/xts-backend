from XTS.celery import app
from celery import group, shared_task
from XTSApp.models import SharedObject
import os
import logging
from datetime import datetime
from utils.server_request import Data
from utils.extras import get_atm, get_trade_ltp, get_otm, create_logs, check_finished
from strategies.updated_connect import UpdatedXTSConnect
from utils.config import *
import time
from dotenv import load_dotenv
import json

logger = logging.getLogger("BNFLogger")
formatter = logging.Formatter("%(asctime)s - %(message)s")
file_handler = logging.FileHandler("process_logs.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
load_dotenv()

# login to the XT server
# xt = UpdatedXTSConnect(
#     os.getenv("INTERACTIVE_KEY"), os.getenv("INTERACTIVE_SECRET"), "WEBAPI"
# )
# print("TOKEN: ", os.getenv("TOKEN"))
# xt.marketdata_login(token=os.getenv("TOKEN"), userId=os.getenv("USERID"))
# print("BALANCE", xt.get_holding("*****"))
# LOGIN BY DEMO CRED

xt = UpdatedXTSConnect(
    os.getenv("DEMO_INTERACTIVE_KEY"), os.getenv("DEMO_INTERACTIVE_SECRET"), "WEBAPI"
)
print("TOKEN: ", os.getenv("DEMO_TOKEN"))
xt.marketdata_login(token=os.getenv("DEMO_TOKEN"), userId=os.getenv("USERID"))
print("BALANCE", xt.get_holding("DELOPT"))

def stop_trade(process, process_id, trade):
    '''
    {102: {'task_id': "Failed: 'str' object has no attribute 'app'", 'logs': [], 
    'EMERGENCY_STOP': True, 'trade_side_dic': {'CE': {'instrument_id': 44164, 'strike': 47900, 
    'sl_point': 417.835, 'entry_price': 379.85}, 'PE': {'instrument_id': 44171, 'strike': 47900, 
    'sl_point': 458.81, 'entry_price': 417.1}}}} {'process_id': 102, 'trade': 'SELL'}

    params:
    process: Contains all the different id values that is in process
    id: Process ID which need to be stopped
    '''
    # variables for each data points
    global CE_MULTIPLE, OTM_GAP, PE_MULTIPLE, CURRENT_PE_MARGIN, CURRENT_CE_MARGIN, CURRENT_PNL, \
        EMERGENCY_STOP, CE_REENTRY, PE_REENTRY, SL_PERCENTAGE, QTY, TRADE, TRAIL_SL, CUR_CE_MULTIPLE, \
        CUR_PE_MULTIPLE, PORTFOLIO_RISK, SL_FLAG, TRADE_TYPE
    
    logs = SharedObject.get_value(process_id, key='logs')
    finished_flag = check_finished(logs)
    if finished_flag:
        return 'Already Square off or stoploss hit.'
    trade_side_dic = process['trade_side_dic']
    print(f'Stopping trade for {process}, ')
    if trade == 'SELL':
        # starting stop mechanism
        for side, trade_dic in trade_side_dic.items():
            # entry_price = trade_dic['entry_price']
            # sl_point = trade_dic['sl_point']
            if check_finished(logs, side):
                continue
            entry_price = 0
            sl_point = 0
            strike = trade_dic['strike']
            exchange_instrument_id = trade_dic['instrument_id']
            create_logs(logger, process_id,
                        f"1 | {datetime.now()} | Strategy Stopped by User {side}, {strike}, {entry_price}.")
            if not exchange_instrument_id:
                continue
            stoploss_order_res = xt.place_order(
                exchangeSegment=xt.EXCHANGE_NSEFO,
                exchangeInstrumentID=exchange_instrument_id,
                productType=xt.PRODUCT_NRML,
                orderType=xt.ORDER_TYPE_MARKET,
                orderSide=xt.TRANSACTION_TYPE_BUY,
                timeInForce=xt.VALIDITY_DAY,
                disclosedQuantity=0,
                orderQuantity=QTY,
                limitPrice=0,
                stopPrice=0,
                orderUniqueIdentifier="BNF_1",
            )
            print('Order-stopped-response--->', stoploss_order_res)
            order_id = stoploss_order_res["result"]["AppOrderID"]
            time.sleep(0.5)
            #change
            # sl_ltp = get_trade_ltp(xt, order_id=order_id)
            sl_ltp = 0
            CURRENT_PNL += entry_price - sl_ltp
            create_logs(
                logger, process_id, f"1 | {datetime.now()} | Square off remaining Trades: {str(strike)+side} | CMP: {sl_ltp} | SL: {sl_point}")
            print(
                f"1 | {datetime.now()} | Square off stop remaining Trades: {str(strike)+side} | CMP: {sl_ltp} | SL: {sl_point}")

    else:
        # starting stop mechanism
        for side, trade_dic in trade_side_dic.items():
            if check_finished(logs, side):
                continue
            entry_price = 0
            sl_point = 0
            strike = trade_dic['strike']
            exchange_instrument_id = trade_dic['instrument_id']
            create_logs(logger, process_id,
                        f"1 | {datetime.now()} | Strategy Stopped by User.")
            if not exchange_instrument_id:
                continue
            stoploss_order_res = xt.place_order(
                exchangeSegment=xt.EXCHANGE_NSEFO,
                exchangeInstrumentID=exchange_instrument_id,
                productType=xt.PRODUCT_NRML,
                orderType=xt.ORDER_TYPE_MARKET,
                orderSide=xt.TRANSACTION_TYPE_SELL,
                timeInForce=xt.VALIDITY_DAY,
                disclosedQuantity=0,
                orderQuantity=QTY,
                limitPrice=0,
                stopPrice=0,
                orderUniqueIdentifier="BNF_1",
            )
            order_id = stoploss_order_res["result"]["AppOrderID"]
            time.sleep(0.5)
            sl_ltp = get_trade_ltp(xt, order_id=order_id)
            CURRENT_PNL += entry_price + sl_ltp
            create_logs(logger,  process_id,
                        f"1 | {datetime.now()} | Square off remaining Trades: {str(strike)+side} | CMP: {sl_ltp} | SL: {sl_point}"
                        )
            create_logs(logger,  process_id,
                        f"{datetime.now()} | Stoploss hit {side}: {str(strike)+side} | CMP: {sl_ltp} | SL: {sl_point}"
                        )
    return "Stopped"

@shared_task(bind=True, max_retries=None)
def check_and_trail_sl_sell(self, exchange_instrument_id, strike, side, process_id, CUR_CE_MULTIPLE, CUR_PE_MULTIPLE):
    global CE_MULTIPLE, OTM_GAP, PE_MULTIPLE, CURRENT_PE_MARGIN, CURRENT_CE_MARGIN, CURRENT_PNL, \
        EMERGENCY_STOP, CE_REENTRY, PE_REENTRY, SL_PERCENTAGE, QTY, TRADE, TRAIL_SL, \
        PORTFOLIO_RISK, SL_FLAG, TRADE_TYPE
    
    process = SharedObject.get_value(process_id)
    print(f'Process after trading {process}')
    process_points = process['trade_side_dic'][side]
    entry_price = process_points['entry_price']
    sl_point = process_points['sl_point']
    EMERGENCY_STOP = process['EMERGENCY_STOP']
    create_logs(logger,  process_id,
                f"Starting Sell Check And Trail {str(strike)+side} | Entry: {entry_price}")
    while EMERGENCY_STOP:
        process = SharedObject.get_value(process_id)
        #check for square off mechanism
        if not process or (hasattr(self.request, 'revoke') and self.request.revoke):
            # If the task is revoked, stop the loop and exit
            return
        # Emergency stop
        EMERGENCY_STOP = process.get('EMERGENCY_STOP', True)
        cur_price = round(Data.get_ltp(exchange_instrument_id))
        
        if SL_FLAG.upper() == 'FALSE':
            continue
        pnl_percent = (entry_price - cur_price) / cur_price * 100
        if side == "CE":
            # ask
            CURRENT_CE_MARGIN = entry_price - cur_price
            if IS_POINTS:
                if CURRENT_CE_MARGIN >= CUR_CE_MULTIPLE:
                    sl_point -= TRAIL_SL
                    CUR_CE_MULTIPLE += CE_MULTIPLE
                    create_logs(logger,  process_id,
                                f"{datetime.now()} | Modified Order Placed: {str(strike)+side} | Entry: {entry_price} | UpdatedSL: {sl_point}"
                                )
            else:
                if pnl_percent >= CUR_CE_MULTIPLE:
                    sl_point = sl_point - (sl_point * (TRAIL_SL / 100))
                    CUR_CE_MULTIPLE += CE_MULTIPLE
                    create_logs(logger,  process_id,
                                f"{datetime.now()} | Modified Order Placed: {str(strike)+side} | Entry: {entry_price} | UpdatedSL: {sl_point}"
                                )

        if side == "PE":
            CURRENT_PE_MARGIN = entry_price - cur_price
            if IS_POINTS:
                if CURRENT_PE_MARGIN >= CUR_PE_MULTIPLE:
                    sl_point -= TRAIL_SL
                    CUR_PE_MULTIPLE += PE_MULTIPLE
                    create_logs(logger,  process_id,
                                f"{datetime.now()} | Modified Order Placed: {str(strike)+side} | Entry: {entry_price} | UpdatedSL: {sl_point}"
                                )
            else:
                if pnl_percent >= CUR_PE_MULTIPLE:
                    sl_point = sl_point - (sl_point * (TRAIL_SL / 100))
                    CUR_PE_MULTIPLE += PE_MULTIPLE
                    create_logs(logger,  process_id,
                                f"{datetime.now()} | Modified Order Placed: {str(strike)+side} | Entry: {entry_price} | UpdatedSL: {sl_point}"
                                )

        current_portfolio_value = float(CURRENT_CE_MARGIN + CURRENT_PE_MARGIN)
        print(current_portfolio_value)

        if cur_price >= sl_point:
            stoploss_order_res = xt.place_order(
                exchangeSegment=xt.EXCHANGE_NSEFO,
                exchangeInstrumentID=exchange_instrument_id,
                productType=xt.PRODUCT_NRML,
                orderType=xt.ORDER_TYPE_MARKET,
                orderSide=xt.TRANSACTION_TYPE_BUY,
                timeInForce=xt.VALIDITY_DAY,
                disclosedQuantity=0,
                orderQuantity=QTY,
                limitPrice=0,
                stopPrice=0,
                orderUniqueIdentifier="BNF_1",
            )
            order_id = stoploss_order_res["result"]["AppOrderID"]
            sl_ltp = get_trade_ltp(xt, order_id=order_id)
            CURRENT_PNL += entry_price - sl_ltp
            create_logs(logger,  process_id,
                        f"1 | {datetime.now()} | Stoploss hit {side}: {str(strike)+side} | CMP: {sl_ltp} | SL: {sl_point}"
                        )
            create_logs(logger,  process_id,
                        f"{datetime.now()} | Stoploss hit {side}: {str(strike)+side} | CMP: {sl_ltp} | SL: {sl_point}"
                        )
            return

        if QTY * (CURRENT_PE_MARGIN + CURRENT_CE_MARGIN) <= PORTFOLIO_RISK:
            order_res = xt.place_order(
                exchangeSegment=xt.EXCHANGE_NSEFO,
                exchangeInstrumentID=exchange_instrument_id,
                productType=xt.PRODUCT_NRML,
                orderType=xt.ORDER_TYPE_MARKET,
                orderSide=xt.TRANSACTION_TYPE_BUY,
                timeInForce=xt.VALIDITY_DAY,
                disclosedQuantity=0,
                orderQuantity=QTY,
                limitPrice=0,
                stopPrice=0,
                orderUniqueIdentifier="BNF_1",
            )
            order_id = order_res["result"]["AppOrderID"]
            sl_ltp = get_trade_ltp(xt, order_id=order_id)
            create_logs(logger,  process_id,
                        f"{datetime.now()} | PORTFOLIO RISK HIT: {str(strike)+side} | SL: {sl_ltp}"
                        )
            process['EMERGENCY_STOP'] = False
            SharedObject.modify_value(False, process_id, nested_key='EMERGENCY_STOP')
            return

    if EMERGENCY_STOP is False:
        order_res = xt.place_order(
            exchangeSegment=xt.EXCHANGE_NSEFO,
            exchangeInstrumentID=exchange_instrument_id,
            productType=xt.PRODUCT_NRML,
            orderType=xt.ORDER_TYPE_MARKET,
            orderSide=xt.TRANSACTION_TYPE_BUY,
            timeInForce=xt.VALIDITY_DAY,
            disclosedQuantity=0,
            orderQuantity=QTY,
            limitPrice=0,
            stopPrice=0,
            orderUniqueIdentifier="BNF_1",
        )
        order_id = order_res["result"]["AppOrderID"]
        time.sleep(0.5)
        sl_ltp = get_trade_ltp(xt, order_id=order_id)
        process_points['sl_point'] = sl_ltp
        create_logs(logger,  process_id,
                    f"1 | {datetime.now()} | Sqaure off remaining Trades: {str(strike)+side} | Exit: {sl_ltp}"
                    )
        if QTY * (CURRENT_PE_MARGIN + CURRENT_CE_MARGIN) <= PORTFOLIO_RISK:
            create_logs(logger,  process_id,
                        f"{datetime.now()} | PORTFOLIO RISK HIT: {str(strike)+side} | SL: {sl_ltp}"
                        )
        else:
            create_logs(logger,  process_id,
                        f"{datetime.now()} | User Exits: {str(strike)+side} | Square off: {sl_ltp}"
                        )
        return

    # After updating all the sl_point
    update_sl_point = {'sl_point': sl_point}
    SharedObject.modify_value(update_sl_point, process_id, nested_key=f'trade_side_dic.{side}')
    return 'Success'

@shared_task(bind=True, max_retries=None)
def check_and_trail_sl_buy(self, exchange_instrument_id, strike, side, process_id, CUR_CE_MULTIPLE, CUR_PE_MULTIPLE):
    global CE_MULTIPLE, OTM_GAP, PE_MULTIPLE, CURRENT_PE_MARGIN, CURRENT_CE_MARGIN, CURRENT_PNL, \
        EMERGENCY_STOP, CE_REENTRY, PE_REENTRY, SL_PERCENTAGE, QTY, TRADE, TRAIL_SL, \
        PORTFOLIO_RISK, SL_FLAG, TRADE_TYPE
   
    # Fetching latest value
    process = SharedObject.get_value(process_id)
    print(f'Process after trading {process}')

    process_points = process['trade_side_dic'][side]
    entry_price = process_points['entry_price']
    sl_point = process_points['sl_point']
    EMERGENCY_STOP = process['EMERGENCY_STOP']
    create_logs(logger,  process_id,
                f"Starting Sell Check And Trail {str(strike)+side} | Entry: {entry_price}")
    while EMERGENCY_STOP:
        process = SharedObject.get_value(process_id)
        #check for square off mechanism
        if not process or (hasattr(self.request, 'revoke') and self.request.revoke):
            # If the task is revoked, stop the loop and exit
            return
        # Emergency stop
        EMERGENCY_STOP = process.get('EMERGENCY_STOP', True)
        cur_price = round(Data.get_ltp(exchange_instrument_id))
        create_logs(logger, process_id, f'price on process {cur_price}-{exchange_instrument_id}')
        if SL_FLAG.upper() == 'FALSE':
            continue
        pnl_percent = (cur_price-entry_price)/ cur_price * 100
        if side == "CE":
            CURRENT_CE_MARGIN = cur_price - entry_price
            if IS_POINTS:
                if CURRENT_CE_MARGIN <= CUR_CE_MULTIPLE:
                    sl_point += TRAIL_SL
                    CUR_CE_MULTIPLE += CE_MULTIPLE
                    create_logs(logger,  process_id,
                                f"{datetime.now()} | Modified Order Placed: {str(strike)+side} | Entry: {entry_price} | UpdatedSL: {sl_point}"
                                )
            else:
                if pnl_percent <= CUR_CE_MULTIPLE:
                    sl_point = sl_point + (sl_point * (TRAIL_SL / 100))
                    CUR_CE_MULTIPLE += CE_MULTIPLE
                    create_logs(logger,  process_id,
                                f"{datetime.now()} | Modified Order Placed: {str(strike)+side} | Entry: {entry_price} | UpdatedSL: {sl_point}"
                                )

        if side == "PE":
            CURRENT_PE_MARGIN = cur_price - entry_price
            if IS_POINTS:
                if CURRENT_PE_MARGIN <= CUR_PE_MULTIPLE:
                    sl_point += TRAIL_SL
                    CUR_PE_MULTIPLE += PE_MULTIPLE
                    create_logs(logger,  process_id,
                                f"{datetime.now()} | Modified Order Placed: {str(strike)+side} | Entry: {entry_price} | UpdatedSL: {sl_point}"
                                )
            else:
                if pnl_percent <= CUR_PE_MULTIPLE:
                    sl_point = sl_point + (sl_point * (TRAIL_SL / 100))
                    CUR_PE_MULTIPLE += PE_MULTIPLE
                    create_logs(logger,  process_id,
                                f"{datetime.now()} | Modified Order Placed: {str(strike)+side} | Entry: {entry_price} | UpdatedSL: {sl_point}"
                                )

        current_portfolio_value = float(CURRENT_CE_MARGIN + CURRENT_PE_MARGIN)
        print(current_portfolio_value)

        if cur_price <= sl_point:
            stoploss_order_res = xt.place_order(
                exchangeSegment=xt.EXCHANGE_NSEFO,
                exchangeInstrumentID=exchange_instrument_id,
                productType=xt.PRODUCT_NRML,
                orderType=xt.ORDER_TYPE_MARKET,
                orderSide=xt.TRANSACTION_TYPE_SELL,
                timeInForce=xt.VALIDITY_DAY,
                disclosedQuantity=0,
                orderQuantity=QTY,
                limitPrice=0,
                stopPrice=0,
                orderUniqueIdentifier="BNF_1",
            )
            order_id = stoploss_order_res["result"]["AppOrderID"]
            sl_ltp = get_trade_ltp(xt, order_id=order_id)
            CURRENT_PNL += entry_price + sl_ltp
            create_logs(logger,  process_id,
                        f"1 | {datetime.now()} | Stoploss hit {side}: {str(strike)+side} | CMP: {sl_ltp} | SL: {sl_point}"
                        )
            create_logs(logger,  process_id,
                        f"{datetime.now()} | Stoploss hit {side}: {str(strike)+side} | CMP: {sl_ltp} | SL: {sl_point}"
                        )
            return

        if QTY * (CURRENT_PE_MARGIN + CURRENT_CE_MARGIN) <= PORTFOLIO_RISK:
            order_res = xt.place_order(
                exchangeSegment=xt.EXCHANGE_NSEFO,
                exchangeInstrumentID=exchange_instrument_id,
                productType=xt.PRODUCT_NRML,
                orderType=xt.ORDER_TYPE_MARKET,
                orderSide=xt.TRANSACTION_TYPE_SELL,
                timeInForce=xt.VALIDITY_DAY,
                disclosedQuantity=0,
                orderQuantity=QTY,
                limitPrice=0,
                stopPrice=0,
                orderUniqueIdentifier="BNF_1",
            )
            order_id = order_res["result"]["AppOrderID"]
            sl_ltp = get_trade_ltp(xt, order_id=order_id)
            create_logs(logger,  process_id,
                        f"{datetime.now()} | PORTFOLIO RISK HIT: {str(strike)+side} | SL: {sl_ltp}"
                        )
            process['EMERGENCY_STOP'] = False
            SharedObject.modify_value(False, process_id, nested_key='EMERGENCY_STOP')
            return

    if EMERGENCY_STOP is False:
        order_res = xt.place_order(
            exchangeSegment=xt.EXCHANGE_NSEFO,
            exchangeInstrumentID=exchange_instrument_id,
            productType=xt.PRODUCT_NRML,
            orderType=xt.ORDER_TYPE_MARKET,
            orderSide=xt.TRANSACTION_TYPE_SELL,
            timeInForce=xt.VALIDITY_DAY,
            disclosedQuantity=0,
            orderQuantity=QTY,
            limitPrice=0,
            stopPrice=0,
            orderUniqueIdentifier="BNF_1",
        )
        order_id = order_res["result"]["AppOrderID"]
        time.sleep(0.5)
        sl_ltp = get_trade_ltp(xt, order_id=order_id)
        process_points['sl_point'] = sl_ltp
        create_logs(logger,  process_id,
                    f"1 | {datetime.now()} | Sqaure off remaining Trades: {str(strike)+side} | Exit: {sl_ltp}"
                    )
        if QTY * (CURRENT_PE_MARGIN + CURRENT_CE_MARGIN) <= PORTFOLIO_RISK:
            create_logs(logger,  process_id,
                        f"{datetime.now()} | PORTFOLIO RISK HIT: {str(strike)+side} | SL: {sl_ltp}"
                        )
        else:
            create_logs(logger,  process_id,
                        f"{datetime.now()} | User Exits: {str(strike)+side} | Square off: {sl_ltp}"
                        )
        return

    # After updating all the sl_point
    update_sl_point = {'sl_point': sl_point}
    SharedObject.modify_value(update_sl_point, process_id, nested_key=f'trade_side_dic.{side}')
    return 'Success'

# for selling
def order_selling(exchange_instrument_id, strike, side, process_id):
    global CE_MULTIPLE, OTM_GAP, PE_MULTIPLE, CURRENT_PE_MARGIN, CURRENT_CE_MARGIN, CURRENT_PNL, \
        EMERGENCY_STOP, CE_REENTRY, PE_REENTRY, SL_PERCENTAGE, QTY, TRADE, TRAIL_SL, CUR_CE_MULTIPLE, \
        CUR_PE_MULTIPLE, PORTFOLIO_RISK, SL_FLAG, TRADE_TYPE

    order_res = xt.place_order(
        exchangeSegment=xt.EXCHANGE_NSEFO,
        exchangeInstrumentID=exchange_instrument_id,
        productType=xt.PRODUCT_NRML,
        orderType=xt.ORDER_TYPE_MARKET,
        orderSide=xt.TRANSACTION_TYPE_SELL,
        timeInForce=xt.VALIDITY_DAY,
        disclosedQuantity=0,
        orderQuantity=QTY,
        limitPrice=0,
        stopPrice=0,
        orderUniqueIdentifier="BNF_1",
    )
    print(f"Order response: {str(strike)+side} {order_res}")
    create_logs(logger,  process_id,
                f"Order response: {str(strike)+side} {order_res}")
    order_id = order_res["result"]["AppOrderID"]
    time.sleep(0.5)
    entry_price = get_trade_ltp(xt, order_id=order_id)
    create_logs(logger,  process_id, f'response --> {entry_price}')
    sl_point = entry_price - SL_PERCENTAGE
    create_logs(logger,  process_id, f"SL POINT: {sl_point}")
    print("1. SL POINT: ", sl_point)
    if IS_POINTS is False:
        sl_point = round(
            entry_price * (1 + SL_PERCENTAGE / 100), 3
        )
    create_logs(logger,  process_id, f"SL POINT: {sl_point}")
    create_logs(logger,  process_id,
                f"{datetime.now()} | Opening Order Placed For Sell: {str(strike)+side} | ENTRY: {entry_price} | SL: {sl_point}"
                )

    return sl_point, entry_price

# for buying


def order_buying(exchange_instrument_id, strike, side, process_id):
    global CE_MULTIPLE, OTM_GAP, PE_MULTIPLE, CURRENT_PE_MARGIN, CURRENT_CE_MARGIN, CURRENT_PNL, \
        EMERGENCY_STOP, CE_REENTRY, PE_REENTRY, SL_PERCENTAGE, QTY, TRADE, TRAIL_SL, CUR_CE_MULTIPLE, \
        CUR_PE_MULTIPLE, PORTFOLIO_RISK, SL_FLAG, TRADE_TYPE
    order_res = xt.place_order(
        exchangeSegment=xt.EXCHANGE_NSEFO,
        exchangeInstrumentID=exchange_instrument_id,
        productType=xt.PRODUCT_NRML,
        orderType=xt.ORDER_TYPE_MARKET,
        orderSide=xt.TRANSACTION_TYPE_BUY,
        timeInForce=xt.VALIDITY_DAY,
        disclosedQuantity=0,
        orderQuantity=QTY,
        limitPrice=0,
        stopPrice=0,
        orderUniqueIdentifier="BNF_1",
    )
    print(f"Order response: {str(strike)+side} {order_res}")
    create_logs(logger,  process_id,
                f"Order response: {str(strike)+side} {order_res}")
    order_id = order_res["result"]["AppOrderID"]
    time.sleep(2)
    entry_price = get_trade_ltp(xt, order_id=order_id)
    create_logs(logger,  process_id, entry_price)
    sl_point = entry_price + SL_PERCENTAGE
    create_logs(logger,  process_id, f"SL POINT: {sl_point}")
    print("1. SL POINT: ", sl_point)
    if IS_POINTS is False:
        sl_point = round(
            entry_price * (1 - SL_PERCENTAGE / 100), 3
        )
    create_logs(logger,  process_id, f"SL POINT: {sl_point}")
    create_logs(logger,  process_id,
                f"{datetime.now()} | Opening Order Placed For Buy: {str(strike)+side} | ENTRY: {entry_price} | SL: {sl_point}"
                )

    return sl_point, entry_price

# def get_tm(trade_type):
#     exchange_instrument_id, _ = get_otm(xt, "BANKNIFTY", atm, OTM_GAP, strike)  
                  
#     _, exchange_instrument_id, strike = get_atm(xt, "BANKNIFTY", atm)

@app.task
def place_trade(strike, side, exchange_instrument_id, process_id, TRADE, CUR_CE_MULTIPLE, CUR_PE_MULTIPLE):
    global CE_MULTIPLE, OTM_GAP, PE_MULTIPLE, CURRENT_PE_MARGIN, CURRENT_CE_MARGIN, CURRENT_PNL, \
        EMERGENCY_STOP, CE_REENTRY, PE_REENTRY, SL_PERCENTAGE, QTY, TRAIL_SL, \
        PORTFOLIO_RISK, SL_FLAG, TRADE_TYPE
    try:
        sl_point, entry_price = 0, 0
        create_logs(logger, process_id, f'Trade -->> {TRADE} {side}, {strike}')
        # for selling
        if TRADE == 'SELL':
            sl_point, entry_price = order_selling(
                exchange_instrument_id, strike, side, process_id)

        elif TRADE == 'BUY':
            sl_point, entry_price = order_buying(
                exchange_instrument_id, strike, side, process_id)

        # Updating Process ID variables
        update_value = {
            'sl_point': sl_point,
            'entry_price': entry_price
        }
        check_before = False
        if side == 'PE':
            check_before = True
        SharedObject.modify_value(
            update_value, process_id, nested_key=f'trade_side_dic.{side}', check_before=check_before)

        print("2. SL POINT: ", sl_point)
        # checking trade for sell
        if TRADE == 'SELL':
            print('Process starts for selling')
            check_and_trail_sl_sell.apply_async(
                (exchange_instrument_id, strike, side, str(process_id), CUR_CE_MULTIPLE, CUR_PE_MULTIPLE),
                countdown=0  # Optionally add delay here if needed
            )
            EMERGENCY_STOP = SharedObject.get_value(process_id, key='EMERGENCY_STOP')
            # Re-Entry Logic
            ## == ##
            if side == "CE":
                while CE_REENTRY > 0 and EMERGENCY_STOP:
                    EMERGENCY_STOP = SharedObject.get_value(process_id, key='EMERGENCY_STOP')
                    spot = Data.get_ltp(exchange_instrument_id)
                    atm = int(round(spot / 100)) * 100
                    exchange_instrument_id, _ = get_otm(
                        xt, "BANKNIFTY", atm, OTM_GAP, side)
                    try:
                        order_res = xt.place_order(exchangeSegment=xt.EXCHANGE_NSEFO,
                                                   exchangeInstrumentID=exchange_instrument_id,
                                                   productType=xt.PRODUCT_NRML,
                                                   orderType=xt.ORDER_TYPE_MARKET,
                                                   orderSide=xt.TRANSACTION_TYPE_SELL,
                                                   timeInForce=xt.VALIDITY_DAY,
                                                   disclosedQuantity=0,
                                                   orderQuantity=QTY,
                                                   stopPrice=0,
                                                   orderUniqueIdentifier="BNF_1")
                        create_logs(logger, process_id, f'2. Order Placed for CE {TRADE}-{order_res}')
                        print('Placeing 2 order ->', order_res)
                        order_id = order_res["result"]['AppOrderID']
                        time.sleep(2)
                        entry_price = get_trade_ltp(xt, order_id=order_id)
                        sl_point = entry_price - SL_PERCENTAGE
                        if IS_POINTS is False:
                            sl_point = round(
                                entry_price * (1 + SL_PERCENTAGE / 100), 3)
                        create_logs(logger,  process_id,
                                    f"{datetime.now()} | Reentry Order Placed: {strike} | Entry Price: {entry_price} | SL: {sl_point}")
                        CE_REENTRY -= 1
                        check_and_trail_sl_sell.apply_async(
                            (exchange_instrument_id, strike, side, str(process_id),CUR_CE_MULTIPLE, CUR_PE_MULTIPLE),
                            countdown=0  # Optionally add delay here if needed
                        )
                        if CE_REENTRY == 0 and PE_REENTRY != 0:
                            PE_REENTRY = 0
                    except KeyError as e:
                        logger.error(f"Error in placing CE order: {e}")
                        create_logs(logger, process_id, f'ERROR IN PLACING CE ORDER {e}', 'ERROR')

            if side == "PE":
                while PE_REENTRY > 0 and EMERGENCY_STOP:
                    EMERGENCY_STOP = SharedObject.get_value(process_id, key='EMERGENCY_STOP')
                    spot = Data.get_ltp(exchange_instrument_id)
                    atm = int(round(spot / 100)) * 100
                    exchange_instrument_id, _ = get_otm(
                        xt, "BANKNIFTY", atm, OTM_GAP, strike)
                    try:
                        order_res = xt.place_order(exchangeSegment=xt.EXCHANGE_NSEFO,
                                                   exchangeInstrumentID=exchange_instrument_id,
                                                   productType=xt.PRODUCT_NRML,
                                                   orderType=xt.ORDER_TYPE_MARKET,
                                                   orderSide=xt.TRANSACTION_TYPE_SELL,
                                                   timeInForce=xt.VALIDITY_DAY,
                                                   disclosedQuantity=0,
                                                   orderQuantity=QTY,
                                                   stopPrice=0,
                                                   orderUniqueIdentifier="BNF_1")
                        create_logs(logger, process_id, f'2. Order Placed for PE {TRADE}-{order_res}')
                        order_id = order_res["result"]['AppOrderID']
                        time.sleep(0.5)
                        entry_price = get_trade_ltp(xt, order_id=order_id)
                        sl_point = entry_price - SL_PERCENTAGE
                        if IS_POINTS is False:
                            sl_point = round(
                                entry_price * (1 + SL_PERCENTAGE / 100), 3)
                        create_logs(logger,  process_id,
                                    f"{datetime.now()} | Reentry Order Placed: {strike} | Entry Price: {entry_price} | SL: {sl_point}")
                        PE_REENTRY -= 1
                        check_and_trail_sl_sell.apply_async(
                            (exchange_instrument_id, strike, side, str(process_id),CUR_CE_MULTIPLE, CUR_PE_MULTIPLE),
                            countdown=0)
                        if PE_REENTRY == 0 and CE_REENTRY != 0:
                            CE_REENTRY = 0
                    except KeyError as e:
                        logger.error(f"Error in placing PE order: {e}")
                        create_logs(logger, process_id, f'ERROR IN PLACING PE ORDER {e}', 'ERROR')
            ## == ##

        elif TRADE == 'BUY':
            print('Process starts for buying')
            check_and_trail_sl_buy.delay(
                exchange_instrument_id, strike, side, str(process_id), CUR_CE_MULTIPLE, CUR_PE_MULTIPLE)
            EMERGENCY_STOP = SharedObject.get_value(process_id, key='EMERGENCY_STOP')
            # Re-Entry Logic
            ## == ##
            if side == "CE":
                while CE_REENTRY > 0 and EMERGENCY_STOP:
                    EMERGENCY_STOP = SharedObject.get_value(process_id, key='EMERGENCY_STOP')
                    spot = Data.get_ltp(exchange_instrument_id)
                    atm = int(round(spot / 100)) * 100
                    exchange_instrument_id, _ = get_otm(
                        xt, "BANKNIFTY", atm, OTM_GAP, side)
                    try:
                        order_res = xt.place_order(exchangeSegment=xt.EXCHANGE_NSEFO,
                                                   exchangeInstrumentID=exchange_instrument_id,
                                                   productType=xt.PRODUCT_NRML,
                                                   orderType=xt.ORDER_TYPE_MARKET,
                                                   orderSide=xt.TRANSACTION_TYPE_BUY,
                                                   timeInForce=xt.VALIDITY_DAY,
                                                   disclosedQuantity=0,
                                                   orderQuantity=QTY,
                                                   stopPrice=0,
                                                   orderUniqueIdentifier="BNF_1")
                        create_logs(logger, process_id, f'2. Order Placed for CE {TRADE}-{order_res}')
                        order_id = order_res["result"]['AppOrderID']
                        time.sleep(0.5)
                        entry_price = get_trade_ltp(xt, order_id=order_id)
                        sl_point = entry_price + SL_PERCENTAGE
                        if IS_POINTS is False:
                            sl_point = round(
                                entry_price * (1 - SL_PERCENTAGE / 100), 3)
                        create_logs(logger,  process_id,
                                    f"{datetime.now()} | Reentry Order Placed: {strike} | Entry Price: {entry_price} | SL: {sl_point}")
                        CE_REENTRY -= 1
                        check_and_trail_sl_buy.delay(
                            exchange_instrument_id, strike, side, str(process_id), CUR_CE_MULTIPLE, CUR_PE_MULTIPLE)
                        if CE_REENTRY == 0 and PE_REENTRY != 0:
                            PE_REENTRY = 0
                    except KeyError as e:
                        logger.error(f"Error in placing CE order: {e}")
                        create_logs(logger, process_id, f'ERROR IN PLACING CE ORDER {e}', 'ERROR')

            if side == "PE":
                while PE_REENTRY > 0 and EMERGENCY_STOP:
                    EMERGENCY_STOP = SharedObject.get_value(process_id, key='EMERGENCY_STOP')
                    spot = Data.get_ltp(exchange_instrument_id)
                    atm = int(round(spot / 100)) * 100
                    exchange_instrument_id, _ = get_otm(
                        xt, "BANKNIFTY", atm, OTM_GAP, strike)
                    try:
                        order_res = xt.place_order(exchangeSegment=xt.EXCHANGE_NSEFO,
                                                   exchangeInstrumentID=exchange_instrument_id,
                                                   productType=xt.PRODUCT_NRML,
                                                   orderType=xt.ORDER_TYPE_MARKET,
                                                   orderSide=xt.TRANSACTION_TYPE_BUY,
                                                   timeInForce=xt.VALIDITY_DAY,
                                                   disclosedQuantity=0,
                                                   orderQuantity=QTY,
                                                   stopPrice=0,
                                                   orderUniqueIdentifier="BNF_1")
                        create_logs(logger, process_id, f'2. Order Placed for PE {TRADE}-{order_res}')
                        order_id = order_res["result"]['AppOrderID']
                        time.sleep(0.5)
                        entry_price = get_trade_ltp(xt, order_id=order_id)
                        sl_point = entry_price + SL_PERCENTAGE
                        if IS_POINTS is False:
                            sl_point = round(
                                entry_price * (1 - SL_PERCENTAGE / 100), 3)
                        create_logs(logger,  process_id,
                                    f"{datetime.now()} | Reentry Order Placed: {strike} | Entry Price: {entry_price} | SL: {sl_point}")
                        PE_REENTRY -= 1
                        check_and_trail_sl_buy.delay(
                            exchange_instrument_id, strike, side, str(process_id),CUR_CE_MULTIPLE, CUR_PE_MULTIPLE)
                        if PE_REENTRY == 0 and CE_REENTRY != 0:
                            CE_REENTRY = 0
                    except KeyError as e:
                        logger.error(f"Error in placing PE order: {e}")
                        create_logs(logger, process_id, f'ERROR IN PLACING PE ORDER {e}', 'ERROR')
            ## == ##
        create_logs(logger, process_id, 'FINISHED')
    except Exception as e:
        logger.error(f"Opening Order Placing: {e}")
        create_logs(logger, process_id, f'ERROR IN PLACING ORDER {e}', 'ERROR')


def bnfv_1(process_id, data: dict):
    global CE_MULTIPLE, OTM_GAP, PE_MULTIPLE, CURRENT_PE_MARGIN, CURRENT_CE_MARGIN, CURRENT_PNL, \
        EMERGENCY_STOP, CE_REENTRY, PE_REENTRY, SL_PERCENTAGE, QTY, TRADE, TRAIL_SL, CUR_CE_MULTIPLE, \
        CUR_PE_MULTIPLE, PORTFOLIO_RISK, SL_FLAG, TRADE_TYPE
    QTY = int(data['qty'])
    PORTFOLIO_RISK = -1 * ((QTY / 15) * 1_00_000) * 0.01
    SL_PERCENTAGE = int(data['sl_percentage'])
    TRAIL_SL = int(data['trailing_sl'])
    CUR_CE_MULTIPLE = CE_MULTIPLE = int(data['trailing_tp'])
    CUR_PE_MULTIPLE = PE_MULTIPLE = int(data['trailing_tp'])
    CE_REENTRY = int(data['ce_rentry'])
    PE_REENTRY = int(data['pe_rentry'])
    IS_POINTS = data['isPoint']
    TRADE = data['trade'].upper()
    TRADE_TYPE = ''
    OTM_GAP = int(data['otm_gap'])
    SL_FLAG = data['sl_flag']
    CURRENT_PNL = 0
    CURRENT_CE_MARGIN = 0
    CURRENT_PE_MARGIN = 0
    EMERGENCY_STOP = True
    create_logs(logger,  process_id,
                f"""
            Running New Strategy at {datetime.now()}
        QTY: {QTY}
        PR: {PORTFOLIO_RISK}
        SL: {SL_PERCENTAGE}
        TRAIL_SL: {TRAIL_SL}
        TRAIL TP CE: {CUR_CE_MULTIPLE}
        TRAIL TP PE: {CUR_PE_MULTIPLE}
        CE REENTRY: {CE_REENTRY}
        PE REENTRY: {PE_REENTRY}
        IS POINTS: {IS_POINTS}
        """
                )
    print(
        f"""
            Running New Strategy at {datetime.now()}
        QTY: {QTY}
        PR: {PORTFOLIO_RISK}
        SL: {SL_PERCENTAGE}
        TRAIL_SL: {TRAIL_SL}
        TRAIL TP CE: {CUR_CE_MULTIPLE}
        TRAIL TP PE: {CUR_PE_MULTIPLE}
        CE REENTRY: {CE_REENTRY}
        PE REENTRY: {PE_REENTRY}
        IS POINTS: {IS_POINTS}
        """
    )
    try:
        print("NIFTY", NIFTY_ID)
        spot = Data.get_ltp(26001)
        print("SPOT", spot)
        atm = int(round(spot / 100)) * 100

        if TRADE == 'SELL':
            ce_instrument_id, pe_instrument_id, strike = get_atm(
                xt, "BANKNIFTY", atm
            )
            # updating the process ID for using below values in stop process execution
            strike_ce = strike_pe = strike
            trade_dic = {'trade_side_dic': {
                'CE': {'instrument_id': ce_instrument_id, 'strike': strike_ce},
                'PE': {'instrument_id': pe_instrument_id, 'strike': strike_pe}
            }
            }

        else:
            ce_instrument_id, strike_ce = get_otm(
                xt, "BANKNIFTY", atm, OTM_GAP, 'CE'
            )
            pe_instrument_id, strike_pe = get_otm(
                xt, "BANKNIFTY", atm, OTM_GAP, 'PE'
            )
            # updating the process ID for using below values in stop process execution
            trade_dic = {'trade_side_dic': {
                'CE': {'instrument_id': ce_instrument_id, 'strike': strike_ce},
                'PE': {'instrument_id': pe_instrument_id, 'strike': strike_pe}
            }
            }

        # update in database
        SharedObject.modify_value(trade_dic, process_id)

        print(f'Trading place for {TRADE}-{TRADE_TYPE}')
        # Create task signatures for place_trade tasks
        # Your asynchronous task for strategy here
        async_result = group(place_trade.s(strike_ce, "CE", ce_instrument_id, process_id, TRADE, CUR_CE_MULTIPLE, CUR_PE_MULTIPLE),
                             place_trade.s(strike_pe, "PE", pe_instrument_id, process_id, TRADE, CUR_CE_MULTIPLE, CUR_PE_MULTIPLE)).apply_async()

        print(
            f'{datetime.now()}| async result {async_result} | Trade Place: {TRADE} | Has been completed')
        # Task for sell has been started

        return async_result.id, strike_ce, strike_pe, ce_instrument_id, pe_instrument_id

    except Exception as e:
        logger.error(f"error in main(): {e}")
        return f'Failed: {e}'
