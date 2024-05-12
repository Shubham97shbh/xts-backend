from XTS.celery import app
from celery import group, shared_task
import os
import logging
from datetime import datetime
from utils.server_request import Data
from utils.extras import get_atm, get_trade_ltp, get_otm
from utils import prices
from strategies.updated_connect import UpdatedXTSConnect
from utils.config import *
import time
from dotenv import load_dotenv

logger = logging.getLogger("BNFLogger")
formatter = logging.Formatter("%(asctime)s - %(message)s")
file_handler = logging.FileHandler("process_logs.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

load_dotenv()

# login to the XT server
xt = UpdatedXTSConnect(
    os.getenv("INTERACTIVE_KEY"), os.getenv("INTERACTIVE_SECRET"), "WEBAPI"
)
print("TOKEN: ", os.getenv("TOKEN"))
xt.marketdata_login(token=os.getenv("TOKEN"), userId=os.getenv("USERID"))
print("BALANCE", xt.get_holding("*****"))

@app.task
def stop_trade(process, id, trade):
    '''
    params:
    process: Contains all the different id values that is in process
    id: Process ID which need to be stopped
    '''
    # variables for each data points
    trade_side_dic = process[id]['trade_side_dic']
    print(f'Stopping trade for {process}, ')
    # starting stop mechanism
    time.sleep(10)
    return "Stopped"
        
@shared_task
def check_and_trail_sl_sell():
    global CE_MULTIPLE, OTM_GAP, PE_MULTIPLE, CURRENT_PE_MARGIN, CURRENT_CE_MARGIN, CURRENT_PNL,\
              EMERGENCY_STOP, CE_REENTRY, PE_REENTRY, SL_PERCENTAGE, QTY, TRADE, TRAIL_SL, CUR_CE_MULTIPLE,\
              CUR_PE_MULTIPLE, PORTFOLIO_RISK, SL_FLAG
    time.sleep(5)

@app.task
def check_and_trail_sl_buy(exchange_instrument_id, strike, side, process, process_id):
    global CE_MULTIPLE, OTM_GAP, PE_MULTIPLE, CURRENT_PE_MARGIN, CURRENT_CE_MARGIN, CURRENT_PNL,\
              EMERGENCY_STOP, CE_REENTRY, PE_REENTRY, SL_PERCENTAGE, QTY, TRADE, TRAIL_SL, CUR_CE_MULTIPLE,\
              CUR_PE_MULTIPLE, PORTFOLIO_RISK, SL_FLAG
    process_points = process[process_id]['trade_side_dic'][side]
    entry_price = process_points['entry_price']
    sl_point = process_points['sl_point']
    EMERGENCY_STOP = process[process_id]['EMERGENCY_STOP']
    logger.info(f"Starting Sell Check And Trail {str(strike)+side} | Entry: {entry_price}")
    while EMERGENCY_STOP:
        EMERGENCY_STOP = process[process_id]['EMERGENCY_STOP']
        cur_price = round(Data.get_ltp(exchange_instrument_id))
        # it's included for removing trail functionality
        if SL_FLAG.upper()=='FALSE':
            continue
        pnl_percent = (entry_price + cur_price) / cur_price * 100
        if side == "CE":
            CURRENT_CE_MARGIN = cur_price - entry_price
            if IS_POINTS:
                if CURRENT_CE_MARGIN <= CUR_CE_MULTIPLE:
                    sl_point += TRAIL_SL
                    CUR_CE_MULTIPLE += CE_MULTIPLE
                    logger.info(
                        f"{datetime.now()} | Modified Order Placed: {str(strike)+side} | Entry: {entry_price} | UpdatedSL: {sl_point}"
                    )
            else:
                if pnl_percent <= CUR_CE_MULTIPLE:
                    sl_point = sl_point + (sl_point * (TRAIL_SL / 100))
                    CUR_CE_MULTIPLE += CE_MULTIPLE
                    logger.info(
                        f"{datetime.now()} | Modified Order Placed: {str(strike)+side} | Entry: {entry_price} | UpdatedSL: {sl_point}"
                    )

        if side == "PE":
            CURRENT_PE_MARGIN = cur_price - entry_price
            if IS_POINTS:
                if CURRENT_PE_MARGIN <= CUR_PE_MULTIPLE:
                    sl_point += TRAIL_SL
                    CUR_PE_MULTIPLE += PE_MULTIPLE
                    logger.info(
                        f"{datetime.now()} | Modified Order Placed: {str(strike)+side} | Entry: {entry_price} | UpdatedSL: {sl_point}"
                    )
            else:
                if pnl_percent <= CUR_PE_MULTIPLE:
                    sl_point = sl_point + (sl_point * (TRAIL_SL / 100))
                    CUR_PE_MULTIPLE += PE_MULTIPLE
                    logger.info(
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
            logger.info(
                f"1 | {datetime.now()} | Stoploss hit: {str(strike)+side} | CMP: {sl_ltp} | SL: {sl_point}"
            )
            logger.info(
                f"{datetime.now()} | Stoploss hit: {str(strike)+side} | CMP: {sl_ltp} | SL: {sl_point}"
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
            logger.info(
                f"{datetime.now()} | PORTFOLIO RISK HIT: {str(strike)+side} | SL: {sl_ltp}"
            )
            process[process_id]['EMERGENCY_STOP'] = False
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
        logger.info(
            f"1 | {datetime.now()} | Sqaure off remaining Trades: {str(strike)+side} | Exit: {sl_ltp}"
        )
        if QTY * (CURRENT_PE_MARGIN + CURRENT_CE_MARGIN) <= PORTFOLIO_RISK:
            logger.info(
                f"{datetime.now()} | PORTFOLIO RISK HIT: {str(strike)+side} | SL: {sl_ltp}"
            )
        else:
            logger.info(
                f"{datetime.now()} | User Exits: {str(strike)+side} | Square off: {sl_ltp}"
            )
        return
    
    # After updating all the sl_point
    process_points['sl_point'] = sl_point

    return 'Success'

# for selling
def order_selling(xt, exchange_instrument_id, strike, side):
    global CE_MULTIPLE, OTM_GAP, PE_MULTIPLE, CURRENT_PE_MARGIN, CURRENT_CE_MARGIN, CURRENT_PNL,\
              EMERGENCY_STOP, CE_REENTRY, PE_REENTRY, SL_PERCENTAGE, QTY, TRADE, TRAIL_SL, CUR_CE_MULTIPLE,\
              CUR_PE_MULTIPLE, PORTFOLIO_RISK, SL_FLAG
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
    logger.info(f"Order response: {str(strike)+side} {order_res}")
    order_id = order_res["result"]["AppOrderID"]
    time.sleep(0.5)
    entry_price = get_trade_ltp(xt, order_id=order_id)
    logger.info(entry_price)
    sl_point = entry_price - SL_PERCENTAGE
    logger.info("SL POINT: ", sl_point)
    print("1. SL POINT: ", sl_point)
    if IS_POINTS is False:
        sl_point = round(
            entry_price * (1 + SL_PERCENTAGE / 100), 3
        )
    logger.info("SL POINT: ", sl_point)
    logger.info(
            f"{datetime.now()} | Opening Order Placed For Sell: {str(strike)+side} | ENTRY: {entry_price} | SL: {sl_point}"
        )
    
    return sl_point, entry_price

# for buying
def order_buying(xt, exchange_instrument_id, strike, side):
    global CE_MULTIPLE, OTM_GAP, PE_MULTIPLE, CURRENT_PE_MARGIN, CURRENT_CE_MARGIN, CURRENT_PNL,\
              EMERGENCY_STOP, CE_REENTRY, PE_REENTRY, SL_PERCENTAGE, QTY, TRADE, TRAIL_SL, CUR_CE_MULTIPLE,\
              CUR_PE_MULTIPLE, PORTFOLIO_RISK, SL_FLAG
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
    logger.info(f"Order response: {str(strike)+side} {order_res}")
    order_id = order_res["result"]["AppOrderID"]
    time.sleep(0.5)
    entry_price = get_trade_ltp(xt, order_id=order_id)
    logger.info(entry_price)
    sl_point = entry_price + SL_PERCENTAGE
    logger.info("SL POINT: ", sl_point)
    print("1. SL POINT: ", sl_point)
    if IS_POINTS is False:
        sl_point = round(
            entry_price * (1 - SL_PERCENTAGE / 100), 3
        )
    logger.info("SL POINT: ", sl_point)
    logger.info(
            f"{datetime.now()} | Opening Order Placed For Buy: {str(strike)+side} | ENTRY: {entry_price} | SL: {sl_point}"
        )
    
    return sl_point, entry_price

@app.task
def place_trade():
    global CE_MULTIPLE, OTM_GAP, PE_MULTIPLE, CURRENT_PE_MARGIN, CURRENT_CE_MARGIN, CURRENT_PNL,\
              EMERGENCY_STOP, CE_REENTRY, PE_REENTRY, SL_PERCENTAGE, QTY, TRADE, TRAIL_SL, CUR_CE_MULTIPLE,\
              CUR_PE_MULTIPLE, PORTFOLIO_RISK, SL_FLAG
    time.sleep(1)
    check_and_trail_sl_sell.delay()

def bnfv_1(processes, process_id, data: dict):
        global CE_MULTIPLE, OTM_GAP, PE_MULTIPLE, CURRENT_PE_MARGIN, CURRENT_CE_MARGIN, CURRENT_PNL,\
              EMERGENCY_STOP, CE_REENTRY, PE_REENTRY, SL_PERCENTAGE, QTY, TRADE, TRAIL_SL, CUR_CE_MULTIPLE,\
              CUR_PE_MULTIPLE, PORTFOLIO_RISK, SL_FLAG
        QTY = data['qty']
        PORTFOLIO_RISK = -1 * ((QTY / 15) * 1_00_000) * 0.01
        SL_PERCENTAGE = data['sl_percentage']
        TRAIL_SL = data['trailing_sl']
        CUR_CE_MULTIPLE = CE_MULTIPLE = data['trailing_tp']
        CUR_PE_MULTIPLE = PE_MULTIPLE = data['trailing_tp']
        CE_REENTRY = data['ce_rentry']
        PE_REENTRY = data['pe_rentry']
        IS_POINTS = data['isPoint']
        TRADE = data['trade'].upper()
        OTM_GAP = data['otm_gap']
        SL_FLAG = data['sl_flag']
        CURRENT_PNL = 0
        CURRENT_CE_MARGIN = 0
        CURRENT_PE_MARGIN = 0
        EMERGENCY_STOP = True
        logger.info(
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
            print("NIFTY",NIFTY_ID)
            spot = Data.get_ltp(26001)
            print("SPOT", spot)
            atm = int(round(spot / 100)) * 100

            if TRADE == 'SELL':
                ce_instrument_id, pe_instrument_id, strike = 10,1200,1213
                # updating the process ID for using below values in stop process execution
                strike_ce = strike_pe = strike
                processes[process_id].update({'trade_side_dic': {
                    'CE': {'instrument_id':ce_instrument_id, 'strike': strike_ce},
                    'PE': {'instrument_id':pe_instrument_id, 'strike': strike_pe}
                    }
                })

            else:
                ce_instrument_id, strike_ce = get_otm(
                    xt, "BANKNIFTY", atm, OTM_GAP, 'CE'
                )
                pe_instrument_id, strike_pe = get_otm(
                    xt, "BANKNIFTY", atm, OTM_GAP, 'PE'
                )
            
            # updating the process ID for using below values in stop process execution
            processes[process_id].update({'trade_side_dic': {
                    'CE': {'instrument_id':ce_instrument_id, 'strike': strike_ce},
                    'PE': {'instrument_id':pe_instrument_id, 'strike': strike_pe}
                    }
                })
            
            print(f'Trading place for {TRADE}')
            # Create task signatures for place_trade tasks
            # Your asynchronous task for strategy here
            async_result = group(place_trade.s(),
                          place_trade.s()).apply_async()

            
            print(f'{datetime.now()}| async result {async_result} | Trade Place: {TRADE} | Has been completed {str(processes)}')
            # Task for sell has been started
            
            return async_result.id

        except Exception as e:
            logger.error(f"error in main(): {e}")
            return f'Failed: {e}'