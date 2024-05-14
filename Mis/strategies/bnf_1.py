from celery import shared_task
import os
import logging
from datetime import datetime
from utils.server_request import Data
from utils.extras import get_atm, get_trade_ltp
from utils import prices
from strategies.updated_connect import UpdatedXTSConnect
from utils.config import *
import asyncio
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger("BNFLogger")
formatter = logging.Formatter("%(asctime)s - %(message)s")
file_handler = logging.FileHandler("process_logs.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# login to the XT server
xt = UpdatedXTSConnect(
    os.getenv('INTERACTIVE_KEY'), os.getenv("INTERACTIVE_SECRET"), "WEBAPI"
)
xt.marketdata_login(token=os.getenv('TOKEN'), userId=os.getenv("USERID"))

@shared_task
async def stop_trade(process, id):
    '''
    params:
    process: Contains all the different id values that is in process
    id: Process ID which need to be stopped
    '''
    # variables for each data points
    trade_side_dic = process[id]['trade_side_dic']
    strike = process[id]['strike']

    # starting stop mechanism
    for side, exchange_instrument_id in trade_side_dic.items():
        entry_price = process[id][side]['entry_price']
        sl_point = process[id][side]['sl_point']
        logger.info(f"1 | {datetime.now()} | Strategy Stopped by User.")
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
        order_id = stoploss_order_res["result"]["AppOrderID"]
        await asyncio.sleep(0.5)
        sl_ltp = get_trade_ltp(xt, order_id=order_id)
        CURRENT_PnL += entry_price - sl_ltp
        logger.info(
            f"1 | {datetime.now()} | Square off remaining Trades: {str(strike)+side} | CMP: {sl_ltp} | SL: {sl_point}"
        )
        logger.info(
            f"{datetime.now()} | Stoploss hit: {str(strike)+side} | CMP: {sl_ltp} | SL: {sl_point}"
        )
        return "Stopped"


async def check_and_trail_sl(exchange_instrument_id, strike, side, proccess_points):
    entry_price = proccess_points['entry_price']
    sl_point = proccess_points['sl_point']
    while EMERGENCY_STOP:
        cur_price = round(Data.get_ltp(exchange_instrument_id))
        pnl_percent = (entry_price - cur_price) / cur_price * 100

        if side == "CE":
            CURRENT_CE_MARGIN = entry_price - cur_price
            if IS_POINTS:
                if CURRENT_CE_MARGIN >= CUR_CE_MULTIPLE:
                    sl_point -= TRAIL_SL
                    CUR_CE_MULTIPLE += CE_MULTIPLE
                    logger.info(
                        f"{datetime.now()} | Modified Order Placed: {str(strike)+side} | Entry: {entry_price} | UpdatedSL: {sl_point}"
                    )
            else:
                if pnl_percent >= CUR_CE_MULTIPLE:
                    sl_point = sl_point - (sl_point * (TRAIL_SL / 100))
                    CUR_CE_MULTIPLE += CE_MULTIPLE
                    logger.info(
                        f"{datetime.now()} | Modified Order Placed: {str(strike)+side} | Entry: {entry_price} | UpdatedSL: {sl_point}"
                    )

        if side == "PE":
            CURRENT_PE_MARGIN = entry_price - cur_price
            if IS_POINTS:
                if CURRENT_PE_MARGIN >= CUR_PE_MULTIPLE:
                    sl_point -= TRAIL_SL
                    CUR_PE_MULTIPLE += PE_MULTIPLE
                    logger.info(
                        f"{datetime.now()} | Modified Order Placed: {str(strike)+side} | Entry: {entry_price} | UpdatedSL: {sl_point}"
                    )
            else:
                if pnl_percent >= CUR_PE_MULTIPLE:
                    sl_point = sl_point - (sl_point * (TRAIL_SL / 100))
                    CUR_PE_MULTIPLE += PE_MULTIPLE
                    logger.info(
                        f"{datetime.now()} | Modified Order Placed: {str(strike)+side} | Entry: {entry_price} | UpdatedSL: {sl_point}"
                    )

        current_portfolio_value = float(CURRENT_CE_MARGIN + CURRENT_PE_MARGIN)
        logger.info(current_portfolio_value)

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
            CURRENT_PnL += entry_price - sl_ltp
            logger.info(
                f"1 | {datetime.now()} | Stoploss hit: {str(strike)+side} | CMP: {sl_ltp} | SL: {sl_point}"
            )
            logger.info(
                f"{datetime.now()} | Stoploss hit: {str(strike)+side} | CMP: {sl_ltp} | SL: {sl_point}"
            )
            break

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
            logger.info(
                f"{datetime.now()} | PORTFOLIO RISK HIT: {str(strike)+side} | SL: {sl_ltp}"
            )
            EMERGENCY_STOP = False
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
        await asyncio.sleep(0.5)
        sl_ltp = get_trade_ltp(xt, order_id=order_id)
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
    # After updating all the sl_point
    proccess_points['sl_point'] = sl_point

async def place_trade(xt, strike, side, exchange_instrument_id, proccess, process_id):
    global CE_MULTIPLE, PE_MULTIPLE, CURRENT_PE_MARGIN, CURRENT_CE_MARGIN, CURRENT_PnL, EMERGENCY_STOP
    try:
        sl_point, entry_price = 0, 0
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
        logger.info(f"Order response: {str(strike)+side} {order_res}")
        order_id = order_res["result"]["AppOrderID"]
        await asyncio.sleep(0.5)
        entry_price = get_trade_ltp(xt, order_id=order_id)
        logger.info(entry_price)
        sl_point = entry_price - SL_PERCENTAGE
        logger.info("SL POINT: ", sl_point)
        if IS_POINTS is False:
            sl_point = round(
                entry_price * (1 + SL_PERCENTAGE / 100), 3
            )

        logger.info("SL POINT: ", sl_point)
        logger.info(
            f"{datetime.now()} | Opening Order Placed: {str(strike)+side} | ENTRY: {entry_price} | SL: {sl_point}"
        )
    except Exception as e:
        logger.error(f"Opening Order Placing: {e}")
    
    # Updating Process ID vaiables
    proccess[process_id][side].update({
        'sl_point':sl_point,
        'entry_point':entry_price
    })
    await check_and_trail_sl(
        exchange_instrument_id, strike, side, proccess[process_id][side]
    )

@shared_task
def bnfv_1(processes, process_id, data):
        print(data)
        global CE_MULTIPLE, PE_MULTIPLE, CURRENT_PE_MARGIN, CURRENT_CE_MARGIN, CURRENT_PnL, EMERGENCY_STOP
        QTY = data["qty"]
        PORTFOLIO_RISK = -1 * ((QTY / 15) * 1_00_000) * 0.01
        SL_PERCENTAGE = data["sl_percentage"]
        TRAIL_SL = data["trailing_sl"]
        CUR_CE_MULTIPLE = CE_MULTIPLE = data["trailing_tp"]
        CUR_PE_MULTIPLE = PE_MULTIPLE = data["trailing_tp"]
        CE_REENTRY = data["ce_rentry"]
        PE_REENTRY = data["pe_rentry"]
        IS_POINTS = data["isPoint"]
        CURRENT_PnL = 0
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
        try:
            logger.info("Started BNF_1 stratergy")
            exp_details = prices.getCurrentExpiryDetails()
            spot = Data.get_ltp(NIFTY_ID)
            atm = int(round(spot / 100)) * 100
            # EX_Y = exp_details["BANKNIFTY"]["year"]
            # EX_M = exp_details["BANKNIFTY"]["month"]
            # EX_D = exp_details["BANKNIFTY"]["day"]
            if data is not None:
                logger.info(str(exp_details["BANKNIFTY"]))
            ce_instrument_id, pe_instrument_id, strike = get_atm(
                xt, "BANKNIFTY", atm
            )
            # updating the process ID for using below values in stop process execution
            processes[process_id].update({'trade_side_dic': {
                'CE': {'instrument_id':ce_instrument_id},
                'PE': {'instrument_id':pe_instrument_id}
            },
            'strike': strike
            })
            # Task for sell has been started
            tasks = [
                asyncio.create_task(
                    place_trade(
                        xt, "BANKNIFTY", strike, "CE", ce_instrument_id, processes, process_id
                    )
                ),
                asyncio.create_task(
                    place_trade(
                        xt, "BANKNIFTY", strike, "PE", pe_instrument_id, processes, process_id
                    )
                ),
            ]

            asyncio.gather(*tasks)

        except Exception as e:
            logger.error(f"error in main(): {e}")

