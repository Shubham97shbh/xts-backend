from celery import shared_task
import os
import logging
from datetime import datetime
from utils.server_request import Data
from utils.extras import get_atm, get_trade_ltp
from utils import prices
from strategies.updated_connect import UpdatedXTSConnect
from utils.config import *
from asgiref.sync import async_to_sync
import asyncio
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger("BNFLogger")
formatter = logging.Formatter("%(asctime)s - %(message)s")
file_handler = logging.FileHandler("process_logs.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# login to the XT server
xt = {}

@shared_task
async def stop_trade(process, id):
    '''
    params:
    process: Contains all the different id values that is in process
    id: Process ID which need to be stopped
    '''
    # variables for each data points
    asyncio.sleep(100)
    logger.error(f"Opening Order Stopped")


@async_to_sync
async def check_and_trail_sl():
    asyncio.sleep(100)
@async_to_sync
async def place_trade(xt, strike, side, exchange_instrument_id, proccess, process_id):
    global CE_MULTIPLE, PE_MULTIPLE, CURRENT_PE_MARGIN, CURRENT_CE_MARGIN, CURRENT_PnL, EMERGENCY_STOP
    try:
        logger.info(f"Order response: {str(strike)+side} {121}")
        logger.info("SL POINT: ", 1212)
        logger.info(
            f"{datetime.now()} | Opening Order Placed: {str(strike)+side} | ENTRY: {'xcz'} | SL: {'sl_point'}"
        )
    except Exception as e:
        logger.error(f"Opening Order Placing: {e}")
    
    # Updating Process ID vaiables
    await check_and_trail_sl()

@shared_task
def bnfv_1(processes, process_id, data):
    logger.info(f"Running New Strategy at {datetime.now()}")
    result = {}
    QTY = data["qty"]
    PORTFOLIO_RISK = -1 * ((QTY / 15) * 1_00_000) * 0.01
    SL_PERCENTAGE = data["sl_percentage"]
    TRAIL_SL = data["trailing_sl"]
    CUR_CE_MULTIPLE = data["trailing_tp"]
    CUR_PE_MULTIPLE = data["trailing_tp"]
    CE_REENTRY = data["ce_rentry"]
    PE_REENTRY = data["pe_rentry"]
    IS_POINTS = data["isPoint"]
    CURRENT_PnL = 0
    CURRENT_CE_MARGIN = 0
    CURRENT_PE_MARGIN = 0
    EMERGENCY_STOP = True
    
    logger.info("Started BNF_1 strategy")
    # Task for placing trades
    tasks = [
        place_trade.delay(xt, "BANKNIFTY", 10, "CE", processes, process_id),
        place_trade.delay(xt, "BANKNIFTY", 101, "PE", processes, process_id)
    ]

    # Await the results before returning
    result = asyncio.gather(*tasks).result()
    logger.info(result)
    return result
