from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
import logging
from strategies.bnf_1 import bnfv_1, stop_trade
from rest_framework.permissions import AllowAny
from dotenv import load_dotenv
# from XTSApp.apps import monitor_task_results
from celery.result import AsyncResult
from django.http import JsonResponse
from XTSApp.models import SharedObject
from utils.config import PROCESS_CONFIG
from XTS.celery import app

load_dotenv()

logger = logging.getLogger(__name__)
trade_bnf = None

class StartProcessView(APIView):
    permission_classes = [AllowAny]

    def post(self, request):
        data = request.data
        print(f'Requested data for process:-> {data}')
        
        # Validate incoming data
        process_id = data.get('process_id')
        if process_id is None:
            return Response({"message": "Process ID is missing"}, status=status.HTTP_400_BAD_REQUEST)

        # Check if process already exists
        if SharedObject.get_value(process_id):
            return Response({"status_code": 200, "process_id": process_id, "message": f"Process {process_id} is already running"}, status=status.HTTP_200_OK)

        try:
            # Insert the value into SharedObject
            print('Started')
            inserted_value = SharedObject.insert_value(PROCESS_CONFIG, process_id)
            if inserted_value is None:
                return Response({"message": f"Failed to start process {process_id}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

            # Calling Strategy
            task_id, strike_ce, strike_pe, ce_instrument_id, pe_instrument_id = bnfv_1(process_id, data)
            print(task_id, strike_ce, strike_pe, ce_instrument_id, pe_instrument_id )
            # Modify the value in SharedObject
            SharedObject.modify_value(task_id, process_id, nested_key='task_id')

            if isinstance(task_id, str) and 'Failed' in task_id:
                error_data = {'message': 'success',
                              'error': task_id}
                return JsonResponse(error_data, status=status.HTTP_202_ACCEPTED)

        except Exception as E:
            error_message = f"Process {process_id}: Failed to execute"
            error_data = {'message': error_message, 'error': str(E)}
            return JsonResponse(error_data, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return JsonResponse({"status_code": 200, "process_id": process_id,"strike_ce":strike_ce,"strike_pe": strike_pe,
                        "ce_instrument_id": ce_instrument_id, "pe_instrument_id":pe_instrument_id,
                        "message": f"Process {process_id} started"}, status=status.HTTP_200_OK)


class LogsView(APIView):
    permission_classes = [AllowAny]

    def get(self, request):
        process_id = request.query_params.get('process_id')  # Accessing query parameters correctly
        if not process_id:
            return Response({"message": "Process ID is missing"}, status=status.HTTP_400_BAD_REQUEST)

        try:
            logs = SharedObject.get_value(process_id, key='logs')
            process = SharedObject.get_value(process_id)
            return Response({'message': logs,
                             'process':process, 'status': status.HTTP_200_OK})  # Fixed status variable name
        
        except KeyError:
            return Response({"message": f"Process {process_id} not found"}, status=status.HTTP_404_NOT_FOUND)


class StopProcessView(APIView):
    permission_classes = [AllowAny]

    def delete(self, request):
        data = request.data
        process_id = data.get('process_id')
        trade = data.get('trade')
        if not process_id:
            return Response({"message": "Process ID is missing"}, status=status.HTTP_400_BAD_REQUEST)

        process = SharedObject.get_value(process_id)
        print(f'Process Flush -> {process}')
        if not process:
            return Response({"message": f"Process {process_id} not found"}, status=status.HTTP_404_NOT_FOUND)
        
        # stopping the process
        stop_trade(process, process_id, trade)

        task_id = process.get('task_id')  # Accessing task_id safely
        if not task_id:
            return Response({"message": f"Process {process_id} stopped but without revoke for task_id: {task_id}"}, status=status.HTTP_202_ACCEPTED)

        # Delete process value
        SharedObject.delete_value(process_id)

        # Revoke the task
        app.control.revoke(task_id, terminate=True, signal='SIGKILL')
        logger.info(f'Task for process {process_id}')
        return Response({"message": f"Process {process_id} stopped"}, status=status.HTTP_200_OK)
