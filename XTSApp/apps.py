from django.apps import AppConfig
from XTS.celery import app

class XtsappConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'XTSApp'

# # Function to monitor the results of individual tasks in the chain
# def monitor_task_results(task_id):
#     # Retrieve the AsyncResult object for the entire chain using the task_id
# 	tasks_result = app.AsyncResult(task_id)
# 	# Check if any of the tasks in the chain are completed
#     if any(task.ready() for task in tasks_result.children):
# 		# Iterate over the tasks in the chain and print their results
# 		for task in tasks_result.children:
# 				if task.ready():
# 					print("Task ID:", task.id)
# 					print("Task Result:", task.get())
