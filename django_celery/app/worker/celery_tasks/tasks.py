import logging
import time
import traceback
from celery import Task, group
from worker.celery import app
from worker.tasks import add

# Define custom task class
class CustomTask(Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        # exc: The exception that caused the task to fail.
        # task_id: The ID of the failed task.
        # args: The arguments passed to the task.
        # kwargs: The keyword arguments passed to the task.
        # einfo: An object containing information about the exception.

        # This method is called when a task fails
        print(f'Task failed: {exc}')

        # Optionally, you can perform actions like logging or sending notifications here
        # For example, you might want to retry the task under certain conditions
        if isinstance(exc, Exception):
            logging.error(f"Error happens on {task_id}... fix this!!!")

# Register custom task class with Celery
app.task(base=CustomTask)

@app.task(queue='celery', base=CustomTask,
          autoretry_for=(IOError,), max_retries=3, default_retry_delay=10)
def my_super_task():
    # try:
    raise IOError("File X does not exists")
    # except IOError as e:
    #     logging.error(e)

@app.task(bind=True, queue='celery')
def is_positive_number(self, num: int):
    try:
        if num < 0:
            raise ValueError(f"{num} is negative..")
        return True
    except Exception as e:
        traceback_str = traceback.format_exc()
        handle_error.apply_async(args=[self.request.id, str(e), traceback_str])

@app.task(queue="dlq")
def handle_error(task_id, exception, traceback_str):
    print(f"task_id: {task_id}")
    print(f"exception: {exception}")
    print(f"traceback_str: {traceback_str}")

# @app.task(queue="celery", time_limit=5)
@app.task(queue="celery")
def long_running_job():
    time.sleep(10)
    print("finished long_running_job")

# https://docs.celeryq.dev/en/stable/userguide/canvas.html#group-results
def run_group():
    g = group(
        is_positive_number.s(2),
        is_positive_number.s(4),
        is_positive_number.s(-1)) # type: ignore
    result = g.apply_async()

    print(f"ready: {result.ready()}")  # have all subtasks completed?
    print(f"successful: {result.successful()}") # were all subtasks successful?

    try:
        result.get()
    except ValueError as e:
        print(e)

    print(f"ready: {result.ready()}")  # have all subtasks completed?
    print(f"successful: {result.successful()}") # were all subtasks successful?

    for elem in result:
        print(elem.status)


def simulating_timeout():
    result = long_running_job.delay()
    result.get(timeout=3)


@app.task(queue="celery")
def multiply(result, z):
    return result * z

@app.task(queue="celery")
def error_handler(request, exc, traceback):
    print('Task {0} raised exception: {1!r}\n{2!r}'.format(
          request.id, exc, traceback))

def simulating_link():
    result = add.apply_async(
                args=[2, "error"],
                link=multiply.s(10),
                link_error=error_handler.s()) # type: ignore
    # parent result
    print(result.get())
    # child result
    print(result.children[0].get())
