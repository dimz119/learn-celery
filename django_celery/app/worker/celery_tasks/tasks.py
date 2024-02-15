import logging
from worker.celery import app

@app.task(queue='celery')
def my_super_task():
    try:
        raise IOError("File X does not exists")
    except IOError as e:
        logging.error(e)
