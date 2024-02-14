from worker.celery import app

@app.task(queue='celery')
def my_super_task():
    pass
