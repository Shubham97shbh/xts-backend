from celery import Celery
from sqlalchemy import create_engine, text

celery = Celery("tasks", broker="redis://localhost:6379/0")
celery.conf.worker_send_task_events = True
connection_string = "mysql+mysqldb://root:root@127.0.0.1:3306/zerodha"
engine = create_engine(connection_string, echo=False)
conn = engine.connect()


@celery.task
def process_data(ltp, id):
    try:
        if id in [26000, 26001, 26121, 26034, 26065, 26118]:
            conn.execute(
                text(
                    f"INSERT INTO ind (Id, ltp) VALUES ('{id}', {ltp}) ON DUPLICATE KEY UPDATE ltp=VALUES(ltp)"
                )
            )
        else:
            conn.execute(
                text(
                    f"INSERT INTO rates (Id, ltp) VALUES ('{id}', {ltp}) ON DUPLICATE KEY UPDATE ltp=VALUES(ltp)"
                )
            )
        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
