from time import sleep

from airflow.models.dag import DAG
from airflow.decorators import task
from datetime import datetime

@dag(dag_id = "test", description = "new etl", schedule = "@daily", start_date=datetime(2025,7,9), catchup=False)
def pipeline():

    @task
    def first_activity():
        print("first activity ran successfully")
        sleep(2)

    @task
    def second_activity():
        print("second activity ran successfully")
        sleep(2)

    @task
    def third_activity():
        print("third activity ran successfully")
        sleep(2)

    t1 = first_activity()
    t2 = second_activity()
    t3 = third_activity()

    t1 >> t2 >> t3

pipeline()