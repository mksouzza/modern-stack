from time import sleep
from airflow.decorators import dag, task # Correct import for the decorator
from datetime import datetime

@dag(
    dag_id="test",
    description="new etl",
    schedule="@daily",
    start_date=datetime(2025,7,9), # Using the current date
    catchup=False,
)
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

# You no longer call pipeline() explicitly like pipeline().
# The @dag decorator handles registration.