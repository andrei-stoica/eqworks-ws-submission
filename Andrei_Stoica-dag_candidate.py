#from msilib.schema import tables
import os
import random
from typing import List

import sqlalchemy
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago

MODULE_NAME = os.path.splitext(os.path.basename(__file__))[0]
DAG_ID = MODULE_NAME

DB_HOST = 'postgres'
DB_PORT = '5432'
DB_USERNAME = 'airflow'
DB_PASSWORD = 'airflow'
DB_DATABASE = 'airflow'

DB_URI = sqlalchemy.engine.url.URL(
     'postgresql+psycopg2',
     username=DB_USERNAME,
     password=DB_PASSWORD,
     host=DB_HOST,
     port=DB_PORT,
     database=DB_DATABASE,
 )

JOB_TABLE_NAME = 'dummy_job'
JOB_RESULT_TABLE_NAME = 'dummy_job_result'

QUERY_JOB_TABLE_CREATION = f"""
    CREATE TABLE IF NOT EXISTS {JOB_TABLE_NAME} (
        id SERIAL PRIMARY KEY,
        is_active BOOLEAN NOT NULL DEFAULT TRUE
    );
"""
QUERY_JOB_RESULT_TABLE_CREATION = f"""
    CREATE TABLE IF NOT EXISTS {JOB_RESULT_TABLE_NAME} (
        job_id INT UNIQUE NOT NULL REFERENCES {JOB_TABLE_NAME}(id),
        is_gt_th BOOLEAN,
        is_successful BOOLEAN
    );
"""

# The string may be changed to another one with length ranging from 0 to
# 1 million in different test cases.
NUM_STR = '1867-07-01T00:00:00.000'

RAND_DIGIT_AMOUNT = 10 ** 6
HIT_COUNT_THRESHOLD = RAND_DIGIT_AMOUNT // 2

# IDs of all Airflow tasks in the DAG
TASK_ID_CREATE_TABLES = 'create_tables'
TASK_ID_INSERT_RECS = 'insert_recs'
TASK_ID_GET_HIT_COUNT = 'get_hit_count'
TASK_ID_BRANCHING = 'branching'
TASK_ID_ACTION_ON_LTE_THRESHOLD = 'action_on_lte_threshold'
TASK_ID_ACTION_ON_GT_THRESHOLD = 'action_on_gt_threshold'
TASK_ID_ACTION_ON_ERROR = 'action_on_error'


random.seed()


# Helper functions/classes ####################################################

def get_rand_digit() -> int:
    """Return a random digit between 0 and 9 (inclusive)."""
    return random.randint(0, 9)


def get_engine() -> sqlalchemy.engine.Engine:
    return sqlalchemy.create_engine(DB_URI)


def is_raise_error() -> bool:
    """Returns True with a probability of 0.3."""
    return get_rand_digit() < 3


def get_id(instance) -> int:
    "returns the id generated in [TASK_ID_INSERT_RECS]"
    return instance.xcom_pull(key = 'id', task_ids = [TASK_ID_INSERT_RECS])[0]


def gen_update_query(table, where_col,  **values) -> sqlalchemy.text:
    """returns an SQL quere for updating a table with the values supplied.

    Args:
        table       (str) : name of the tabe to be updated
        where_col   (str) : column name for where clause
        values            : new set of values

    Returns:
        sqlalchemy.text   : Generated SQL UPDATE query
    """
    update = f"UPDATE {table} SET "
    cols = [f"{col} = {values[col]}" for col in values]
    where = f" WHERE {where_col} = :id"
    
    return sqlalchemy.text(update + ", ".join(cols) + where)



# Functions for the tasks in the DAG ##########################################


def create_tables(**kwargs) -> None:
    """Create table 'dummy_job' and table 'dummy_job_result' if they do not
    exist.

    This function will be run in a DAG task with task ID
    [TASK_ID_CREATE_TABLES].

    The query for the table creation can be found in variable
    [QUERY_JOB_TABLE_CREATION] and variable [QUERY_JOB_RESULT_TABLE_CREATION].

    The tables can be created either by executing the provided raw query,
    or by using ORM (based on the information in the provided query).
    """

    with get_engine().begin() as conn:
        conn.execute(sqlalchemy.text(QUERY_JOB_TABLE_CREATION))
        conn.execute(sqlalchemy.text(QUERY_JOB_RESULT_TABLE_CREATION))


def insert_recs(**kwargs) -> None:
    """Insert a new record into table 'dummy_job' and table
    'dummy_job_result' respectively.

    This function will be run in a DAG task with task ID
    [TASK_ID_INSERT_RECS].

    Actions:
        - insert a record into table [JOB_TABLE_NAME]. The DB will
            automatically generate an integer for column 'id'. We regard
            this ID as the job ID for the current pipeline run. Store that
            value in a way that other DAG task in the same run can access.
            No need to set values for any other columns.
        - insert a record into table [JOB_RESULT_TABLE_NAME], with the value
            in column 'job_id' being the same with the 'id' value obtained
            in the previous task. No need to set values for any other columns.
        - add other thing you think necessary.
    """
    task_instance = kwargs['task_instance']
    query_insert_job_table =  f"""
        INSERT INTO {JOB_TABLE_NAME} 
        DEFAULT VALUES
        RETURNING id;
    """
    query_insert_job_result_table = f"""
        INSERT INTO {JOB_RESULT_TABLE_NAME}(job_id) 
        VALUES (:id);
    """
    
    with get_engine().begin() as conn:
        result = conn.execute(sqlalchemy.text(query_insert_job_table))
        id = result.first()['id']
        
        conn.execute(sqlalchemy.text(query_insert_job_result_table), id = id)
    
    task_instance.xcom_push(key = 'job_id', value = id)


def get_hit_count(
        num_str: str,
        rand_digit_amount: int,
        **kwargs
) -> None:
    """
    Args:
        num_str: a string that contains ASCII characters.
        rand_digit_amount: total amount of random digits to generate.

    This function will be run in a DAG task with task ID
    [TASK_ID_GET_HIT_COUNT].

    Actions:
      - randomly generate a random digit (from 0 to 9) and tell whether
        it can be found in string [num_str].
      - repeat the above step for [rand_digit_amount] times.
      - store the amount of times that a random digit is found in the given
          string, such that the value can be accessed by other DAG tasks
          in the same run.
      - add other thing you think necessary.
    """
    # Simulate a scenario that this task fails for unknown reasons
    if is_raise_error():
        raise ValueError

    task_instance = kwargs['task_instance']

    count = 0
    for n in range(RAND_DIGIT_AMOUNT):
        x = str(get_rand_digit())
        if x in NUM_STR:
            count += 1

    
    task_instance.xcom_push(key = 'rand_digit_count', value=count)


def branching(**kwargs) -> List[str]:
    """Returns the list of task IDs for the next tasks.

    This function will be run in a DAG task with task ID [TASK_ID_BRANCHING].
    That DAG task is based on 'BranchPythonOperator'. Please refer to the
    official documentation for 'BranchPythonOperator'.

    Actions:
        - Get the hit count in task [TASK_ID_GET_HIT_COUNT].
        - If the hit count in [TASK_ID_GET_HIT_COUNT] is greater than
            [HIT_COUNT_THRESHOLD], the next task ID would be
            [TASK_ID_ACTION_ON_GT_THRESHOLD].
        - Otherwise, the next task ID would be
            [TASK_ID_ACTION_ON_LTE_THRESHOLD].
        - add other thing you think necessary.
    """
    task_instance = kwargs['task_instance']
    rand_digit_count = task_instance.xcom_pull(key = 'rand_digit_count',
                                               task_ids = [TASK_ID_GET_HIT_COUNT])[0]

    if rand_digit_count > HIT_COUNT_THRESHOLD:
        return [TASK_ID_ACTION_ON_GT_THRESHOLD]

    return [TASK_ID_ACTION_ON_LTE_THRESHOLD]


def action_on_gt_threshold(**kwargs) -> None:
    """Actions to be taken when the hit count is greater than the given
    threshold.

    This function will be run in a DAG task with task ID
    [TASK_ID_ACTION_ON_GT_THRESHOLD].

    Actions:
        - Get the job ID generated in task [TASK_ID_INSERT_RECS].
        - Update the record created in task [TASK_ID_INSERT_RECS] in table
            [JOB_TABLE_NAME]:
            - set column 'is_active' to be 'false'
        - Update the record created in task [TASK_ID_INSERT_RECS] in table
            [JOB_TABLE_RESULT_NAME]:
            - set column 'is_successful' to be 'true'
            - set column 'is_gt_th' to be 'true'
        - add other thing you think necessary.
    """
    id = get_id(kwargs['task_instance'])

    with get_engine().begin() as conn:
        conn.execute(gen_update_query(JOB_TABLE_NAME, 'id', is_active = 'FALSE'),
                     id = id)

        conn.execute(gen_update_query(JOB_RESULT_TABLE_NAME,
                                      'job_id',
                                      is_successful = "TRUE",
                                      is_gt_th = 'TRUE'),
                     id = id)
                                      

def action_on_lte_threshold(**kwargs) -> None:
    """Actions to be taken when the hit count is less than or equal to the
    given threshold.

    This function will be run in a DAG task with task ID
    [TASK_ID_ACTION_ON_LTE_THRESHOLD].

    Actions:
        - Get the job ID generated in task [TASK_ID_INSERT_RECS].
        - Update the record created in task [TASK_ID_INSERT_RECS] in table
            [JOB_TABLE_NAME]:
            - set column 'is_active' to be 'false'
        - Update the record created in task [TASK_ID_INSERT_RECS] in table
            [JOB_TABLE_RESULT_NAME]:
            - set column 'is_successful' to be 'true'
            - set column 'is_gt_th' to be 'false'
        - add other thing you think necessary.
    """
    id = get_id(kwargs['task_instance'])

    with get_engine().begin() as conn:
        conn.execute(gen_update_query(JOB_TABLE_NAME, 'id', is_active = 'FALSE'),
                     id = id)

        conn.execute(gen_update_query(JOB_RESULT_TABLE_NAME,
                                      'job_id',
                                      is_successful = 'TRUE',
                                      is_gt_th = 'FALSE'),
                     id = id)


def action_on_error(**kwargs) -> None:
    """Actions to be taken when an exception is raised.

    This function will be run in a DAG task with task ID
    [TASK_ID_ACTION_ON_ERROR].

    Actions:
        - Get the job ID generated in task [TASK_ID_INSERT_RECS].
        - Update the record created in task [TASK_ID_INSERT_RECS] in table
            [JOB_TABLE_NAME]:
            - set column 'is_active' to be 'false'
        - Update the record created in task [TASK_ID_INSERT_RECS] in table
            [JOB_TABLE_RESULT_NAME]:
            - set column 'is_successful' to be 'false'
            - the value in column 'is_gt_th' doesn't matter
        - add other thing you think necessary.
    """
    id = get_id(kwargs['task_instance'])

    with get_engine().begin() as conn:
        conn.execute(gen_update_query(JOB_TABLE_NAME, 'id', is_active = 'FALSE'),
                     id = id)

        conn.execute(gen_update_query(JOB_RESULT_TABLE_NAME,
                                      'job_id',
                                      is_successful = 'FALSE'),
                     id = id)
        



# DAG creation ##########################################


default_args = {
    'owner': DAG_ID[4:],
    'end_date': '2100-01-01',
    'depends_on_past': False,
}

dag_kwargs = {
    "dag_id": DAG_ID,
    "default_args": default_args,
    "catchup": False,
    'start_date': days_ago(0),
    "schedule_interval": "* * * * *",
}

with DAG(**dag_kwargs) as dag:
    task_create_tables = PythonOperator(
        task_id=TASK_ID_CREATE_TABLES,
        python_callable=create_tables,
    )
    task_insert_recs = PythonOperator(
        task_id=TASK_ID_INSERT_RECS,
        python_callable=insert_recs,
    )
    task_get_hit_count = PythonOperator(
        task_id=TASK_ID_GET_HIT_COUNT,
        python_callable=get_hit_count,
        op_kwargs={"num_str": NUM_STR, "rand_digit_amount": RAND_DIGIT_AMOUNT},
    )
    task_branching = BranchPythonOperator(
        task_id=TASK_ID_BRANCHING,
        python_callable=branching,
    )
    task_action_on_lte_threshold = PythonOperator(
        task_id=TASK_ID_ACTION_ON_LTE_THRESHOLD,
        python_callable=action_on_lte_threshold,
    )
    task_action_on_gt_threshold = PythonOperator(
        task_id=TASK_ID_ACTION_ON_GT_THRESHOLD,
        python_callable=action_on_gt_threshold,
    )
    task_action_on_error = PythonOperator(
        task_id=TASK_ID_ACTION_ON_ERROR,
        python_callable=action_on_error,
        trigger_rule='one_failed',
    )

    task_create_tables >>       \
        task_insert_recs >>     \
        task_get_hit_count >>   \
        task_branching >>       \
        [task_action_on_gt_threshold, task_action_on_lte_threshold] 

    task_get_hit_count >> task_action_on_error 