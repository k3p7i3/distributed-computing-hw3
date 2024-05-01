from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.utils.dates import days_ago


default_args = {
    'start_date': days_ago(0),
    'depends_on_past': False,
}

with DAG(
    dag_id="homework3",
    default_args=default_args,
    schedule_interval='@once',
    catchup=False

) as dag:
    
    # create table
    create_order_table = PostgresOperator(
        task_id="create_orders_table",
        postgres_conn_id="postgres_default",
        sql = """
            CREATE TABLE IF NOT EXISTS orders (
                order_id SERIAL PRIMARY KEY,
                item VARCHAR NOT NULL,
                price INTEGER NOT NULL,
                number INTEGER NOT NULL
            );
        """
    )

    # insert data to the table
    populate_order_table = PostgresOperator(
        task_id="populate_orders_table",
        postgres_conn_id="postgres_default",
        sql = """
            INSERT INTO orders (item, price, number) VALUES ('juice', 175, 2);
            INSERT INTO orders (item, price, number) VALUES ('coffee', 120, 3);
            INSERT INTO orders (item, price, number) VALUES ('tea', 95, 1);
        """
    )

    # get rows from the table for further processing
    select_order_table = PostgresOperator(
        task_id="select_orders_table",
        postgres_conn_id="postgres_default",
        sql = """
            SELECT * from orders;
        """
    )


    # calculate the total income from orders table
    def count_total_price(**context):
        orders = eval(context['templates_dict']['orders'])
        print(orders)
        print(len(orders[0]))
        return sum([order[2] * order[3] for order in orders])
    
    python_task = PythonOperator(
        task_id="count_total_price",
        python_callable=count_total_price,
        provide_context=True,
        templates_dict={'orders': "{{ ti.xcom_pull(task_ids='select_orders_table') }}" }
    )

    # log\print the result of total income to console
    bash_task = BashOperator(
        task_id="bash_echo_price",
        bash_command='echo {{ti.xcom_pull(task_ids="count_total_price")}} is the total price of all orders saved in the database'
    )

    # send email with information about total income
    send_email = EmailOperator(
        task_id="send_email",
        to="pikopyrina@edu.hse.ru",
        subject="Airflow Homework3",
        html_content='The total price of all orders is {{ti.xcom_pull(task_ids="count_total_price")}}'
    )

    create_order_table >> populate_order_table >> select_order_table >> python_task
    python_task >> [bash_task, send_email]

