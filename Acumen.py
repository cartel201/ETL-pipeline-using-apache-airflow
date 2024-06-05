from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='An ETL pipeline to load employee data',
    schedule_interval=timedelta(days=1),
)
def extract(**kwargs):
    df = pd.read_csv('"D:\Company\employees.csv"')
    return df.to_dict()

def transform(**kwargs):
    ti = kwargs['ti']
    df = pd.DataFrame(ti.xcom_pull(task_ids='extract'))
    
    df = df[(df['Age'] >= 18) & (df['Age'] <= 65)]
    
    df['Salary'].fillna(df['Salary'].mean(), inplace=True)
    
    df['Name'] = df['Name'].str.title()
    
    df['Start Date'] = pd.to_datetime(df['Start Date'])
    df['Tenure'] = datetime.now().year - df['Start Date'].dt.year
    
    df['Salary Category'] = pd.cut(df['Salary'],
                                   bins=[0, 50000, 100000, float('inf')],
                                   labels=['Low', 'Medium', 'High'])
    
    df['Department'] = df['Department'].replace({'Human Resources': 'HR'})
    
    return df.to_dict()

def load(**kwargs):
    ti = kwargs['ti']
    df = pd.DataFrame(ti.xcom_pull(task_ids='transform'))
    
    conn = psycopg2.connect(
        host='localhost',
        dbname='company_db',
        user='root',
        password='root@cartel'
    )
    
    cursor = conn.cursor()
    
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO employees (name, age, department, start_date, salary, tenure, salary_category)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (row['Name'], row['Age'], row['Department'], row['Start Date'], row['Salary'], row['Tenure'], row['Salary Category']))
    
    conn.commit()
    cursor.close()
    conn.close()

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> load_task