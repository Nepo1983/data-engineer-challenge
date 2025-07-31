import os
import json
import requests
import pandas as pd
import logging
import pyarrow as pa
import pyarrow.parquet as pq
import glob

from datetime import datetime, date
from typing import Optional, List, Dict
from pydantic import BaseModel, validator, PositiveInt, PositiveFloat, confloat, constr
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from airflow.decorators import dag, task

# Constants and Path Configuration
BASE_PATH = '/opt/airflow/data'
BRONZE_PATH = os.path.join(BASE_PATH, 'bronze')
SILVER_PATH = os.path.join(BASE_PATH, 'silver')
GOLD_PATH = os.path.join(BASE_PATH, 'gold')
LOGS_PATH = os.path.join(BASE_PATH, 'logs_dq')

def _ensure_dirs(path_list):
    """Ensure directories exist"""
    for p in path_list:
        os.makedirs(p, exist_ok=True)

# Initialize directories
_ensure_dirs([BRONZE_PATH, SILVER_PATH, GOLD_PATH, LOGS_PATH])

# Pydantic Models for Data Validation
class Order(BaseModel):
    id: PositiveInt
    customer_id: PositiveInt
    order_date: datetime
    total_amount: confloat(gt=0)
    currency: constr(strip_whitespace=True, min_length=3) 
    status: constr(strip_whitespace=True, min_length=1)
    
    @validator('status')
    def validate_status(cls, v):
        valid_statuses = {'pending', 'completed', 'cancelled', 'shipped'}
        if v.lower() not in valid_statuses:
            raise ValueError(f"Invalid order status: {v}. Must be one of {valid_statuses}")
        return v.lower()

class OrderItem(BaseModel):
    id: PositiveInt
    order_id: PositiveInt
    product_id: PositiveInt
    quantity: PositiveInt
    unit_price: confloat(gt=0)
    currency: constr(strip_whitespace=True, min_length=3)
    
    @validator('unit_price')
    def round_price(cls, v):
        return round(v, 2)

class Customer(BaseModel):
    id: PositiveInt
    name: constr(strip_whitespace=True, min_length=1)
    email: constr(strip_whitespace=True, min_length=3)
    registration_date: datetime
    country: constr(strip_whitespace=True, min_length=2)
    
    @validator('email')
    def validate_email(cls, v):
        if '@' not in v:
            raise ValueError("Invalid email format")
        return v.lower()

class Product(BaseModel):
    id: PositiveInt
    name: constr(strip_whitespace=True, min_length=1)
    category: constr(strip_whitespace=True, min_length=1)
    description: constr(strip_whitespace=True, min_length=1)
    base_price: confloat(gt=0)
    currency: constr(strip_whitespace=True, min_length=3)    

class CurrencyRate(BaseModel):
    rate_date: date
    currency_from: constr(strip_whitespace=True, min_length=3, max_length=3)
    currency_to: constr(strip_whitespace=True, min_length=3, max_length=3)
    rate: confloat(gt=0)

def validate_dataframe(df: pd.DataFrame, model: BaseModel) -> pd.DataFrame:
    """Validate DataFrame against a Pydantic model"""
    errors = []
    validated_records = []
    
    for record in df.to_dict('records'):
        try:
            validated = model(**record).dict()
            validated_records.append(validated)
        except Exception as e:
            errors.append(f"Record {record.get('id', 'unknown')} failed validation: {str(e)}")
    
    if errors:
        raise AirflowException(f"Data validation failed: {' '.join(errors)}")
    
    return pd.DataFrame(validated_records)


# Extraction Functions with Validation using PostgresHook
def extract_data(table: str, model: BaseModel, conn_id: str):
    """Generic extraction function with validation using PostgresHook"""
    date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    try:
        hook = PostgresHook(postgres_conn_id=conn_id)
        df = pd.read_sql(f'SELECT * FROM {table}', hook.get_conn())
        print(f'Loaded rows --> {df.count()[0]}')
        print(f'Columns:\n{df.columns}')
        
        # Validate data
        df = validate_dataframe(df, model)
        
        # Save to bronze layer
        os.makedirs(BRONZE_PATH, exist_ok=True)
        output_path = os.path.join(BRONZE_PATH, f'{table}_{date_str}.parquet')
        
        table = pa.Table.from_pandas(df)
        pq.write_table(table, output_path)
        
        # df.to_parquet(os.path.join(BRONZE_PATH, f'{table}_{date_str}.parquet'), index=False)
        print(f'Path -> {output_path}')
        
    except Exception as e:
        raise AirflowException(f"Failed to extract {table}: {str(e)}")
    
    
    return output_path

def extract_orders():
    extract_data('orders', Order, 'e_orders_conn_id')

def extract_order_items():
    extract_data('order_items', OrderItem, 'e_orders_conn_id')

def extract_customers():
    extract_data('customers', Customer, 'e_orders_conn_id')

def extract_products():
    extract_data('product_descriptions', Product, 'e_products_conn_id')

def extract_currency_rates():
    """ It uses API not DB """
    _ensure_dirs([BRONZE_PATH])
    date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    try:
        url = 'https://api.exchangerate-api.com/v4/latest/USD'
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        
        data = resp.json().get('rates', {})
        df = pd.DataFrame([
            {'rate_date': date_str, 'currency_from': 'USD', 'currency_to': k, 'rate': v} 
            for k, v in data.items()
        ])
        
        # Validate rates
        df = validate_dataframe(df, CurrencyRate)
        df.to_parquet(os.path.join(BRONZE_PATH, f'rates_{date_str}.parquet'), index=False)
    except Exception as e:
        raise AirflowException(f"Failed to extract currency rates: {str(e)}")

# Transformation with Data Quality Checks using PostgresHook
def transform_and_load():
    date_str = datetime.now().date().isoformat()
    
    try:
        # Read bronze
        order_files = glob.glob(os.path.join(BRONZE_PATH, 'orders_*.parquet'))
        df_orders = pd.concat([pd.read_parquet(file) for file in order_files], ignore_index=True)
        
        items_files = glob.glob(os.path.join(BRONZE_PATH, 'order_items_*.parquet'))
        df_items = pd.concat([pd.read_parquet(file) for file in items_files], ignore_index=True)
        
        customers_files = glob.glob(os.path.join(BRONZE_PATH, 'customers_*.parquet'))
        df_customers = pd.concat([pd.read_parquet(file) for file in customers_files], ignore_index=True)
        df_customers[['first_name', 'last_name']] = df_customers['name'].str.split(' ', expand=True)
        df_customers.drop(columns=['name'], inplace=True)
        df_customers.drop_duplicates(inplace=True)
        
        products_files = glob.glob(os.path.join(BRONZE_PATH, 'product_*.parquet'))
        df_products = pd.concat([pd.read_parquet(file) for file in products_files], ignore_index=True)
        df_products.drop_duplicates(inplace=True)
        
        rates_files = glob.glob(os.path.join(BRONZE_PATH, 'rates_*.parquet'))
        df_rates = pd.concat([pd.read_parquet(file) for file in rates_files], ignore_index=True)
        
        # Build time dimension
        df_orders['sale_date'] = pd.to_datetime(df_orders['order_date']).dt.date
        df_orders['sale_hour'] = pd.to_datetime(df_orders['order_date']).dt.hour
        df_time = df_orders[['sale_date','sale_hour']].drop_duplicates()
        df_time['day_of_week'] = pd.to_datetime(df_time['sale_date']).dt.day_name()
        df_time['month'] = pd.to_datetime(df_time['sale_date']).dt.month
        df_time['year'] = pd.to_datetime(df_time['sale_date']).dt.year
        df_time.to_parquet(os.path.join(SILVER_PATH, f'dim_time_{date_str}.parquet'), index=False)

        # Join and compute revenue
        df = df_items.merge(df_orders[['id','customer_id','sale_date','sale_hour']], left_on='order_id', right_on='id')
        df = df.merge(df_products[['id','name','category']], left_on='product_id', right_on='id')
        df = df.merge(df_rates.rename(columns={'rate_date':'sale_date'}), on=['sale_date'], how='left')
        df['revenue_usd'] = df['quantity'] * df['unit_price'] * df['rate']
        df.to_parquet(os.path.join(SILVER_PATH, f'fact_sales_{date_str}.parquet'), index=False)
        print(f'df columns\n{df.columns}')
        print(df)

        # Load to Data Warehouse using PostgresHook
        warehouse_hook = PostgresHook(postgres_conn_id='dw_conn_id')
        conn = warehouse_hook.get_conn()
        with conn.cursor() as cur:
            # Insert dim_time
            cur.execute('DELETE FROM dim_time WHERE sale_date = %s;', (date_str,))
            for _, row in df_time.iterrows():
                cur.execute(
                    "INSERT INTO dim_time(sale_date, sale_hour, day_of_week, month, year) VALUES(%s, %s, %s, %s, %s)",
                    (row['sale_date'], row['sale_hour'], row['day_of_week'], row['month'], row['year'])
                )
            
            # Insert dim_currency_rate
            cur.execute('DELETE FROM dim_currency_rate WHERE rate_date = %s;', (date_str,))
            for _, row in df_rates.iterrows():
                cur.execute(
                    "INSERT INTO dim_currency_rate(rate_date, currency_from, currency_to, rate) VALUES(%s, %s, %s, %s)",
                    (row['rate_date'], row['currency_from'], row['currency_to'], row['rate'])
                )
                        
            # Insert dim_customer
            cur.execute('DELETE FROM dim_customer WHERE customer_id IN (SELECT customer_id FROM dim_customer);')
            for _, row in df_customers.iterrows():
                cur.execute(
                    "INSERT INTO dim_customer(customer_id, first_name, last_name, email, signup_date) VALUES(%s, %s, %s, %s, %s)",
                    (row['id'], row['first_name'], row['last_name'], row['email'], row['registration_date'])
                )
                
            # Insert dim_product
            cur.execute('DELETE FROM dim_product WHERE product_id IN (SELECT product_id FROM dim_product);')
            for _, row in df_products.iterrows():
                cur.execute(
                    "INSERT INTO dim_product(product_id, name, description, category) VALUES(%s, %s, %s, %s)",
                    (row['id'], row['name'], row['description'], row['category'])
                )
            
            # Insert fact_sales
            for _, row in df.iterrows():
                cur.execute(
                    'SELECT time_id FROM dim_time WHERE sale_date=%s AND sale_hour=%s;',
                    (row['sale_date'], row['sale_hour'])
                )
                time_id = cur.fetchone()[0]
                cur.execute(
                    "INSERT INTO fact_sales(order_id, customer_id, product_id, time_id, quantity, unit_price, revenue_usd) VALUES(%s, %s, %s, %s, %s, %s, %s)",
                    (row['order_id'], row['customer_id'], row['product_id'], time_id, row['quantity'], row['unit_price'], row['revenue_usd'])
                )
            
            conn.commit()
        conn.close()
    except Exception as e:
        raise AirflowException(f"Transform and load failed: {str(e)}")

# Quality Check Task Implementation
def quality_checks():
    """Perform quality checks and log errors without failing the DAG"""
    date_str = datetime.now().date().isoformat()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    error_logs = {}
    
    try:
        # Check 1: Verify data completeness in silver layer
        check_name = "silver_data_completeness"
        required_files = [
            f'dim_time_{date_str}.parquet',
            f'fact_sales_{date_str}.parquet'
        ]
        
        missing_files = [
            f for f in required_files 
            if not os.path.exists(os.path.join(SILVER_PATH, f))
        ]
        
        if missing_files:
            error_logs[check_name] = {
                "error": "Missing silver layer files",
                "missing_files": missing_files,
                "timestamp": timestamp
            }
    
    except Exception as e:
        error_logs[check_name] = {
            "error": f"Error during {check_name}: {str(e)}",
            "timestamp": timestamp
        }
    
    try:
        # Check 2: Verify revenue calculations
        check_name = "revenue_calculation_validation"
        df_sales = pd.read_parquet(os.path.join(SILVER_PATH, f'fact_sales_{date_str}.parquet'))
        
        # Calculate expected revenue
        df_sales['expected_revenue'] = df_sales['quantity'] * df_sales['unit_price'] * df_sales['rate']
        
        # Find discrepancies
        discrepancies = df_sales[
            abs(df_sales['revenue_usd'] - df_sales['expected_revenue']) > 0.01
        ]
        
        if not discrepancies.empty:
            error_logs[check_name] = {
                "error": "Revenue calculation discrepancies found",
                "discrepancy_count": len(discrepancies),
                "sample_discrepancies": discrepancies[
                    ['order_id', 'product_id', 'revenue_usd', 'expected_revenue']
                ].head().to_dict('records'),
                "timestamp": timestamp
            }
    
    except Exception as e:
        error_logs[check_name] = {
            "error": f"Error during {check_name}: {str(e)}",
            "timestamp": timestamp
        }
    
    try:
        # Check 3: Verify foreign key relationships
        check_name = "foreign_key_validation"
        df_sales = pd.read_parquet(os.path.join(SILVER_PATH, f'fact_sales_{date_str}.parquet'))
        df_time = pd.read_parquet(os.path.join(SILVER_PATH, f'dim_time_{date_str}.parquet'))
        
        # Check time references
        invalid_time_refs = []
        for _, row in df_sales.iterrows():
            time_match = df_time[
                (df_time['sale_date'] == row['sale_date']) & 
                (df_time['sale_hour'] == row['sale_hour'])
            ]
            if time_match.empty:
                invalid_time_refs.append({
                    'order_id': row['order_id'],
                    'sale_date': str(row['sale_date']),
                    'sale_hour': row['sale_hour']
                })
        
        if invalid_time_refs:
            error_logs[check_name] = {
                "error": "Invalid time references found",
                "invalid_references_count": len(invalid_time_refs),
                "sample_invalid_references": invalid_time_refs[:5],
                "timestamp": timestamp
            }
    
    except Exception as e:
        error_logs[check_name] = {
            "error": f"Error during {check_name}: {str(e)}",
            "timestamp": timestamp
        }
    
    # Write error logs for each failed check
    for check_name, log_data in error_logs.items():
        log_file = os.path.join(LOGS_PATH, f"{check_name}_{timestamp}.json")
        with open(log_file, 'w') as f:
            json.dump(log_data, f, indent=2)

# DAG Definition
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 30),
    'retries': 1,
}

dag = DAG(
    'ecommerce_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

# Define tasks
extract_orders_task = PythonOperator(task_id='extract_orders', python_callable=extract_orders, dag=dag)
extract_items_task = PythonOperator(task_id='extract_order_items', python_callable=extract_order_items, dag=dag)
extract_customers_task = PythonOperator(task_id='extract_customers', python_callable=extract_customers, dag=dag)
extract_products_task = PythonOperator(task_id='extract_products', python_callable=extract_products, dag=dag)
extract_rates_task = PythonOperator(task_id='extract_currency_rates', python_callable=extract_currency_rates, dag=dag)
transform_and_load_task = PythonOperator(task_id='transform_and_load', python_callable=transform_and_load, dag=dag)
quality_check_task = PythonOperator(
    task_id='quality_checks',
    python_callable=quality_checks,
    dag=dag,
    trigger_rule='all_done'  # Run even if previous tasks fail
)

# Task ordering
defaults = [extract_orders_task, extract_items_task, extract_customers_task, extract_products_task]
for t in defaults:
    t >> extract_rates_task
extract_rates_task >> transform_and_load_task >> quality_check_task