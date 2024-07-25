"""
This is where the DAG is being run from
"""

import uuid
import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def get_data():
    import requests
    
    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    
    return res

def format_data(raw_res):
    data = {}
    
    location = raw_res['location']
    #data['id'] = uuid.uuid4()
    data['first_name'] = raw_res['name']['first']
    data['last_name'] = raw_res['name']['last']
    data['gender'] = raw_res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = raw_res['email']
    data['username'] = raw_res['login']['username']
    data['dob'] = raw_res['dob']['date']
    data['registered_date'] = raw_res['registered']['date']
    data['phone'] = raw_res['phone']
    data['picture'] = raw_res['picture']['medium']

    return data

def stream_data():
    raw_res = get_data()
    res = format_data(raw_res)
    print(json.dumps(res, indent=3))

    """ default_args = {
        'owner': 'airscholar',
        'start_date': datetime(2023, 9, 3, 10, 00)
    }

    with DAG('user_automation', 
            default_args=default_args,
            schedule_interval='@daily',
            catchup=False) as dag:
        
        streaming_task = PythonOperator(
            task_id = 'stream_data_from_api',
            python_callable='stream_data'
        ) """

stream_data()