from airflow.hooks.base import BaseHook
import boto3
import json
import logging
import pandas as pd
import vertica_python
import contextlib
import json
from typing import Dict, List, Optional

def insert_file_to_vertica(
    dataset_path: str,
    table: str,
    columns: List[str],
    type_override: Optional[Dict[str, str]] = None
):
    df = pd.read_csv(dataset_path, dtype=type_override)
    num_rows = len(df)
    log = logging.getLogger(__name__)
    vert_conn = BaseHook.get_connection('vertica_db')

    vertica_conn = vertica_python.connect(
        host=vert_conn.host,
        port=vert_conn.port,
        user=vert_conn.login,
        password=vert_conn.password
    )
    copy_expr = f"""
    COPY STV2023081257__STAGING.{table} ({columns}) FROM STDIN DELIMITER ',' ENCLOSED BY '"';
    """
    chunk_size = num_rows // 100
    with contextlib.closing(vertica_conn.cursor()) as cur:
        start = 0
        while start <= num_rows:
            end = min(start + chunk_size, num_rows)
            log.info(f"loading rows {start}-{end}")
            df.loc[start: end].to_csv('/tmp/chunk.csv', index=False)
            with open('/tmp/chunk.csv', 'rb') as chunk:
                cur.copy(copy_expr, chunk, buffer_size=65536)
            vertica_conn.commit()
            log.info("loaded")
            start += chunk_size + 1

    vertica_conn.close()

def load_s3_file(bucket: str, key: str, type_override: Optional[Dict[str, str]] = None):
    log = logging.getLogger(__name__)
    s3_conn = json.loads(BaseHook.get_connection('yandex_cloud').get_extra())
    AWS_ACCESS_KEY_ID = s3_conn['AWS_ACCESS_KEY_ID']
    AWS_SECRET_ACCESS_KEY = s3_conn['AWS_SECRET_ACCESS_KEY']

    cols_dict = {'groups': 'id,admin_id,group_name,registration_dt,is_private',
                 'users': 'id,chat_name,registration_dt,country,age',
                 'dialogs': 'message_id,message_ts,message_from,message_to,message,message_group',
                 'group_log': 'group_id, user_id, user_id_from, event, event_dt'}

    log.info(f'Params for load: columns = {cols_dict[key]}, file = {key}.csv')
    log.info('Create s3 session.')
    
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    log.info(f'Start download file {key}.csv')
    s3_client.download_file(
        Bucket=bucket,
        Key=f'{key}.csv',
        Filename=f'/data/{key}.csv'
    )
    
    log.info(f'File {key}.csv downloaded.')

    insert_file_to_vertica(f'/data/{key}.csv', key, cols_dict[key], type_override)

