"""
Copy client addresses from mongo to postgres to redshift, every_10_min
"""
from datetime import datetime
from dags.dag_factories import ClientAddressDagFacto

###################################
#      %%% MAGIC COMMENT %%%      #
# Force airflow DAG autodiscovery #
# from airflow import DAG         #
###################################

DAG_NAME = 'every_10_min_fake_client_address_dag'

factory = ClientAddressDagFacto(
    dag_name=DAG_NAME,
    schedule_interval='*/10 * * * *',
    start_date=datetime(2021, 1, 30),
)

dag = factory.make_dag()

# Dag tasks must be setup in global scope
with dag:
    factory.setup_operators(dag)
