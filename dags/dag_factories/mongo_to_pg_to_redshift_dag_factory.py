"""
Copy resources from mongo to postgres to redshift
"""
from abc import ABC, abstractmethod
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

import plugins.alert_management as alert_management


# dag_name='daily_order_price_pop_dev'
# schedule_interval='@daily'
# start_date=datetime(2019, 8, 20)


class MongoToPgToRedshiftDagFactory(ABC):
    def __init__(
        self,
        dag_name,
        schedule_interval,
        start_date,
        end_date=None,
        additional_default_args={}
    ):
        assert dag_name
        assert schedule_interval
        assert start_date
        self.dag_name = dag_name
        self.schedule_interval = schedule_interval
        self.start_date = start_date
        self.end_date = end_date
        self.additional_default_args = additional_default_args
        self._setup_factory()

    def make_dag(self):
        self.dag = DAG(
            self.dag_name,
            default_args=self.default_args,
            schedule_interval=self.schedule_interval,
            catchup=True
        )
        return self.dag

    def setup_operators(self, dag):
        with dag:
            kick_off = self._make_kick_off_operator()
            mongo_to_pg = self._make_mongo_to_pg_operator()

            (
                kick_off >>
                mongo_to_pg
            )

    def _setup_factory(self):
        self._compute_default_args()
        assert self.default_args

    def _compute_default_args(self):
        self.default_args = {
            **self._date_args(),
            'retries': 2,
            'retry_delay': timedelta(minutes=5),
            'email': alert_management.alert_receiver_emails(),
            'email_on_failure': True,
            'email_on_retry': False,
            **self.additional_default_args
        }

    def _date_args(self):
        if (self.end_date):
            return {
                'start_date': self.start_date,
                'end_date': self.end_date
            }
        return {
            'start_date': self.start_date
        }

    def _make_kick_off_operator(self):
        return DummyOperator(task_id='kick_off')

    def _make_mongo_to_pg_operator(self):
        operator_klass = self._mongo_to_pg_operator_klass()
        task_id = self._mongo_to_pg_operator_task_id()
        assert operator_klass
        assert task_id
        return operator_klass(
            task_id=task_id
        )

    @abstractmethod
    def _mongo_to_pg_operator_task_id(self):
        raise NotImplementedError(
                """
                    Example implementation:
                    return 'dev_orders_lifecycle_mongo_to_pg'
                """
        )

    @abstractmethod
    def _mongo_to_pg_operator_klass(self):
        raise NotImplementedError(
            """
                Example implementation:
                return TransferOrderPriceFromMongoToPgOperator
            """
        )
