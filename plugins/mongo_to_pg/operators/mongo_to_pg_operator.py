from abc import abstractmethod
import logging

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class MongoToPgOperator(BaseOperator):

    @apply_defaults
    def __init__(
        self,
        *args,
        **kwargs
    ):
        super(
            MongoToPgOperator,
            self
        ).__init__(*args, **kwargs)

    def execute(self, context):
        self.starting_date = context['prev_execution_date']
        self.end_date = context['next_execution_date']
        assert self.starting_date
        assert self.end_date
        self.log_details()

    def log_details(self):
        logging.info('starting_date', self.starting_date)
        logging.info('end_date', self.end_date)
        logging.info('Operator =>', type(self))
        logging.info('shuttle =>', self._shuttle_klass)

    @abstractmethod
    def _shuttle_klass(self):
        raise NotImplementedError(
            """
                Please Implement this method. Example implementation:

                return OrderCreationsMongoToPgShuttle
            """
        )
