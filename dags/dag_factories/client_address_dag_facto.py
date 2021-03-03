"""
Copy client addresses from mongo to postgres to redshift
"""
from plugins.mongo_to_pg.operators import (
    TransferClientAddressFromMongoToPgOperator
)

from dags.dag_factories import (
    MongoToPgToRedshiftDagFactory
)


class ClientAddressDagFacto(
    MongoToPgToRedshiftDagFactory,
):
    def _mongo_to_pg_operator_klass(self):
        return TransferClientAddressFromMongoToPgOperator
