from plugins.mongo_to_pg.db_helpers import ClientAddressesMongoToPgShuttle
from plugins.mongo_to_pg.operators import MongoToPgOperator


class TransferClientAddressFromMongoToPgOperator(
    MongoToPgOperator
):
    def _shuttle_klass(self):
        return ClientAddressesMongoToPgShuttle
