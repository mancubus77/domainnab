import datetime
import os
from sqlalchemy import __version__ as pgsql_version
from sqlalchemy import create_engine, Table, Column, MetaData, JSON, Date
from sqlalchemy.exc import ProgrammingError, IntegrityError


class Psql:
    def __init__(self, database="domain", table="listings"):
        print(f"sqlalchemy version: {pgsql_version}")
        db_string = f"postgresql://{os.getenv('PSQL_USERNAME')}:{os.getenv('PSQL_PASSWORD')}@{os.getenv('PSQL_HOST')}/"
        db = create_engine(db_string, isolation_level="AUTOCOMMIT")

        with db.connect() as conn:
            print(f"Creating database {database}")
            try:
                conn.execute("CREATE DATABASE {}".format(database))
                print(f"Database {database} has been created")
            except ProgrammingError:
                print(f"Database {database} Exists")

        db_string = f"postgresql://{os.getenv('PSQL_USERNAME')}:{os.getenv('PSQL_PASSWORD')}@{os.getenv('PSQL_HOST')}/{database}"
        self.db = create_engine(db_string, isolation_level="AUTOCOMMIT")
        # Table
        meta = MetaData(self.db)
        self.listings_table = Table(
            "listings",
            meta,
            Column("data", JSON),
            Column("ts", Date),
        )
        with self.db.connect() as self.conn:
            try:
                self.listings_table.create()
                print(f"Table {table} has been created")
                self.conn.execute(
                    "CREATE UNIQUE INDEX ui_products_id ON listings((data->>'id'))"
                )
            except ProgrammingError:
                print(f"Table {table} exists")

    def insert_document(self, document) -> None:
        """
        Insert document into database
        @param document: document
        @return: None
        """
        with self.db.connect() as self.conn:
            statement = self.listings_table.insert().values(data=document)
            try:
                self.conn.execute(statement)
            except IntegrityError:
                pass


if __name__ == "__main__":
    p = Psql()
