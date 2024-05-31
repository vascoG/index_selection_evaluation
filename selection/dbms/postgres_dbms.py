import logging
import re

import psycopg2

from ..database_connector import DatabaseConnector


class PostgresDatabaseConnector(DatabaseConnector):
    def __init__(self, db_name, autocommit=False):
        DatabaseConnector.__init__(self, db_name, autocommit=autocommit)
        self.db_system = "postgres"
        self._connection = None

        if not self.db_name:
            self.db_name = "postgres"
        self.create_connection()

        self.set_random_seed()

        self.exec_only("SET max_parallel_workers_per_gather = 0;")
        self.exec_only("SET enable_bitmapscan TO off;")

        logging.debug("Postgres connector created: {}".format(db_name))

    def create_connection(self):
        if self._connection:
            self.close()
        self._connection = psycopg2.connect("dbname={}".format(self.db_name))
        self._connection.autocommit = self.autocommit
        self._cursor = self._connection.cursor()

    def enable_simulation(self):
        self.exec_only("create extension hypopg")
        self.commit()

    def database_names(self):
        result = self.exec_fetch("select datname from pg_database", False)
        return [x[0] for x in result]

    # Updates query syntax to work in PostgreSQL
    def update_query_text(self, text):
        text = text.replace(";\nlimit ", " limit ").replace("limit -1", "")
        text = re.sub(r" ([0-9]+) days\)", r" interval '\1 days')", text)
        text = self._add_alias_subquery(text)
        return text

    # PostgreSQL requires an alias for subqueries
    def _add_alias_subquery(self, query_text):
        text = query_text.lower()
        positions = []
        for match in re.finditer(r"((from)|,)[  \n]*\(", text):
            counter = 1
            pos = match.span()[1]
            while counter > 0:
                char = text[pos]
                if char == "(":
                    counter += 1
                elif char == ")":
                    counter -= 1
                pos += 1
            next_word = query_text[pos:].lstrip().split(" ")[0].split("\n")[0]
            if next_word[0] in [")", ","] or next_word in [
                "limit",
                "group",
                "order",
                "where",
            ]:
                positions.append(pos)
        for pos in sorted(positions, reverse=True):
            query_text = query_text[:pos] + " as alias123 " + query_text[pos:]
        return query_text

    def create_database(self, database_name):
        self.exec_only("create database {}".format(database_name))
        logging.info("Database {} created".format(database_name))

    def import_data(self, table, path, delimiter="|", encoding=None):
        if encoding:
            with open(path, "r", encoding=encoding) as file:
                self._cursor.copy_expert(
                    (
                        f"COPY {table} FROM STDIN WITH DELIMITER AS '{delimiter}' NULL "
                        f"AS 'NULL' CSV QUOTE AS '\"' ENCODING '{encoding}'"
                    ),
                    file,
                )
        else:
            with open(path, "r") as file:
                self._cursor.copy_from(file, table, sep=delimiter, null="")

    def indexes_size(self):
        # Returns size in bytes
        statement = (
            "select sum(pg_indexes_size(table_name::text)) from "
            "(select table_name from information_schema.tables "
            "where table_schema='public') as all_tables"
        )
        result = self.exec_fetch(statement)
        return result[0]

    def drop_database(self, database_name):
        statement = f"DROP DATABASE {database_name};"
        self.exec_only(statement)

        logging.info(f"Database {database_name} dropped")

    def create_statistics(self):
        logging.info("Postgres: Run `analyze`")
        self.commit()
        self._connection.autocommit = True
        self.exec_only("analyze")
        self._connection.autocommit = self.autocommit

    def set_random_seed(self, value=0.17):
        logging.info(f"Postgres: Set random seed `SELECT setseed({value})`")
        self.exec_only(f"SELECT setseed({value})")

    def supports_index_simulation(self):
        if self.db_system == "postgres":
            return True
        return False

    def _simulate_index(self, index):
        table_name = index.table()
        statement = (
            "select * from hypopg_create_index( "
            f"'create index on {table_name} "
            f"({index.joined_column_names()})')"
        )
        result = self.exec_fetch(statement)
        return result

    def _drop_simulated_index(self, oid):
        statement = f"select * from hypopg_drop_index({oid})"
        result = self.exec_fetch(statement)

        assert result[0] is True, f"Could not drop simulated index with oid = {oid}."
    
    def _type(self, column):
        statement = f"select data_type from information_schema.columns where table_name = '{column.table}' and column_name = '{column.name}';"
        result = self.exec_fetch(statement)

        column.type = result[0]
        return result[0]

    def _simulate_partition(self, partition):
        table_name = partition.table()
        minimum = partition.column.minimum
        maximum = partition.column.maximum
        median = partition.column.median

        if partition.column.is_text_or_date():
            # remove white trailing spaces
            minimum = minimum.replace('"', '').strip()
            maximum = maximum.replace('"', '').strip()
            median = median.replace('"', '').strip()
            minimum = f"$${minimum}$$"
            maximum = f"$${maximum}$$"
            median = f"$${median}$$"

        statement = (
            f"select hypopg_partition_table( '{table_name}', 'PARTITION BY RANGE ({partition.column.name})');")
        statement1 = (
            f"select hypopg_add_partition('hypo_part_range_{table_name}_1', 'PARTITION OF {table_name} FOR VALUES FROM ({minimum}) TO ({median})');")
        statement2 = (
            f"select hypopg_add_partition('hypo_part_range_{table_name}_2', 'PARTITION OF {table_name} FOR VALUES FROM ({median}) TO ({maximum})');")

        logging.info(statement)
        logging.info(statement1)
        logging.info(statement2)

        result = self.exec_fetch(statement)
        self.exec_fetch(statement1)
        self.exec_fetch(statement2)
        return result

    def _drop_simulated_partition(self, tablename, partition):
        statement = f"SELECT hypopg_drop_table(relid) FROM hypopg_table() WHERE tablename = '{tablename}';"
        result = self.exec_fetch(statement)

        assert result is not None, f"Could not drop simulated partition with tablename = {tablename} and column {partition.column}."

    def get_column_percentiles(self, column):
        if column.name == "order":
            column.name = '"order"'
        statement = f"select max({column.name}) from (select {column.name}, ntile(10) over (order by {column.name}) as percentile from {column.table})as p group by percentile order by percentile;"
        result = self.exec_fetch(statement, one=False)
        return result

    def create_index(self, index):
        table_name = index.table()
        statement = (
            f"create index {index.index_idx()} "
            f"on {table_name} ({index.joined_column_names()})"
        )
        self.exec_only(statement)
        size = self.exec_fetch(
            f"select relpages from pg_class c " f"where c.relname = '{index.index_idx()}'"
        )
        size = size[0]
        index.estimated_size = size * 8 * 1024

    def drop_indexes(self):
        logging.info("Dropping indexes")
        stmt = "select indexname from pg_indexes where schemaname='public'"
        indexes = self.exec_fetch(stmt, one=False)
        for index in indexes:
            index_name = index[0]
            drop_stmt = "drop index {}".format(index_name)
            logging.debug("Dropping index {}".format(index_name))
            self.exec_only(drop_stmt)

    def drop_partitions(self):
        logging.info("Dropping partitions")
        stmt = "select hypopg_reset_table()"
        self.exec_fetch(stmt, one=False)

    # PostgreSQL expects the timeout in milliseconds
    def exec_query(self, query, timeout=None, cost_evaluation=False):
        # Committing to not lose indexes after timeout
        if not cost_evaluation:
            self._connection.commit()
        query_text = self._prepare_query(query)
        if timeout:
            set_timeout = f"set statement_timeout={timeout}"
            self.exec_only(set_timeout)
        statement = f"explain (analyze, buffers, format json) {query_text}"
        try:
            plan = self.exec_fetch(statement, one=True)[0][0]["Plan"]
            result = plan["Actual Total Time"], plan
        except Exception as e:
            logging.error(f"{query.nr}, {e}")
            self._connection.rollback()
            result = None, self._get_plan(query)
        # Disable timeout
        self._cursor.execute("set statement_timeout = 0")
        self._cleanup_query(query)
        return result

    def _cleanup_query(self, query):
        for query_statement in query.text.split(";"):
            if "drop view" in query_statement:
                self.exec_only(query_statement)
                self.commit()

    def _get_cost(self, query):
        query_plan = self._get_plan(query)
        total_cost = query_plan["Total Cost"]
        return total_cost

    def get_raw_plan(self, query):
        query_text = self._prepare_query(query)
        statement = f"explain (format json) {query_text}"
        query_plan = self.exec_fetch(statement)[0]
        self._cleanup_query(query)
        return query_plan

    def _get_plan(self, query):
        query_text = self._prepare_query(query)
        statement = f"explain (format json) {query_text}"
        query_plan = self.exec_fetch(statement)
        if query_plan:
            query_plan = query_plan[0][0]["Plan"]
        else:
            raise Exception(f"Could not get plan for query {query.text}")
        self._cleanup_query(query)
        return query_plan

    def number_of_indexes(self):
        statement = """select count(*) from pg_indexes
                       where schemaname = 'public'"""
        result = self.exec_fetch(statement)
        return result[0]

    def table_exists(self, table_name):
        statement = f"""SELECT EXISTS (
            SELECT 1
            FROM pg_tables
            WHERE tablename = '{table_name}');"""
        result = self.exec_fetch(statement)
        return result[0]

    def database_exists(self, database_name):
        statement = f"""SELECT EXISTS (
            SELECT 1
            FROM pg_database
            WHERE datname = '{database_name}');"""
        result = self.exec_fetch(statement)
        return result[0]
