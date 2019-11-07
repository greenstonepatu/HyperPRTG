# -----------------------------------------------------------------------------
#created by greenstonepatu
#based on examples provided by Tableau Software
# -----------------------------------------------------------------------------
from pathlib import Path

from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, \
    TableDefinition, \
    escape_string_literal, \
    TableName, \
    HyperException

# The table is called "sensor" in the "PRTG" schema.
group_table = TableDefinition(
    name=TableName("PRTG", "group"),
)


def run_insert_data_into_single_table():
    path_to_database = str(Path(__file__).parent / "data" / "PRTG.hyper")

    # Starts the Hyper Process with telemetry enabled to send data to Tableau.
    # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:

        # Opens Hyper file "PRTG.hyper".
        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database) as connection:

            # The rows to insert into the "PRTG"."group" table.
            path_to_csv = str(Path(__file__).parent / "data" / "group.csv")

            # Load all rows into "sensor" table from the CSV file.
            # `execute_command` executes a SQL statement and returns the impacted row count.


            #truncate table "group" before population
            connection.execute_command(command=f"TRUNCATE table {group_table.table_name}")
            #populate table "group"
            count_in_group_table = connection.execute_command(
                command=f"COPY {group_table.table_name} from {escape_string_literal(path_to_csv)} with "
                f"(format csv, NULL '', delimiter ';', header, quote '\"', escape '\\')")

            print(f"The number of rows inserted in table {group_table.table_name} is {count_in_group_table}.")

            # # Number of rows in the "PRTG"."group" table.
            # # `execute_scalar_query` is for executing a query that returns exactly one row with one column.
            row_count = connection.execute_scalar_query(query=f"SELECT COUNT(*) FROM {group_table.table_name}")
            print(f"The number of rows in table {group_table.table_name} is {row_count}.")

        print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")


if __name__ == '__main__':
    try:
        run_insert_data_into_single_table()
    except HyperException as ex:
        print(ex)
        exit(1)
