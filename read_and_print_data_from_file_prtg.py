# -----------------------------------------------------------------------------
#created by greenstonepatu
#based on examples provided by Tableau Software
# -----------------------------------------------------------------------------
import shutil

from pathlib import Path

from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, \
    escape_name, escape_string_literal, \
    TableName, \
    HyperException


def run_read_data_from_existing_hyper_file():
    """
    An example of how to read and print data from an existing Hyper file.
    """
    print("EXAMPLE - Read data from an existing Hyper file")

    # Path to a Hyper file containing all data inserted into tables.
    # See "insert_data_into_multiple_tables.py" for an example that works with the complete schema.
    path_to_source_database = Path(__file__).parent / "data" / "PRTG.hyper"

    # Make a copy of the denormalised Hyper file.
    # path_to_database = shutil.copy(src=path_to_source_database, dst="prtg_read.hyper")
    path_to_database = shutil.copy(src=path_to_source_database, dst=Path(__file__).parent / "data" / "prtg_read.hyper")

    # Starts the Hyper Process with telemetry enabled to send data to Tableau.
    # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:

        # Connect to existing Hyper file "prtg.read.hyper".
        with Connection(endpoint=hyper.endpoint, database=path_to_database) as connection:
            # The table names in the "PRTG" schema.
            table_names = connection.catalog.get_table_names(schema="PRTG")
            for table in table_names:
                table_definition = connection.catalog.get_table_definition(name=table)
                print(f"Table {table.name} has qualified name: {table}")
                # # `execute_list_query` executes a SQL query and returns the result as list of rows of data,
                # # each represented by a list of objects.
                rows_in_table = connection.execute_list_query(query=f"SELECT * FROM {table}")
                print(f"The number of rows in table {table.name} is {len(rows_in_table)}.")
                for column in table_definition.columns:
                    print(f"Column {column.name} has type={column.type} and nullability={column.nullability}")
                print("")

        print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")


if __name__ == '__main__':
    try:
        run_read_data_from_existing_hyper_file()
    except HyperException as ex:
        print(ex)
        exit(1)
