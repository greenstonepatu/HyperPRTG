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
device_table = TableDefinition(
    name=TableName("PRTG", "device"),
)
temp_table = TableDefinition(
    name=TableName("PRTG", "temp"),
)


def run_insert_data_into_single_table():
    path_to_database = str(Path(__file__).parent / "data" / "PRTG.hyper")

    # Starts the Hyper Process with telemetry enabled to send data to Tableau.
    # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:

        # Opens Hyper file "PRTG.hyper".
        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database) as connection:

            # Load all rows into "temp" table from tables.
            # `execute_command` executes a SQL statement and returns the impacted row count.

            #truncate table "temp" before population
            connection.execute_command(command=f"TRUNCATE table {temp_table.table_name}")
            #populate table "temp"
            connection.execute_command(
                command=f"INSERT INTO {temp_table.table_name}  SELECT * FROM "
                        f"(with recursive cte (\"Id\", \"Name\", \"ParentId\") as "
                        f"(select \"Id\", \"Name\", \"ParentId\" "
                        f"from {group_table.table_name} "
                        f"union all "
                        f"select p.\"Id\", p.\"Name\", p.\"ParentId\" "
                        f"from {group_table.table_name} p "
                        f"inner join cte "
                        f"on p.\"ParentId\" = cte.\"Id\" ) "
                        f"select distinct "
                        f"a.\"Name\","
                        f" a.\"ParentId\", "
                        f"a.\"Id\", "
                        f"b.\"Name\" as Name1, "
                        f"b.\"ParentId\" as ParentId1, "
                        f"b.\"Id\" as Id1, "
                        f"c.\"Name\" as Name2, "
                        f"c.\"ParentId\" as ParentId2, "
                        f"c.\"Id\" as Id2, "
                        f"d.\"Name\" as Name3, "
                        f"d.\"ParentId\" as ParentId3, "
                        f"e.\"Id\" as Id3, "
                        f"f.\"Name\" as Name4, "
                        f"f.\"ParentId\" as ParentId4, "
                        f"f.\"Id\" as Id4, "
                        f"x.\"ParentId\" as DeviceParentId, "
                        f"x.\"Id\" as DeviceId "
                        f"from cte a left join cte b on b.\"ParentId\" = a.\"Id\" "
                        f"left join cte c on c.\"ParentId\" = b.\"Id\" "
                        f"left join cte d on d.\"ParentId\" = c.\"Id\" "
                        f"left join cte e on e.\"ParentId\" = d.\"Id\" "
                        f"left join cte f on f.\"ParentId\" = e.\"Id\" "
                        f"left join {device_table.table_name} x on x.\"ParentId\" = f.\"Id\" "
                        f"OR x.\"ParentId\" = e.\"Id\" "
                        f"OR x.\"ParentId\" = d.\"Id\" "
                        f"OR x.\"ParentId\" = c.\"Id\" "
                        f"OR x.\"ParentId\" = b.\"Id\" "
                        f"OR x.\"ParentId\" = a.\"Id\" "
                        f")")

            # # Number of rows in the "PRTG"."temp" table.
            # # `execute_scalar_query` is for executing a query that returns exactly one row with one column.
            row_count = connection.execute_scalar_query(query=f"SELECT COUNT(*) FROM {temp_table.table_name}")
            print(f"The number of rows in table {temp_table.table_name} is {row_count}.")

        print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")


if __name__ == '__main__':
    try:
        run_insert_data_into_single_table()
    except HyperException as ex:
        print(ex)
        exit(1)
