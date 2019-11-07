# -----------------------------------------------------------------------------
#created by greenstonepatu
#based on examples provided by Tableau Software
# -----------------------------------------------------------------------------
from pathlib import Path
import pandas as pd

from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, \
    escape_name, escape_string_literal, \
    TableName, \
    SchemaName, \
    HyperException

prtg_schema = SchemaName("PRTG")
sensor_table = TableDefinition(
    # if not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
    name=TableName(prtg_schema, "sensor"),
    columns=[
        TableDefinition.Column(name='Probe', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Group', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Favorite', type=SqlType.bool(), nullability=NULLABLE),
        TableDefinition.Column(name='DisplayLastValue', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='LastValue', type=SqlType.double(), nullability=NULLABLE),
        TableDefinition.Column(name='Device', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Downtime()', type=SqlType.double(), nullability=NULLABLE),
        TableDefinition.Column(name='TotalDowntime()', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='DownDuration', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Uptime()', type=SqlType.double(), nullability=NULLABLE),
        TableDefinition.Column(name='TotalUptime()', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='UpDuration', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='TotalMonitortime()', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='DataCollectedSince', type=SqlType.timestamp(), nullability=NULLABLE),
        TableDefinition.Column(name='LastCheck', type=SqlType.timestamp(), nullability=NULLABLE),
        TableDefinition.Column(name='LastUp', type=SqlType.timestamp(), nullability=NULLABLE),
        TableDefinition.Column(name='LastDown', type=SqlType.timestamp(), nullability=NULLABLE),
        TableDefinition.Column(name='MiniGraph', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Schedule', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='BaseType', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Url', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='NotificationTypes', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Interval', type=SqlType.time(), nullability=NULLABLE),
        TableDefinition.Column(name='InheritInterval', type=SqlType.bool(), nullability=NULLABLE),
        TableDefinition.Column(name='Access', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Dependency', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Position', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='Status', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Comments', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Priority', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Message', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Id', type=SqlType.int(), nullability=NULLABLE),
        TableDefinition.Column(name='ParentId', type=SqlType.int(), nullability=NULLABLE),
        TableDefinition.Column(name='Name', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Tags', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='DisplayType', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Type', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Active', type=SqlType.bool(), nullability=NULLABLE),
        TableDefinition.Column(name='DateTime', type=SqlType.timestamp(), nullability=NULLABLE)
    ]
)

device_table = TableDefinition(
    # if table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
    name=TableName(prtg_schema, "device"),
    columns=[
        TableDefinition.Column(name='Location', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Host', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Group', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Probe', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Favorite', type=SqlType.bool(), nullability=NULLABLE),
        TableDefinition.Column(name='Condition', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='UpSensors', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='DownSensors', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='DownAcknowledgedSensors', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='PartialDownSensors', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='WarningSensors', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='PausedSensors', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='UnusualSensors', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='UnknownSensors', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='TotalSensors', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='Schedule', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='BaseType', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Url', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='NotificationTypes', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Interval', type=SqlType.time(), nullability=NULLABLE),
        TableDefinition.Column(name='InheritInterval', type=SqlType.bool(), nullability=NULLABLE),
        TableDefinition.Column(name='Access', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Dependency', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Position', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='Status', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Comments', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='Priority', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Message', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Id', type=SqlType.int(), nullability=NULLABLE),
        TableDefinition.Column(name='ParentId', type=SqlType.int(), nullability=NULLABLE),
        TableDefinition.Column(name='Name', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Tags', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='DisplayType', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Type', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Active', type=SqlType.bool(), nullability=NULLABLE)
    ]
)

group_table = TableDefinition(
    #if table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
    name=TableName(prtg_schema, "group"),
    columns=[
        TableDefinition.Column(name='Probe', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Condition', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Collapsed', type=SqlType.bool(), nullability=NULLABLE),
        TableDefinition.Column(name='TotalGroups', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='TotalDevices', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='UpSensors', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='DownSensors', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='DownAcknowledgedSensors', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='PartialDownSensors', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='WarningSensors', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='PausedSensors', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='UnusualSensors', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='UnknownSensors', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='TotalSensors', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='Schedule', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='BaseType', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Url', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='NotificationTypes', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Interval', type=SqlType.time(), nullability=NULLABLE),
        TableDefinition.Column(name='InheritInterval', type=SqlType.bool(), nullability=NULLABLE),
        TableDefinition.Column(name='Access', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Dependency', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Position', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='Status', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Comments', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Priority', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Message', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Id', type=SqlType.int(), nullability=NULLABLE),
        TableDefinition.Column(name='ParentId', type=SqlType.int(), nullability=NULLABLE),
        TableDefinition.Column(name='Name', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Tags', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='DisplayType', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Type', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Active', type=SqlType.bool(), nullability=NULLABLE)
    ]
)

probe_table = TableDefinition(
    # if not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
    name=TableName(prtg_schema, "probe"),
    columns=[
        TableDefinition.Column(name='ProbeStatus', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Collapsed', type=SqlType.bool(), nullability=NULLABLE),
        TableDefinition.Column(name='TotalGroups', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='TotalDevices', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='UpSensors', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='DownSensors', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='DownAcknowledgedSensors', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='PartialDownSensors', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='WarningSensors', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='PausedSensors', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='UnusualSensors', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='UnknownSensors', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='TotalSensors', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='Schedule', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='BaseType', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Url', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='NotificationTypes', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Interval', type=SqlType.time(), nullability=NULLABLE),
        TableDefinition.Column(name='InheritInterval', type=SqlType.bool(), nullability=NULLABLE),
        TableDefinition.Column(name='Access', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Dependency', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Position', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='Status', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Comments', type=SqlType.big_int(), nullability=NULLABLE),
        TableDefinition.Column(name='Priority', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Message', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Id', type=SqlType.int(), nullability=NULLABLE),
        TableDefinition.Column(name='ParentId', type=SqlType.int(), nullability=NULLABLE),
        TableDefinition.Column(name='Name', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Tags', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='DisplayType', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Type', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Active', type=SqlType.bool(), nullability=NULLABLE)
    ]
)

history_table = TableDefinition(
    # if not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
    name=TableName(prtg_schema, "history"),
    columns=[
        TableDefinition.Column(name='DateTime', type=SqlType.timestamp(), nullability=NULLABLE),
        TableDefinition.Column(name='SensorId', type=SqlType.int(), nullability=NULLABLE),
        TableDefinition.Column(name='PingTime', type=SqlType.double(), nullability=NULLABLE),
        TableDefinition.Column(name='Minimum', type=SqlType.double(), nullability=NULLABLE),
        TableDefinition.Column(name='Maximum', type=SqlType.double(), nullability=NULLABLE),
        TableDefinition.Column(name='PacketLoss', type=SqlType.double(), nullability=NULLABLE),
        TableDefinition.Column(name='Coverage', type=SqlType.double(), nullability=NULLABLE)
    ]
)


log_table = TableDefinition(
    # if not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
    name=TableName(prtg_schema, "log"),
    columns=[
        TableDefinition.Column(name='Id', type=SqlType.int(), nullability=NULLABLE),
        TableDefinition.Column(name='Name', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='DateTime', type=SqlType.timestamp(), nullability=NULLABLE),
        TableDefinition.Column(name='Parent', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Status', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Sensor', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Device', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Group', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Probe', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Message', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Priority', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='DisplayType', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Type', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Tags', type=SqlType.text(), nullability=NULLABLE),
        TableDefinition.Column(name='Active', type=SqlType.bool(), nullability=NULLABLE),
        TableDefinition.Column(name='ObjectId', type=SqlType.int(), nullability=NULLABLE)
    ]
)


temp_table = TableDefinition(
    # if not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
    name=TableName(prtg_schema, "temp")
)



def run_create_hyper_file_from_csv():
    """
    loading data from a csv into a new Hyper file
    """
    print("Load data from CSV into table in new Hyper file")

    path_to_database = Path(__file__).parent / "data" / "PRTG.hyper"

    # Starts the Hyper Process with telemetry enabled to send data to Tableau.
    # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:

        # Creates new Hyper file "PRTG_GetSensor.hyper".
        # Replaces file with CreateMode.CREATE_AND_REPLACE if it already exists.
        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database,
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
            connection.catalog.create_schema(schema=prtg_schema)
            connection.catalog.create_table(table_definition=sensor_table)
            connection.catalog.create_table(table_definition=device_table)
            connection.catalog.create_table(table_definition=group_table)
            connection.catalog.create_table(table_definition=probe_table)
            connection.catalog.create_table(table_definition=history_table)
            connection.catalog.create_table(table_definition=log_table)


            # # Using path to current file, create a path that locates CSV file packaged with these examples.
            # path_to_device_csv = str(Path(__file__).parent / "data" / "device.csv")
            # path_to_sensor_csv = str(Path(__file__).parent / "data" / "sensor.csv")
            # path_to_group_csv = str(Path(__file__).parent / "data" / "group.csv")
            # path_to_probe_csv = str(Path(__file__).parent / "data" / "probe.csv")
            # path_to_history_csv = str(Path(__file__).parent / "data" / "history.csv")
            # path_to_log_csv = str(Path(__file__).parent / "data" / "log.csv")
            #
            # #format dates in the history.csv
            # df = pd.read_csv(path_to_history_csv, delimiter=';')
            # df['DateTime'] = pd.to_datetime(df['DateTime'], dayfirst=False)
            # df.to_csv(path_to_history_csv, index=False, quotechar='\"', escapechar='\\', sep=';', )
            #
            # #format dates in the sensor.csv
            # df = pd.read_csv(path_to_sensor_csv, delimiter=';')
            # df['DataCollectedSince'] = pd.to_datetime(df['DataCollectedSince'], dayfirst=False)
            # df['LastCheck'] = pd.to_datetime(df['LastCheck'], dayfirst=False)
            # df['LastDown'] = pd.to_datetime(df['LastDown'], dayfirst=False)
            # df['LastUp'] = pd.to_datetime(df['LastUp'], dayfirst=False)
            # df['DateTime'] = pd.Timestamp.now()
            # df.to_csv(path_to_sensor_csv, index=False, quotechar='\"', escapechar='\\', sep=';', )
            #
            # #format dates in the log.csv
            # df = pd.read_csv(path_to_log_csv, delimiter=';')
            # df['DateTime'] = pd.to_datetime(df['DateTime'], dayfirst=False)
            # df.to_csv(path_to_log_csv, index=False, quotechar='\"', escapechar='\\', sep=';', )


            # # Load all rows into tables from the CSV file.
            # # `execute_command` executes a SQL statement and returns the impacted row count.
            # count_in_device_table = connection.execute_command(
            #     command=f"COPY {probe_table.table_name} from {escape_string_literal(path_to_device_csv)} with "
            #     f"(format csv, NULL '', delimiter ';', header, quote '\"', escape '\\')")
            #
            # print(f"The number of rows in table {probe_table.table_name} is {count_in_device_table}.")
            #
            # count_in_sensor_table = connection.execute_command(
            #     command=f"COPY {probe_table.table_name} from {escape_string_literal(path_to_sensor_csv)} with "
            #     f"(format csv, NULL '', delimiter ';', header, quote '\"', escape '\\')")
            #
            # print(f"The number of rows in table {probe_table.table_name} is {count_in_sensor_table}.")
            #
            # count_in_group_table = connection.execute_command(
            #     command=f"COPY {probe_table.table_name} from {escape_string_literal(path_to_group_csv)} with "
            #     f"(format csv, NULL '', delimiter ';', header, quote '\"', escape '\\')")
            #
            # print(f"The number of rows in table {probe_table.table_name} is {count_in_group_table}.")
            #
            # count_in_probe_table = connection.execute_command(
            #     command=f"COPY {probe_table.table_name} from {escape_string_literal(path_to_probe_csv)} with "
            #     f"(format csv, NULL '', delimiter ';', header, quote '\"', escape '\\')")
            #
            # print(f"The number of rows in table {probe_table.table_name} is {count_in_probe_table}.")
            #
            # count_in_history_table = connection.execute_command(
            #     command=f"COPY {history_table.table_name} from {escape_string_literal(path_to_history_csv)}  with "
            #     f"(format csv, NULL '', delimiter ';', header, quote '\"', escape '\\')")
            #
            # print(f"The number of rows in table {history_table.table_name} is {count_in_history_table}.")
            #
            # count_in_log_table = connection.execute_command(
            #     command=f"COPY {log_table.table_name} from {escape_string_literal(path_to_log_csv)} with "
            #     f"(format csv, NULL '', delimiter ';', header, quote '\"', escape '\\')")
            #
            # print(f"The number of rows in table {log_table.table_name} is {count_in_log_table}.")
            #
            #
            # # connection.execute_command(command=f" INSERT INTO {reference_table.table_name} SELECT {probe_table.table_name}.\"Id\" FROM {probe_table.table_name}")
            connection.execute_command(
                command=f"CREATE table {temp_table.table_name}  AS "
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
            # row_count = connection.execute_scalar_query(query=f"SELECT COUNT(*) FROM {temp_table.table_name}")
            # The table names in the "PRTG" schema.
            table_names = connection.catalog.get_table_names("PRTG")
            print(f"Tables available in {path_to_database} are: {table_names}")
    print("The Hyper process has been shut down.")


if __name__ == '__main__':
    try:
        run_create_hyper_file_from_csv()
    except HyperException as ex:
        print(ex)
        exit(1)
