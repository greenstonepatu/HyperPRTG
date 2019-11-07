$ErrorActionPreference = "Stop"
#server
$server = $args[0]
# credentials
$login = $args[1]
$pass = $args[2]
#paths
$virtualenv = $args[3]
$directory = $args[4]

# connect to PRTG server
Connect-PrtgServer $server (New-Credential $login $pass)

# activate virtual env
Set-Location -Path $virtualenv
.\Scripts\activate.ps1

#set scripts location
Set-Location -Path $directory
New-Item -ItemType directory -Force -Path .\data

#get data from PRTG api
Get-Probe | Export-Csv ".\data\probe.csv" -NoTypeInfo -UseCulture -Verbose
Get-Group | Export-Csv ".\data\group.csv" -NoTypeInfo -UseCulture -Verbose
Get-Device | Export-Csv ".\data\device.csv" -NoTypeInfo -UseCulture -Verbose
Get-Sensor -Tags 'tableau' | Export-Csv ".\data\sensor.csv" -NoTypeInfo -UseCulture -Verbose

#create hyper file with schema
python create_hyper_file_from_csv_prtg_schema.py
python insert_data_into_table_prtg_get_probe.py
python insert_data_into_table_prtg_get_group.py
python insert_data_into_table_prtg_get_device.py
python insert_data_into_table_prtg_get_sensor.py
python insert_data_into_table_prtg_get_reference.py

#disconnect from PRTG server
Disconnect-PrtgServer