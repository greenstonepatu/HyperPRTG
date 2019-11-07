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

#get sensor data from PRTG api every 2 minutes

while($true)
{
    Get-Sensor -Tags 'tableau' | Export-Csv ".\data\sensor.csv" -NoTypeInfo -UseCulture -Verbose
    python insert_data_into_table_prtg_get_sensor.py
    Start-Sleep -s 120
}
#disconnect from PRTG server
Disconnect-PrtgServer