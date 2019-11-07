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

#get log data from PRTG api daily

Get-Sensor -Tags 'tableau' -Verbose | Get-ObjectLog -StartDate (get-date).Date -EndDate (get-date).date.AddDays(-1) -Verbose | Export-Csv ".\data\log.csv" -NoTypeInfo -UseCulture -Verbose
python insert_data_into_table_prtg_get_log.py

#disconnect from PRTG server
Disconnect-PrtgServer
