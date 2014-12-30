### Licensed to the Apache Software Foundation (ASF) under one or more
### contributor license agreements.  See the NOTICE file distributed with
### this work for additional information regarding copyright ownership.
### The ASF licenses this file to You under the Apache License, Version 2.0
### (the "License"); you may not use this file except in compliance with
### the License.  You may obtain a copy of the License at
###
###     http://www.apache.org/licenses/LICENSE-2.0
###
### Unless required by applicable law or agreed to in writing, software
### distributed under the License is distributed on an "AS IS" BASIS,
### WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
### See the License for the specific language governing permissions and
### limitations under the License.

###
### Global test variables
###

$ScriptDir = Resolve-Path (Split-Path $MyInvocation.MyCommand.Path)

### Templates
$Username = "sparkadmin"
$Password = "HDInsight123!"
$ENV:HADOOP_NODE_INSTALL_ROOT = "C:\apps\dist"
$ENV:WINPKG_LOG = "winpkg_core_install.log"
$SparkVersion = "1.2.0"

###
### Uncomment and update below section for testing from sources
###
#$Username = "hadoop"
#$Password = "TestUser123"
#$ENV:HADOOP_NODE_INSTALL_ROOT = "C:\Hadoop\test"
#$ENV:WINPKG_LOG = "winpkg_core_install.log"

#$HadoopCoreVersion = "2.1.2-SNAPSHOT"
###
### End of testing section
###

$NodeInstallRoot = "$ENV:HADOOP_NODE_INSTALL_ROOT"
$SecurePassword = ConvertTo-SecureString $Password -AsPlainText -Force
$ServiceCredential = New-Object System.Management.Automation.PSCredential ("$ENV:COMPUTERNAME\$Username", $SecurePassword)
if ($ServiceCredential -eq $null)
{
    throw "Failed to create PSCredential object, please check your username/password parameters"
}

function Assert(
    [String]
    [parameter( Position=0 )]
    $message,
    [bool]
    [parameter( Position=1 )]
    $condition = $false
    )
{
    if ( -not $condition )
    {
        throw $message
    }
}


function SparkInstallTestBasic()
{
    Install "spark" $NodeInstallRoot $ServiceCredential ""
    Assert "ENV:SPARK_HOME must be set" (Test-Path ENV:SPARK_HOME)
    Assert "ENV:SPARK_HOME folder must exist" (Test-Path $ENV:SPARK_HOME)
    Uninstall "spark" $NodeInstallRoot
}

function SparkMasterInstallTestBasic()
{
    Install "spark" $NodeInstallRoot $ServiceCredential "sparkmaster"
    StartService "spark" "sparkmaster"
    Assert "ENV:SPARK_HOME must be set" (Test-Path ENV:SPARK_HOME)
    Assert "ENV:SPARK_HOME folder must exist" (Test-Path $ENV:SPARK_HOME)
    Uninstall "spark" $NodeInstallRoot
}

function SparkSlaveInstallTestBasic()
{
    Install "spark" $NodeInstallRoot $ServiceCredential "sparkslave"
    Assert "ENV:SPARK_HOME must be set" (Test-Path ENV:SPARK_HOME)
    Assert "ENV:SPARK_HOME folder must exist" (Test-Path $ENV:SPARK_HOME)
    Uninstall "spark" $NodeInstallRoot
}

function SparkHiveServer2InstallTestBasic()
{
    Install "spark" $NodeInstallRoot $ServiceCredential "sparkhiveserver2"
    Assert "ENV:SPARK_HOME must be set" (Test-Path ENV:SPARK_HOME)
    Assert "ENV:SPARK_HOME folder must exist" (Test-Path $ENV:SPARK_HOME)
    Uninstall "spark" $NodeInstallRoot
}

function SparkConfigureTestBasic()
{
    Install "spark" $NodeInstallRoot $ServiceCredential ""
    Configure "spark" $NodeInstallRoot $ServiceCredential
	
    $sparkDefaultConf = Join-Path $NodeInstallRoot "spark-$sparkVersion\conf\spark-defaults.conf"	
    ValidateSpaceDelimitedConfigVaue $sparkDefaultConf "spark.eventLog.enabled" "true"

    Uninstall "spark" $NodeInstallRoot
}

function SparkConfigureTestWithCustomConfig()
{
    Install "spark" $NodeInstallRoot $ServiceCredential ""
    ### Update to core configuration
    Configure "spark" $NodeInstallRoot $ServiceCredential @{"spark.master" = "spark://namenodehost:7077" }

    ### Verify the update took place
    $sparkDefaultConf = Join-Path $NodeInstallRoot "spark-$sparkVersion\conf\spark-defaults.conf"	
    ValidateSpaceDelimitedConfigVaue $sparkDefaultConf "spark.master" "spark://namenodehost:7077"

    Uninstall "spark" $NodeInstallRoot
}

function ValidateSpaceDelimitedConfigVaue($confFileName, $key, $expectedValue){
	$fileContent = Get-content $confFileName
    $expectedSettingValue = "$key $expectedValue`r`n"
    if($($fileContent -match $expectedSettingValue) -ne $null) {
        throw "$confFileName does not have ""$expectedSettingValue"""
    }
}

try
{
    ###
    ### Import dependencies
    ###
    $utilsModule = Import-Module -Name "$ScriptDir\..\resources\Winpkg.Utils.psm1" -ArgumentList ("Spark") -PassThru
    $apiModule = Import-Module -Name "$ScriptDir\InstallApi.psm1" -PassThru

    ###
    ### Test methods
    ###
    sparksubmitpitest
    sparkinstalltestbasic
    sparkmasterinstalltestbasic
    sparkslaveinstalltestbasic
    sparkconfiguretestbasic
    sparkconfiguretestwithcustomconfig

    Write-Host "TEST COMPLETED SUCCESSFULLY"
}
finally
{
	if( $utilsModule -ne $null )
	{
		Remove-Module $apiModule
        Remove-Module $utilsModule
	}
}
