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
param(
    [String]
    [Parameter( ParameterSetName='UsernamePassword', Position=0, Mandatory=$true )]
    [Parameter( ParameterSetName='UsernamePasswordBase64', Position=0, Mandatory=$true )]
    $username,
    [String]
    [Parameter( ParameterSetName='UsernamePassword', Position=1, Mandatory=$true )]
    $password,
    [String]
    [Parameter( ParameterSetName='UsernamePasswordBase64', Position=1, Mandatory=$true )]
    $passwordBase64,
    [Parameter( ParameterSetName='CredentialFilePath', Mandatory=$true )]
    $credentialFilePath
    )

function Main( $scriptDir )
{
    Write-Log "Installing Apache Spark spark-1.3.1.2.3.0.0-2032 to $sparkInstallPath"
    $FinalName = "spark-1.3.1.2.3.0.0-2032"
   
    ###
    ### Create the Credential object from the given username and password or the provided credentials file
    ###
    $serviceCredential = Get-HadoopUserCredentials -credentialsHash @{"username" = $username; "password" = $password; `
        "passwordBase64" = $passwordBase64; "credentialFilePath" = $credentialFilePath}
    $username = $serviceCredential.UserName
    Write-Log "Username: $username"
    Write-Log "CredentialFilePath: $credentialFilePath"

	
    $roles = "sparkmaster sparkslave sparkhiveserver2 yarnsparkhiveserver2 sparkhistoryserver"
    Install "Spark" $ENV:HADOOP_NODE_INSTALL_ROOT $serviceCredential $roles
        
	$version = $FinalName.Substring($FinalName.Length - 12,12)
	
	$config = @{"spark.yarn.scheduler.heartbeat.interval-ms" = "5000"
	"spark.yarn.applicationMaster.waitTries" = "10"
	"spark.history.ui.port" = "18080"
	"spark.yarn.preserve.staging.files" = "False"
	"spark.yarn.submit.file.replication" = "3"
	"spark.yarn.historyServer.address" = "${ENV:RESOURCEMANAGER_HOST}:18080"
	"spark.yarn.driver.memoryOverhead" = "384"
	"spark.yarn.queue" = "default"
	"spark.yarn.containerLauncherMaxThreads" = "25"
	"spark.yarn.max_executor.failures" = "3"
	"spark.yarn.services" = "org.apache.spark.deploy.yarn.history.YarnHistoryService"
	"spark.driver.extraJavaOptions" = "-Dhdp.version=$version"
	"spark.history.provider" = "org.apache.spark.deploy.yarn.history.YarnHistoryProvider"
	"spark.yarn.am.extraJavaOptions" = "-Dhdp.version=$version"
	"spark.yarn.executor.memoryOverhead" = "384"
	}
	
    Configure "spark" $ENV:HADOOP_NODE_INSTALL_ROOT $null $config
    Write-Log "Finished installing Apache Spark"
}

try
{
    $scriptDir = Resolve-Path (Split-Path $MyInvocation.MyCommand.Path)
    $utilsModule = Import-Module -Name "$scriptDir\..\resources\Winpkg.Utils.psm1" -ArgumentList ("Spark") -PassThru
    $apiModule = Import-Module -Name "$scriptDir\InstallApi.psm1" -PassThru
    Main $scriptDir
}
catch
{
	Write-Log $_.Exception.Message "Failure" $_
	exit 1
}
finally
{
    if( $apiModule -ne $null )
    {        
        Remove-Module $apiModule
    }

    if( $utilsModule -ne $null )
    {        
        Remove-Module $utilsModule
    }
}
