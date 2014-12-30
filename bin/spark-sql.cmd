@echo off

@rem
@rem Licensed to the Apache Software Foundation (ASF) under one or more
@rem contributor license agreements.  See the NOTICE file distributed with
@rem this work for additional information regarding copyright ownership.
@rem The ASF licenses this file to You under the Apache License, Version 2.0
@rem (the "License"); you may not use this file except in compliance with
@rem the License.  You may obtain a copy of the License at
@rem
@rem    http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.
@rem

@rem
@rem Shell script for starting the Spark SQL CLI


set CLASS="org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver"
set CLASS_NOT_FOUND_EXIT_STATUS=1

set FWDIR=%~dp0..\

if "%1"=="--help" (
        goto usage
)    

set SUBMIT_USAGE_FUNCTION=usage

@rem gatherSParkSubmitOpts seperates parameters into SUBMISSION_OPTS and APPLICATION_OPTS
call %FWDIR%bin\utils.cmd gatherSparkSubmitOpts %*

%FWDIR%bin\spark-submit --class %CLASS% %SUBMISSION_OPTS% spark-internal %APPLICATION_OPTS%

set exit_status=%ERRORLEVEL%

if %exit_status% == %CLASS_NOT_FOUND_EXIT_STATUS% (
    echo
    echo Failed to load Hive Thrift server main class %CLASS%.
    echo You need to build Spark with -Phive.
)

exit /B %ERRORLEVEL%

:usage
    echo Usage: .\sbin\start-thriftserver [options] [thrift server options]
    set pattern="usage"
    set pattern=%pattern%"|Spark assembly has been built with Hive"
    set pattern=%pattern%"|NOTE: SPARK_PREPEND_CLASSES is set"
    set pattern=%pattern%"|Spark Command: "
    set pattern=%pattern%"|======="
    set pattern=%pattern%"|--help"

    %FWDIR%bin\spark-submit --help 2>&1 | findstr /v Usage 1>&2
    echo.
    echo Thrift server options:
    %FWDIR%bin\spark-class %CLASS% --help 2>&1 | findstr /v "%pattern%" 1>&2
    goto :eof