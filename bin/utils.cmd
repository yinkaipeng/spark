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

@rem Gather all spark-submit options into SUBMISSION_OPTS
@rem Usage: Utils.cmd <label name> <options>
@rem batch doesn't have notion of calling a function defined at another cmd file, taking in function name at first parameter
setlocal enableDelayedExpansion
set labelName=%1
shift

goto %labelName%
goto eof

:gatherSparkSubmitOpts
	if not defined SUBMIT_USAGE_FUNCTION (
	    echo "Function for printing usage of %labelName% is not set." 
	    echo "Please set usage function to shell variable 'SUBMIT_USAGE_FUNCTION' in %labelName%" 1>&2
        exit /B 1
	)

    @rem NOTE: If you add or remove spark-sumbmit options,
    @rem modify NOT ONLY this script but also SparkSubmitArgument.scala
	set SUBMISSION_OPTS=
	set APPLICATION_OPTS=

	:start_OptsLoop
	if [%~1] == [] ( goto end_OptsLoop )
		
		@rem switch equivalent since cmd doesn't support switch syntax
		@rem parameters where next 2 are set as SUBMISSION_OPTS
		if [%~1]==[--master] ( goto :SET_NEXT_2_SUBMISSION_OPTS )
		if [%~1]==[--deploy-mode] ( goto :SET_NEXT_2_SUBMISSION_OPTS )
		if [%~1]==[--class] ( goto :SET_NEXT_2_SUBMISSION_OPTS )
		if [%~1]==[--name] ( goto :SET_NEXT_2_SUBMISSION_OPTS )
		if [%~1]==[--jars] ( goto :SET_NEXT_2_SUBMISSION_OPTS )
		if [%~1]==[--py-files] ( goto :SET_NEXT_2_SUBMISSION_OPTS )
		if [%~1]==[--files] ( goto :SET_NEXT_2_SUBMISSION_OPTS )
		if [%~1]==[--conf] ( goto :SET_NEXT_2_SUBMISSION_OPTS )
		if [%~1]==[--properties-file] ( goto :SET_NEXT_2_SUBMISSION_OPTS )
		if [%~1]==[--driver-memory] ( goto :SET_NEXT_2_SUBMISSION_OPTS )
		if [%~1]==[--driver-java-options] ( goto :SET_NEXT_2_SUBMISSION_OPTS )
		if [%~1]==[--driver-library-path] ( goto :SET_NEXT_2_SUBMISSION_OPTS )
		if [%~1]==[--driver-class-path] ( goto :SET_NEXT_2_SUBMISSION_OPTS )
		if [%~1]==[--executor-memory] ( goto :SET_NEXT_2_SUBMISSION_OPTS )
		if [%~1]==[--driver-cores] ( goto :SET_NEXT_2_SUBMISSION_OPTS )
		if [%~1]==[--total-executor-cores] ( goto :SET_NEXT_2_SUBMISSION_OPTS )
		if [%~1]==[--executor-cores] ( goto :SET_NEXT_2_SUBMISSION_OPTS )
		if [%~1]==[--queue] ( goto :SET_NEXT_2_SUBMISSION_OPTS )
		if [%~1]==[--num-executors] ( goto :SET_NEXT_2_SUBMISSION_OPTS )
		if [%~1]==[--archives] ( goto :SET_NEXT_2_SUBMISSION_OPTS )
		
		@rem parameters where next 1 option is set as SUBMISSION_OPTS		
		if [%~1]==[--verbose] ( goto :SET_NEXT_1_SUBMISSION_OPT )
		if [%~1]==[-v] ( goto :SET_NEXT_1_SUBMISSION_OPT )
		if [%~1]==[--supervise] ( goto :SET_NEXT_1_SUBMISSION_OPT )
		
		@rem default option
		goto SET_NEXT_1_APPLICATION_OPTS
		
		:SET_NEXT_2_SUBMISSION_OPTS
		    set SUBMISSION_OPTS=%SUBMISSION_OPTS% %~1
			shift
			if [%~1] == [] ( goto usage )
		    set SUBMISSION_OPTS=%SUBMISSION_OPTS% %~1
			shift
			:start_OptsLoop
			
		:SET_NEXT_1_SUBMISSION_OPT	
			set SUBMISSION_OPTS=%SUBMISSION_OPTS% %~1
			shift
			:start_OptsLoop
		
		:SET_NEXT_1_APPLICATION_OPTS
			set APPLICATION_OPTS=%APPLICATION_OPTS% %~1
			shift
			:start_OptsLoop
		
		goto start_OptsLoop
	:end_OptsLoop
	exit /B 0
