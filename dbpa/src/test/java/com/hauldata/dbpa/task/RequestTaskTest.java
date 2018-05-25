/*
 * Copyright (c) 2017, Ronald DeSantis
 *
 *	Licensed under the Apache License, Version 2.0 (the "License");
 *	you may not use this file except in compliance with the License.
 *	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 *	Unless required by applicable law or agreed to in writing, software
 *	distributed under the License is distributed on an "AS IS" BASIS,
 *	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *	See the License for the specific language governing permissions and
 *	limitations under the License.
 */

package com.hauldata.dbpa.task;

import com.hauldata.dbpa.log.Logger.Level;

public class RequestTaskTest extends TaskTest {

	public RequestTaskTest(String name) {
		super(name);
	}

	public void testGet() throws Exception {

		String processId = "GetTest";
		String script =
				"VARIABLES url VARCHAR, nothing VARCHAR END VARIABLES\n" +
				"TASK SET url = 'http://localhost:8080/' END TASK \n" +
				"TASK GetSchedule AFTER \n" +
				"	REQUEST url + 'schedules/{name}' \n" +
				"	GET \n" +
				"	FROM VALUES ('schedule definition', 'now') AS 'what', 'name' \n" +
				"	RESPONSE '\"definition\"' \n" +
				"	KEEP 'what', 'name', 'definition', 'status' \n" +
				"	INTO SQL INSERT INTO test.restarget (stuff, name, propName, status) VALUES (?,?,?,?) \n" +
				"END TASK\n" +
				"TASK GetScheduleNames AFTER \n" +
				"	REQUEST url + 'schedules/-/names' \n" +
				"	GET \n" +
				"	RESPONSE '[name,...]' \n" +
				"	INTO NOTHING \n" +
				"	KEEP 'name', 'status' \n" +
				"	JOIN SQL INSERT INTO test.restarget (stuff, name, status) VALUES ('schedule name list',?,?) \n" +
				"END TASK\n" +
				"TASK GetJobInfo AFTER \n" +
				"	REQUEST url + 'jobs/{name}' \n" +
				"	HEADER nothing NULL 'ignore this' \n" +
				"	GET \n" +
				"	FROM SQL SELECT * FROM test.reqsource END SQL AS 'name', 'random', 'whatever' \n" +
				"	RESPONSE '{ \"scriptName\" : sn, \"propName\" : pn, \"enabled\" : e}' \n" +
				"	KEEP 'random', 'name', 'sn', 'pn', 'e', 'status' \n" +
				"	INTO SQL INSERT INTO test.restarget (stuff, name, scriptName, propName, enabled, status) VALUES (?,?,?,?,?,?) \n" +
				"END TASK\n" +
				"TASK GetJobInfoWithMessage AFTER \n" +
				"	REQUEST url + 'jobs/{name}' \n" +
				"	GET \n" +
				"	FROM SQL SELECT name, stuff AS whatever FROM test.reqsource END SQL \n" +
				"	RESPONSE '{ \"scriptName\" : sn, \"propName\" : pn, \"enabled\" : e}' \n" +
				"	KEEP 'name', 'sn', 'pn', 'e', 'status', 'message' \n" +
				"	INTO SQL INSERT INTO test.restarget (name, scriptName, propName, enabled, status, stuff) VALUES (?,?,?,?,?,?) \n" +
				"END TASK\n" +
				"TASK GetScheduleValidationFromValues AFTER \n" +
				"	REQUEST url + 'schedules/-/validations/{name}' \n" +
				"	GET \n" +
				"	FROM VALUES ('random text', 'invalid') AS 'arbitrary', 'name' \n" +
				"	RESPONSE '{\"validationMessage\":vm}' \n" +
				"	KEEP 'name', 'vm', 'status' \n" +
				"	INTO SQL INSERT INTO test.restarget (name, stuff, status) VALUES (?,?,?) \n" +
				"END TASK\n" +
				"TASK GetLatestJobRuns AFTER \n" +
				"	REQUEST url + 'jobs/-/runs?latest=true' \n" +
				"	GET \n" +
				"	FROM SQL SELECT 'job list' END SQL AS 'first' \n" +
				"	RESPONSE '[{\"runId\":ri, \"jobName\":jn}, ...]' \n" +
				"	INTO NOTHING \n" +
				"	KEEP 'first', 'ri', 'jn', 'status' \n" +
				"	JOIN SQL INSERT INTO test.restarget (stuff, id, name, status) VALUES (?,?,?,?) \n" +
				"END TASK\n" +
				"TASK GetLatestJobRunsWithState AFTER \n" +
				"	REQUEST url + 'jobs/-/runs?latest=true' \n" +
				"	GET \n" +
				"	FROM SQL SELECT 'job list' END SQL AS 'first' \n" +
				"	RESPONSE '[{\"runId\":ri, \"jobName\":jn, \"state\":{\"status\":js,\"message\":m}}, ...]' \n" +
				"	INTO NOTHING \n" +
				"	KEEP 'first', 'ri', 'jn', 'js', 'm', 'status' \n" +
				"	JOIN SQL INSERT INTO test.restarget (stuff, id, name, scriptName, propName, status) VALUES (?,?,?,?,?,?) \n" +
				"END TASK\n" +
				"TASK GetJobDetails AFTER \n" +
				"	REQUEST url + 'jobs/{name}' \n" +
				"	GET \n" +
				"	FROM SQL SELECT name FROM test.reqsource END SQL \n" +
				"	RESPONSE \n" +
				"'	{' + \n" +
				"'		\"scriptName\": sn,' + \n" +
				"'		\"arguments\": [' + \n" +
				"'			{' + \n" +
				"'				\"name\": an,' + \n" +
				"'				\"value\": av' + \n" +
				"'			},' + \n" +
				"'			...' + \n" +
				"'		],' + \n" +
				"'		\"scheduleNames\": [' + \n" +
				"'			scn,' + \n" +
				"'			...' + \n" +
				"'		],' + \n" +
				"'		\"enabled\": e' + \n" +
				"'	}' \n" +
				"	KEEP 'name', 'sn', 'e', 'status', 'message' \n" +
				"	INTO SQL INSERT INTO test.restarget (name, scriptName, enabled, status, stuff) VALUES (?,?,?,?,?) END SQL \n" +
				"	KEEP 'name', 'an', 'av' \n" +
				"	JOIN SQL INSERT INTO test.resjoin (string1, string2, string3) VALUES (?,?,?) END SQL \n" +
				"	KEEP 'name', 'scn' \n" +
				"	JOIN SQL INSERT INTO test.resjoin (string1, string4) VALUES (?,?) END SQL \n" +
				"END TASK\n" +

				"TASK NonUnique AFTER \n" +
				"	REQUEST 'url' GET \n" +
				"	FROM SQL garbage END SQL AS 'fred', 'ethel', 'lucy', 'ricky', 'ethel' \n" +
				"	RESPONSE '{}' \n" +
				"	KEEP 'status' \n" +
				"	INTO SQL ? \n" +
				"END TASK\n" +
				"TASK AFTER NonUnique SUCCEEDS FAIL END TASK \n" +
				"TASK AFTER NonUnique FAILS GO END TASK \n" +
				"TASK WrongTemplate AFTER \n" +
				"	REQUEST url + 'jobs/-/runs?latest=true' \n" +
				"	GET \n" +
				"	FROM SQL SELECT 'job list' END SQL AS 'first' \n" +
				"	RESPONSE '{\"runId\":ri, \"jobName\":jn}}' \n" +
				"	KEEP 'first', 'ri', 'jn', 'status' \n" +
				"	INTO SQL INSERT INTO test.restarget (stuff, id, name, status) VALUES (?,?,?,?) \n" +
				"END TASK\n" +
				"TASK AFTER WrongTemplate SUCCEEDS FAIL END TASK \n" +
				"TASK AFTER WrongTemplate FAILS GO END TASK \n" +
				"";

/*
		String[] resource = RequestTaskTestResource.getResource();

		String sandboxUrl = resource[0];
		String sandboxAuth = resource[1];

		script = script +
				"TASK ArroPay \n" +
				"	AFTER \n" +
				"	--IF nothing IS NOT NULL \n" +
				"	REQUEST '" + sandboxUrl + "' \n" +
				"	HEADER 'Authorization' '" + sandboxAuth + "' \n" +
				"	POST BODY 'cardNumber', 'cardHolderName', 'employeeId', 'governmentId', 'accountNumber', 'routingNumber', 'proxyId', 'marketId', 'status' \n" +
				"	FROM SQL SELECT * FROM test.arropaycardrequest END SQL \n" +
				"	--KEEP 'employeeId', 'marketId' \n" +
				"	RESPONSE 'id', 'lastFour', 'createDate' \n" +
				"	STATUS 'status' \n" +
				"	INTO SQL INSERT INTO test.restarget (stuff, name, scriptName, status) VALUES (?,?,?,?) \n" +
				"END TASK\n" +
				"TASK ArroPayWithMessage \n" +
				"	AFTER \n" +
				"	--IF nothing IS NOT NULL \n" +
				"	REQUEST '" + sandboxUrl + "' \n" +
				"	HEADER 'Authorization' '" + sandboxAuth + "' \n" +
				"	POST BODY 'cardNumber', 'cardHolderName', 'employeeId', 'governmentId', 'accountNumber', 'routingNumber', 'proxyId', 'marketId', 'status' \n" +
				"	FROM SQL SELECT * FROM test.arropaycardrequest END SQL \n" +
				"	KEEP 'marketId', 'employeeId' \n" +
				"	RESPONSE 'createDate' \n" +
				"	STATUS 'status' MESSAGE 'message' \n" +
				"	INTO SQL INSERT INTO test.restarget (stuff, name, scriptName, status, propName) VALUES (?,?,?,?,?) \n" +
				"END TASK\n" +
*/

		Level logLevel = Level.info;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, null, null);
	}

	public void testPut() throws Exception {

		String processId = "PutTest";
		String script =
				"VARIABLES url VARCHAR, true BIT END VARIABLES \n" +
				"TASK SET url = 'http://localhost:8080/', true = 1 END TASK \n" +
				"TASK PutScript AFTER \n" +
				"	REQUEST url + 'scripts/ScriptViaPut' \n" +
				"	PUT '\"' + \n" +
				"'		PARAMETERS msg VARCHAR, num INTEGER END PARAMETERS\r\n' + \n" +
				"'		BEGIN TASK LOG ''Message: '' + msg END TASK\"' \n" +
				"END TASK\n" +
				"TASK PutSchedule AFTER \n" +
				"	REQUEST url + 'schedules/EveryNoon' \n" +
				"	PUT '\"body\"' \n" +
				"	FROM VALUES ('Daily at ''12:00 PM''') AS 'body'\n" +
				"END TASK\n" +
				"TASK PutJob AFTER \n" +
				"	REQUEST url + 'jobs/{jn}' \n" +
				"	PUT \n" +
				"'	{' + \n" +
				"'		\"scriptName\": sn,' + \n" +
				"'		\"arguments\": [' + \n" +
				"'			{' + \n" +
				"'				\"name\": an,' + \n" +
				"'				\"value\": av' + \n" +
				"'			},' + \n" +
				"'			...' + \n" +
				"'		],' + \n" +
				"'		\"scheduleNames\": [' + \n" +
				"'			scn,' + \n" +
				"'			...' + \n" +
				"'		],' + \n" +
				"'		\"enabled\": e' + \n" +
				"'	}' \n" +
				"	FROM VALUES ('JobViaPut', 'ScriptViaPut', true) AS 'jn', 'sn', 'e' \n" +
				"	JOIN VALUES ('JobViaPut', 'message', '\"hello\", \\there'), ('JobViaPut', 'number', 42) AS 'jn', 'an', 'av' \n" +
				"	JOIN SQL SELECT 0,0 FROM test.reqsource WHERE 0=1 END SQL AS 'jn', 'scn' \n" +
				"	RESPONSE '\"id\"' \n" +
				"	KEEP 'jn', 'id', 'status', 'message' \n" +
				"	INTO SQL INSERT INTO test.restarget (name, id, status, stuff) VALUES (?,?,?,?) END SQL \n" +
				"END TASK\n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, null, null);
	}
}
