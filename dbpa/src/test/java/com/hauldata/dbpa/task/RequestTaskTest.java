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

import com.hauldata.dbpa.log.Analyzer;
import com.hauldata.dbpa.log.Logger.Level;

public class RequestTaskTest extends TaskTest {

	private final String url = "http://localhost:8090/api";

	public RequestTaskTest(String name) {
		super(name);
	}

	public void testGet() throws Exception {

		String processId = "GetTest";
		String script =
				"VARIABLES url VARCHAR, nothing VARCHAR END VARIABLES\n" +
				"TASK SET url = '" + url + "/' END TASK \n" +
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
				"	CONNECT TIMEOUT 5 SOCKET TIMEOUT 60 \n" +
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
				"TASK GetJobInfoWithMessage AFTER COMPLETES \n" +
				"	REQUEST url + 'jobs/{name}' \n" +
				"	GET \n" +
				"	FROM SQL SELECT name, stuff AS whatever FROM test.reqsource END SQL \n" +
				"	RESPONSE '{ \"scriptName\" : sn, \"propName\" : pn, \"enabled\" : e}' \n" +
				"	KEEP 'name', 'sn', 'pn', 'e', 'status', 'message' \n" +
				"	INTO SQL INSERT INTO test.restarget (name, scriptName, propName, enabled, status, stuff) VALUES (?,?,?,?,?,?) \n" +
				"END TASK\n" +
				"TASK GetScheduleValidationFromValues AFTER COMPLETES \n" +
				"	REQUEST url + 'schedules/-/validations/{name}' \n" +
				"	SOCKET TIMEOUT 10 \n" +
				"	GET \n" +
				"	FROM VALUES ('random text', 'lastMonday') AS 'arbitrary', 'name' \n" +
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

				"TASK NonUnique AFTER COMPLETES \n" +
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
				"TASK SET url = '" + url + "/', true = 1 END TASK \n" +
				"TASK PutScript AFTER \n" +
				"	REQUEST url + 'scripts/ScriptViaPut' \n" +
				"	PUT '\"' + \n" +
				"'		PARAMETERS msg VARCHAR, num INTEGER END PARAMETERS\r\n' + \n" +
				"'		BEGIN TASK LOG ''Message: '' + msg END TASK\"' \n" +
				"END TASK\n" +
				"TASK PutNestedScript AFTER \n" +
				"	REQUEST url + 'scripts/{name}' \n" +
				"	PUT '\"body\"' \n" +
				"	FROM VALUES \n" +
				"	('nested/thing', 'BEGIN TASK LOG ''nested thing'' END TASK') \n" +
				"	AS 'name', 'body' \n" +
				"END TASK\n" +
				"TASK PutSchedule AFTER \n" +
				"	REQUEST url + 'schedules/EveryNoon' \n" +
				"	PUT '\"body\"' \n" +
				"	FROM VALUES ('Daily at ''12:00 PM''') AS 'body'\n" +
				"END TASK\n" +
				"TASK PutScheduleList AFTER \n" +
				"	REQUEST url + 'schedules/{name}' \n" +
				"	PUT '\"body\"' \n" +
				"	FROM VALUES \n" +
				"	('MondayMorning', 'Every Monday at ''8:00 AM'''), \n" +
				"	('TuesdayAfternoon', 'Every Tuesday at ''2:00 PM'''), \n" +
				"	('WednesdayEvening', 'Every Wednesday at ''8:00 PM''') AS 'name', 'body'\n" +
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

	public void testPost() throws Exception {

		String processId = "PostTest";
		String script =
				"VARIABLES url VARCHAR END VARIABLES \n" +
				"TASK SET url = '" + url + "/' END TASK \n" +
				"TASK StartJob AFTER \n" +
				"	REQUEST url + 'jobs/-/running/sleep5' \n" +
				"	POST '[]' \n" +
				"	RESPONSE '\"runId\"' \n" +
				"	KEEP 'runId', 'message' \n" +
				"	INTO SQL INSERT INTO test.restarget (name, stuff, id, propName) VALUES ('sleep5','job run',?,?) \n" +
				"END TASK\n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		runScript(processId, logLevel, logToConsole, script, null, null, null);
	}


	public void testBadTemplate() throws Exception {
		assertBadTemplate("TASK REQUEST 'url' PUT '\"a\" 77' END TASK", "Request template error: Unexpected token: 77");
		assertBadTemplate("TASK REQUEST 'url' PUT 'x' END TASK", "Request template error: not a valid JSON template");
		assertBadTemplate("TASK REQUEST 'url' PUT '[{c:d,...},...]' FROM VALUES (1,2) AS 'a','b' JOIN VALUES (3,4,5) AS 'a','c','d' END TASK", "Request template error: Dynamic structures cannot be nested");
		assertBadTemplate("TASK REQUEST 'url' PUT '{c:[d,...],...}' FROM VALUES (1,2) AS 'a','b' JOIN VALUES (3,4,5) AS 'a','c','d' END TASK", "Request template error: Dynamic structures cannot be nested");
		assertBadTemplate("TASK REQUEST 'url' PUT '{\"a\":[c,d],...}' FROM VALUES (1,2) AS 'a','b' JOIN VALUES (3,4,5) AS 'a','c','d' END TASK", "Request template error: A dynamic object must start with a column name from a JOIN clause");
		assertBadTemplate("TASK REQUEST 'url' PUT '{c:[d,1]}' FROM VALUES (1,2) AS 'a','b' JOIN VALUES (3,4,5) AS 'a','c','d' END TASK", "Request template error: An object that starts with a column name from a JOIN clause must be a dynamic object");
		assertBadTemplate("TASK REQUEST 'url' PUT '{\"a\":a,b:c}' FROM VALUES (1,2,3) AS 'a','b','c' END TASK", "Request template error: An unquoted field name can only be used in the first field of a dynamic object: b");
		assertBadTemplate("TASK REQUEST 'url' PUT '{\"a\":a,47:c}' FROM VALUES (1,2,3) AS 'a','b','c' END TASK", "Request template error: Unexpected token where a field name is expected: 47");
		assertBadTemplate("TASK REQUEST 'url' PUT '{fred:c}' FROM VALUES (1,2,3) AS 'a','b','c' END TASK", "Request template error: An unquoted field name must be a column name: fred");
		assertBadTemplate("TASK REQUEST 'url' PUT '{\"a\":a,\"x\":[d,...],\"y\":[d,...]}' FROM VALUES (1,2) AS 'a','b' JOIN VALUES (1,4) AS 'a','d' JOIN VALUES (1,6) AS 'a','f' END TASK", "Request template error: Two dynamic structures cannot reference columns from the same JOIN");
		assertBadTemplate("TASK REQUEST 'url' PUT '[a,...]' FROM VALUES (1) AS 'a' END TASK", "Request template error: A dynamic structure must not reference a FROM column");
		assertBadTemplate("TASK REQUEST 'url' PUT '{\"a\":a,\"x\":{d:f,...},\"y\":{d:f,...}}' FROM VALUES (1,2) AS 'a','b' JOIN VALUES (1,4) AS 'a','d' JOIN VALUES (1,6) AS 'a','f' END TASK", "Request template error: A dynamic structure cannot reference more than one JOIN column");
		assertBadTemplate("TASK REQUEST 'url' PUT '\"b\"' FROM VALUES (1) AS 'a' END TASK", "Request template error: Column name not found: b");
		assertBadTemplate("TASK REQUEST 'url' PUT '{\"a\":a,\"c\":[c,...]}' FROM VALUES (1,2) AS 'a','b' JOIN VALUES (3,4) AS 'a','c' JOIN VALUES (5,6) AS 'a','f' END TASK", "There must be exactly one dynamic structure in the JSON request for each JOIN");

		assertBadTemplate("TASK REQUEST 'url' PUT 'garbage' FROM VALUES (1,2) AS 'a','b' JOIN VALUES (3,4) AS 'c','d' JOIN VALUES (5,6) AS 'e','f' END TASK", "The column names in a JOIN clause must include at least one column name in the FROM clause");
	}

	private void assertBadTemplate(String script, String expectedMessage) throws Exception {

		String processId = "BadTemplateTest";

		Level logLevel = Level.error;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null, false);
		Analyzer.RecordIterator recordIterator = analyzer.recordIterator();

		Analyzer.Record record;

		record = recordIterator.next();
		assertEquals(expectedMessage, record.message);
	}
}
