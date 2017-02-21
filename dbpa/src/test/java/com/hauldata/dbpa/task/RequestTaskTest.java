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

		String sandboxUrl = "URL HERE";
		String sandboxAuth = "AUTH HERE";

		String processId = "GetTest";
		String script =
				"VARIABLES nothing VARCHAR END VARIABLES\n" +
				"TASK GetJobInfo \n" +
				"	REQUEST 'http://localhost:8080/jobs/{name}' " +
				"	HEADER nothing NULL 'ignore this' \n" +
				"	GET 'name', 'random', 'whatever' \n" +
				"	FROM SQL SELECT * FROM test.reqsource END SQL \n" +
				"	KEEP 'random', 'name' \n" +
				"	RESPONSE 'scriptName', 'propName', 'enabled' \n" +
				"	STATUS 'status' \n" +
				"	INTO SQL INSERT INTO test.restarget VALUES (?,?,?,?,?,?) \n" +
				"END TASK\n" +
				"TASK GetJobInfoWithMessage \n" +
				"	AFTER \n" +
				"	REQUEST 'http://localhost:8080/jobs/{name}' " +
				"	GET 'name', 'whatever' \n" +
				"	FROM SQL SELECT name, stuff FROM test.reqsource END SQL \n" +
				"	KEEP 'name' \n" +
				"	RESPONSE 'scriptName', 'propName', 'enabled' \n" +
				"	STATUS 'status' MESSAGE 'message' \n" +
				"	INTO SQL INSERT INTO test.restarget (name, scriptName, propName, enabled, status, stuff) VALUES (?,?,?,?,?,?) \n" +
				"END TASK\n" +
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
				"TASK NonUnique \n" +
				"	AFTER \n" +
				"	REQUEST 'url' GET 'fred', 'ethel', 'lucy', 'ricky', 'ethel' \n" +
				"	FROM SQL garbage END SQL \n" +
				"	KEEP NONE \n" +
				"	STATUS 'x' \n" +
				"	INTO SQL ? \n" +
				"END TASK\n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		try {
			runScript(processId, logLevel, logToConsole, script, null, null, null);
		}
		catch (RuntimeException ex) {}
	}
}
