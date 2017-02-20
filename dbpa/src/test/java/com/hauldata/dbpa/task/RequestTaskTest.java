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

	public RequestTaskTest(String name) {
		super(name);
	}

	public void testGet() throws Exception {

		String processId = "GetTest";
		String script =
				"VARIABLES nothing VARCHAR END VARIABLES\n" +
				"TASK GetJobInfo \n" +
				"	IF nothing IS NOT NULL \n" +
				"	REQUEST 'http://localhost:8080/jobs/{name}' " +
				"	HEADER nothing NULL 'ignore this' \n" +
				"	GET 'name', 'random', 'whatever' \n" +
				"	FROM SQL SELECT * FROM test.reqsource END SQL \n" +
				"	KEEP 'random', 'name' \n" +
				"	RESPONSE 'scriptName', 'propName', 'enabled' \n" +
				"	STATUS 'status' \n" +
				"	INTO SQL INSERT INTO test.restarget VALUES (?,?,?,?,?,?) \n" +
				"END TASK\n" +
				"TASK ArroPay \n" +
				"	AFTER \n" +
				"	REQUEST 'URL HERE' " +
				"	HEADER 'Authorization' 'TOKEN HERE' \n" +
				"	POST BODY 'cardNumber', 'cardHolderName', 'employeeId', 'governmentId', 'accountNumber', 'routingNumber', 'proxyId', 'marketId', 'status' \n" +
				"	FROM SQL SELECT * FROM test.arropaycardrequest END SQL \n" +
				"	--KEEP 'employeeId', 'marketId' \n" +
				"	RESPONSE 'id', 'lastFour', 'createDate' \n" +
				"	STATUS 'status' \n" +
				"	INTO SQL INSERT INTO test.restarget (stuff, name, scriptName, status) VALUES (?,?,?,?) \n" +
				"END TASK\n" +
				"";

		Level logLevel = Level.info;
		boolean logToConsole = true;

		Analyzer analyzer = runScript(processId, logLevel, logToConsole, script, null, null, null);
		Analyzer.RecordIterator recordIterator = analyzer.recordIterator();

		Analyzer.Record record;

		recordIterator.next();
		assertFalse(recordIterator.hasNext());
	}
}
