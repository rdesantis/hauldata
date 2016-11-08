/*
 * Copyright (c) 2016, Ronald DeSantis
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

package com.hauldata.dbpa.manage.resources;

import junit.framework.TestCase;

public class ScriptsResourceTest extends TestCase {

	private ScriptsResource scriptsResource;

	public ScriptsResourceTest(String name) {
		super(name);
	}

	protected void setUp() {
		ManagerResource managerResource = new ManagerResource();
		if (!managerResource.isStarted()) {
			managerResource.startup();
		}
		scriptsResource = new ScriptsResource();
	}

	public void testPutScript() {
	
		String name = "garbage";
		String body = "TASK LOG 'It may be garbage, but it''s MY garbage' END TASK";

		scriptsResource.put(name, body);
	}
}
