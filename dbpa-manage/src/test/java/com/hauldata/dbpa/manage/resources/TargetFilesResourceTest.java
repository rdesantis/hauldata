/*
 * Copyright (c) 2018, Ronald DeSantis
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

import java.io.IOException;
import java.sql.SQLException;

import com.hauldata.dbpa.manage.JobManager;

import junit.framework.TestCase;

public class TargetFilesResourceTest extends TestCase {

	private SourceFilesResource sourceFilesResource;
	private TargetFilesResource targetFilesResource;

	public TargetFilesResourceTest(String name) {
		super(name);
	}

	protected void setUp() throws SQLException {
		JobManager.instantiate(false);
		sourceFilesResource = new SourceFilesResource();
		targetFilesResource = new TargetFilesResource();
	}

	protected void tearDown() throws InterruptedException {
		JobManager.killInstance();
	}

	public void testGet() throws IOException {

		String name = "TargetFilesResourceTest.txt";
		String body = "This was written by a call to sourceFilesResource.put(String, String)";
		sourceFilesResource.put("../target/" + name, body);

		String retrievedBody = targetFilesResource.get(name);

		assertEquals(body, retrievedBody);
	}

	public void testDelete() throws IOException {

		String name = "should not exist.txt";
		String body = "If this exists, TargetFilesResourceTest.testDelete() failed";
		sourceFilesResource.put("../target/" + name, body);

		targetFilesResource.delete(name);
	}
}
