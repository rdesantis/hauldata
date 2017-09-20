/*
 * Copyright (c) 2016, 2017, Ronald DeSantis
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

import com.hauldata.dbpa.manage.JobManager;

import junit.framework.TestCase;

public class SchemaResourceTest extends TestCase {


	public SchemaResourceTest(String name) {
		super(name);
	}

	public void testSchema() throws Exception {

		JobManager manager = JobManager.instantiate(false);
		SchemaResource schemaResource = new SchemaResource();

		if (manager.canStartup()) {
			manager.startup();

			assertTrue(schemaResource.confirm());

			manager.shutdown();
		}
		else {
			schemaResource.delete();
			
			assertFalse(schemaResource.confirm());

			schemaResource.create();

			assertTrue(schemaResource.confirm());
		}

		JobManager.killInstance();
	}
}
