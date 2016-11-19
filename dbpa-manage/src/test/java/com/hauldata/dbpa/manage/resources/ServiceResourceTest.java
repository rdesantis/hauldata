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

import com.hauldata.dbpa.ManageDbp;
import com.hauldata.dbpa.manage.JobManager;

import junit.framework.TestCase;

public class ServiceResourceTest extends TestCase {

	public ServiceResourceTest(String name) {
		super(name);
	}

	public void testService() throws Exception {

		String commandLine = "server";	// "server dbpa-manage.yml"
		String args[] = commandLine.split(" ");

		ManageDbp.main(args);

		ManagerResource manager = new ManagerResource();
		assert(manager.isStarted());

		ServiceResource service = ServiceResource.getInstance();
		assertNotNull(service);

		service.kill();

		boolean isManagerAvailable;
		try {
			JobManager.getInstance();
			isManagerAvailable = true;
		}
		catch (RuntimeException ex) {
			assertEquals(JobManager.notAvailableMessage, ex.getMessage());
			isManagerAvailable = false;
		}
		assertFalse(isManagerAvailable);
	}
}