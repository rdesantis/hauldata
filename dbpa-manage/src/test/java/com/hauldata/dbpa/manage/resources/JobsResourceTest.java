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

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import javax.ws.rs.WebApplicationException;

import com.hauldata.dbpa.manage.api.Job;
import com.hauldata.dbpa.manage.api.ScriptArgument;

import junit.framework.TestCase;

public class JobsResourceTest extends TestCase {

	private static final String mondayScheduleName = "Monday";
	private static final String mondayScheduleBody = "Every Monday from '11/9/2015' until '11/16/2015' at '4:00 PM', '11/13/2015' every 2 hours from '10:30 AM' until '8:30 PM'";

	private static final String dailyScheduleName = "Daily";
	private static final String dailyScheduleBody = "Daily";

	private static final String crapScriptName = "crap";
	private static final String crapScriptBody = "TASK LOG 'It may be crap, but it''s MY crap' END TASK";

	private static final String invalidScriptName = "invalid";
	private static final String invalidScriptBody = "This is not a valid script";

	private static final String hasParamsScriptName = "HasParams";
	private static final String hasParamsScriptBody = "PARAMETERS one int, two VARCHAR(50), three DATE END PARAMETERS\n TASK GO END TASK\n";

	private static final String detritusName = "detritus";
	private static final String differentName = "NotGarbage";
	private static final String bogusName = "DoesNotExist";

	JobsResource jobsResource;
	Job detritusJob;
	List<ScriptArgument> someArguments;
	List<String> someSchedules;

	public JobsResourceTest(String name) {
		super(name);
	}

	protected void setUp() {
		ManagerResource managerResource = new ManagerResource();
		if (!managerResource.isStarted()) {
			managerResource.startup();
		}
		jobsResource = new JobsResource();

		ScriptsResource scriptsResource = new ScriptsResource();

		scriptsResource.put(crapScriptName, crapScriptBody);
		scriptsResource.put(invalidScriptName, invalidScriptBody);
		scriptsResource.put(hasParamsScriptName, hasParamsScriptBody);

		someArguments = new LinkedList<ScriptArgument>();
		someArguments.add(new ScriptArgument("first", "value1"));
		someArguments.add(new ScriptArgument("second", "value2"));
		someArguments.add(new ScriptArgument("third", "value3"));

		SchedulesResource schedulesResource = new SchedulesResource();

		schedulesResource.put(mondayScheduleName, mondayScheduleBody);
		schedulesResource.put(dailyScheduleName, dailyScheduleBody);

		someSchedules = new LinkedList<String>();
		someSchedules.add("Monday");
		someSchedules.add("Daily");

		detritusJob = new Job(crapScriptName, "fred", someArguments, someSchedules, true);
	}

	public void testPut() {

		deleteNoError(detritusName);

		int id = jobsResource.put(detritusName, detritusJob);

		assertTrue(id != -1);
	}

	public void testPutOver() {

		Job job = new Job(crapScriptName, "ethel", someArguments, someSchedules, true);

		int id = jobsResource.put(detritusName, job);

		assertTrue(id != -1);

		int newId = jobsResource.put(detritusName, job);

		assertEquals(id, newId);
	}

	public void testGetPostive() {

		// Positive test: can get an existing script

		testPut();

		Job checkJob = jobsResource.get(detritusName);

		assertEquals(detritusJob.getScriptName(), checkJob.getScriptName());
		assertEquals(detritusJob.getPropName(), checkJob.getPropName());
		assertEquals(detritusJob.getArguments().size(), checkJob.getArguments().size());
		for (int i = 0; i < detritusJob.getArguments().size(); i++) {
			assertEquals(detritusJob.getArguments().get(i).getName(), checkJob.getArguments().get(i).getName());
			assertEquals(detritusJob.getArguments().get(i).getValue(), checkJob.getArguments().get(i).getValue());
		}
		assertEquals(detritusJob.getScheduleNames().size(), checkJob.getScheduleNames().size());
		for (int i = 0; i < detritusJob.getScheduleNames().size(); i++) {
			assertEquals(detritusJob.getScheduleNames().get(i), checkJob.getScheduleNames().get(i));
		}
		assertEquals(detritusJob.isEnabled(), checkJob.isEnabled());
	}

	public void testGetNegative() {

		// Negative test: attempt to get a non-existent schedule fails as expected

		deleteNoError(bogusName);

		boolean isNonExistentGotten;
		try {
			jobsResource.get(bogusName);
			isNonExistentGotten = true;
		}
		catch (Exception ex) {
			assertIsWebAppException(notFoundMessage(bogusName), 404, ex);
			isNonExistentGotten = false;
		}

		assertFalse(isNonExistentGotten);
	}

	public void testDeletePositive() {

		// Positive test: can get an existing schedule

		testPut();

		jobsResource.delete(detritusName);
	}

	public void testDeleteNegative() {

		// Negative test: attempt to delete a non-existent schedule fails as expected

		deleteNoError(bogusName);

		boolean isNonExistentDeleted;
		try {
			jobsResource.delete(bogusName);
			isNonExistentDeleted = true;
		}
		catch (Exception ex) {
			assertIsWebAppException(notFoundMessage(bogusName), 404, ex);
			isNonExistentDeleted = false;
		}

		assertFalse(isNonExistentDeleted);
	}

	public void testGetNames() {

		// Create a bunch of like-named jobs and a not-like-named job.

		final int scriptCount = 9;
		for (int i = 1; i <= scriptCount; i++) {
			String garbageNameN = detritusName + String.valueOf(i);

			deleteNoError(garbageNameN);
			Job job = new Job(crapScriptName, garbageNameN, someArguments, someSchedules, true);
			jobsResource.put(garbageNameN, job);
		}
		deleteNoError(differentName);
		Job differentJob = new Job(crapScriptName, differentName, null, null, false);
		jobsResource.put(differentName, differentJob);

		// Confirm all like named are found.  There may be other similarly named schedules too.

		List<String> likeNames = jobsResource.getNames(Optional.of(detritusName + "%"));

		assertTrue(scriptCount <= likeNames.size());
		for (String name : likeNames) {
			assertTrue(name.startsWith(detritusName));
		}

		// Confirm that an all-names list is bigger than the like-named list.

		List<String> allNames = jobsResource.getNames(Optional.empty());

		assertTrue(likeNames.size() < allNames.size());
	}

	private void deleteNoError(String name) {
		try { jobsResource.delete(name); } catch (Exception ex) {}
	}

	private String notFoundMessage(String name) {
		return JobsResource.jobNotFoundMessageStem + name;
	}

	private void assertIsWebAppException(String message, int status, Exception ex) {
		assertTrue(ex instanceof WebApplicationException);
		WebApplicationException wex = (WebApplicationException)ex;
		assertEquals(message, wex.getMessage());
		assertEquals(status, wex.getResponse().getStatus());
	}
}
