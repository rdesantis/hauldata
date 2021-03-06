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

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;

import javax.naming.NameNotFoundException;

import com.hauldata.dbpa.manage.JobManager;
import com.hauldata.dbpa.manage.JobManagerException.NotAvailable;
import com.hauldata.dbpa.manage_control.api.ScheduleValidation;

import junit.framework.TestCase;

public class SchedulesResourceTest extends TestCase {

	private static final String garbageName = "garbage";
	private static final String garbageBody = "Every Monday from '11/9/2015' until '11/16/2015' at '4:00 PM', '11/13/2015' every 2 hours from '10:30 AM' until '8:30 PM'";

	private static final String differentName = "NotGarbage";
	private static final String bogusName = "DoesNotExist";

	private static final String invalidName = "invalid";
	private static final String invalidBody = "This is not a valid schedule";

	SchedulesResource schedulesResource;

	public SchedulesResourceTest(String name) {
		super(name);
	}

	protected void setUp() throws SQLException {
		JobManager.instantiate(false).startup();
		schedulesResource = new SchedulesResource();
	}

	protected void tearDown() throws InterruptedException {
		JobManager.getInstance().shutdown();
		JobManager.killInstance();
	}

	public void testPut() throws SQLException {

		deleteNoError(garbageName);

		int id = schedulesResource.put(garbageName, garbageBody);

		assertTrue(id != -1);
	}

	public void testPutOver() throws SQLException {

		int id = schedulesResource.put(garbageName, garbageBody);

		assertTrue(id != -1);

		int newId = schedulesResource.put(garbageName, garbageBody);

		assertEquals(id, newId);
	}

	public void testGetPostive() throws NameNotFoundException, IllegalArgumentException, SQLException {

		// Positive test: can get an existing script

		testPut();

		String checkBody = schedulesResource.get(garbageName);

		assertEquals(garbageBody, checkBody);
	}

	public void testGetNegative() throws IllegalArgumentException, SQLException {

		// Negative test: attempt to get a non-existent schedule fails as expected

		deleteNoError(bogusName);

		boolean isNonExistentGotten;
		try {
			schedulesResource.get(bogusName);
			isNonExistentGotten = true;
		}
		catch (NameNotFoundException ex) {
			assertEquals(notFoundMessage(bogusName), ex.getMessage());
			isNonExistentGotten = false;
		}

		assertFalse(isNonExistentGotten);
	}

	public void testDeletePositive() throws NameNotFoundException, SQLException {

		// Positive test: can get an existing schedule

		testPut();

		schedulesResource.delete(garbageName);
	}

	public void testDeleteNegative() throws SQLException {

		// Negative test: attempt to delete a non-existent schedule fails as expected

		deleteNoError(bogusName);

		boolean isNonExistentDeleted;
		try {
			schedulesResource.delete(bogusName);
			isNonExistentDeleted = true;
		}
		catch (NameNotFoundException ex) {
			assertEquals(notFoundMessage(bogusName), ex.getMessage());
			isNonExistentDeleted = false;
		}

		assertFalse(isNonExistentDeleted);
	}

	public void testGetNames() throws SQLException {

		// Create a bunch of like-named schedules and a not-like-named schedule.

		final int scriptCount = 9;
		for (int i = 1; i <= scriptCount; i++) {
			String garbageNameN = garbageName + String.valueOf(i);

			deleteNoError(garbageNameN);
			schedulesResource.put(garbageNameN, garbageBody);
		}
		deleteNoError(differentName);
		schedulesResource.put(differentName, garbageBody);

		// Confirm all like named are found.  There may be other similarly named schedules too.

		List<String> likeNames = schedulesResource.getNames(garbageName + "%");

		assertTrue(scriptCount <= likeNames.size());
		for (String name : likeNames) {
			assertTrue(name.startsWith(garbageName));
		}

		// Confirm that an all-names list is bigger than the like-named list.

		List<String> allNames = schedulesResource.getNames(null);

		assertTrue(likeNames.size() < allNames.size());
	}

	public void testValidatePositive() throws SQLException, NameNotFoundException, IllegalArgumentException {

		ScheduleValidation validation;

		testPut();

		validation = schedulesResource.validate(garbageName);
		assertTrue(validation.isValid());
	}

	public void testValidateNegative() throws SQLException, NameNotFoundException, IllegalArgumentException {

		ScheduleValidation validation;

		deleteNoError(invalidName);
		schedulesResource.put(invalidName, invalidBody);

		validation = schedulesResource.validate(invalidName);
		assertFalse(validation.isValid());
		assertNotNull(validation.getValidationMessage());
	}

	public void testValidateNonExistent() throws IllegalArgumentException, SQLException {

		deleteNoError(bogusName);

		boolean isNonExistentValidated;
		try {
			schedulesResource.validate(bogusName);
			isNonExistentValidated = true;
		}
		catch (NameNotFoundException ex) {
			assertEquals(notFoundMessage(bogusName), ex.getMessage());
			isNonExistentValidated = false;
		}

		assertFalse(isNonExistentValidated);
	}

	public void testGetNoneRunning() throws SQLException {
		List<String> names = schedulesResource.getRunning();
		assertTrue(names.isEmpty());
	}

	public void testGetSomeRunning() throws NotAvailable, SQLException, InterruptedException {

		String name = "quickie";

		LocalDateTime sooner = LocalDateTime.now().plusSeconds(2);
		LocalDateTime later = sooner.plusSeconds(2);
		String schedule =
				"TODAY EVERY 2 SECONDS FROM '" +
				sooner.format(DateTimeFormatter.ofPattern("HH:mm:ss")) + "' UNTIL '" +
				later.format(DateTimeFormatter.ofPattern("HH:mm:ss")) + "'";

		int id = schedulesResource.put(name, schedule);

		List<Integer> ids = new LinkedList<Integer>();
		ids.add(id);

		JobManager.getInstance().getScheduler().start(ids);

		List<String> names = schedulesResource.getRunning();

		assertEquals(1, names.size());
		assertEquals(name, names.get(0));

		Thread.sleep(5000);

		testGetNoneRunning();
	}

	private void deleteNoError(String name) {
		try { schedulesResource.delete(name); } catch (Exception ex) {}
	}

	private String notFoundMessage(String name) {
		return SchedulesResource.scheduleNotFoundMessageStem + name;
	}
}
