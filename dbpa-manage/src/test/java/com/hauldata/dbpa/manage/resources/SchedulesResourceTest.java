package com.hauldata.dbpa.manage.resources;

import java.util.List;
import java.util.Optional;

import javax.ws.rs.WebApplicationException;

import com.hauldata.dbpa.manage.api.ScheduleValidation;

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

	protected void setUp() {
		ManagerResource managerResource = new ManagerResource();
		if (!managerResource.isStarted()) {
			managerResource.startup();
		}
		schedulesResource = new SchedulesResource();
	}

	public void testPut() {

		deleteNoError(garbageName);

		schedulesResource.put(garbageName, garbageBody);
	}

	public void testGetPostive() {

		// Positive test: can get an existing script

		testPut();

		String checkBody = schedulesResource.get(garbageName);

		assertEquals(garbageBody, checkBody);
	}

	public void testGetNegative() {

		// Negative test: attempt to get a non-existent schedule fails as expected

		deleteNoError(bogusName);

		boolean isNonExistentGotten;
		try {
			schedulesResource.get(bogusName);
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

		schedulesResource.delete(garbageName);
	}

	public void testDeleteNegative() {

		// Negative test: attempt to delete a non-existent schedule fails as expected

		deleteNoError(bogusName);

		boolean isNonExistentDeleted;
		try {
			schedulesResource.delete(bogusName);
			isNonExistentDeleted = true;
		}
		catch (Exception ex) {
			assertIsWebAppException(notFoundMessage(bogusName), 404, ex);
			isNonExistentDeleted = false;
		}

		assertFalse(isNonExistentDeleted);
	}

	public void testGetNames() {

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

		List<String> likeNames = schedulesResource.getNames(Optional.of(garbageName + "%"));

		assertTrue(scriptCount <= likeNames.size());
		for (String name : likeNames) {
			assertTrue(name.startsWith(garbageName));
		}

		// Confirm that an all-names list is bigger than the like-named list.

		List<String> allNames = schedulesResource.getNames(Optional.empty());

		assertTrue(likeNames.size() < allNames.size());
	}

	public void testValidatePositive() {

		ScheduleValidation validation;

		testPut();

		validation = schedulesResource.validate(garbageName);
		assertTrue(validation.isValid());
	}

	public void testValidateNegative() {

		ScheduleValidation validation;

		deleteNoError(invalidName);
		schedulesResource.put(invalidName, invalidBody);

		validation = schedulesResource.validate(invalidName);
		assertFalse(validation.isValid());
		assertNotNull(validation.getValidationMessage());
	}

	public void testValidateNonExistent() {

		deleteNoError(bogusName);

		boolean isNonExistentValidated;
		try {
			schedulesResource.validate(bogusName);
			isNonExistentValidated = true;
		}
		catch (Exception ex) {
			assertIsWebAppException(notFoundMessage(bogusName), 404, ex);
			isNonExistentValidated = false;
		}

		assertFalse(isNonExistentValidated);
	}

	private void deleteNoError(String name) {
		try { schedulesResource.delete(name); } catch (Exception ex) {}
	}

	private String notFoundMessage(String name) {
		return SchedulesResource.scheduleNotFoundMessageStem + name;
	}

	private void assertIsWebAppException(String message, int status, Exception ex) {
		assertTrue(ex instanceof WebApplicationException);
		WebApplicationException wex = (WebApplicationException)ex;
		assertEquals(message, wex.getMessage());
		assertEquals(status, wex.getResponse().getStatus());
	}
}
