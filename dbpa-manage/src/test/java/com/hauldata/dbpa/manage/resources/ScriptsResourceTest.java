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

import java.util.List;
import java.util.Optional;

import javax.ws.rs.WebApplicationException;

import com.hauldata.dbpa.manage.api.ScriptValidation;

import junit.framework.TestCase;

public class ScriptsResourceTest extends TestCase {

	private static final String garbageName = "garbage";
	private static final String garbageBody = "TASK LOG 'It may be garbage, but it''s MY garbage' END TASK";

	private static final String differentName = "NotGarbage";
	private static final String bogusName = "DoesNotExist";

	private static final String invalidName = "invalid";
	private static final String invalidBody = "This is not a valid script";

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

	public void testPut() {

		scriptsResource.put(garbageName, garbageBody);
	}

	public void testGetPostive() {

		// Positive test: can get an existing script

		testPut();

		String checkBody = scriptsResource.get(garbageName);

		assertEquals(garbageBody, checkBody);
	}

	public void testGetNegative() {

		// Negative test: attempt to get a non-existent script fails as expected

		try { scriptsResource.delete(bogusName); } catch (Exception ex) {}

		boolean isNonExistentGotten;
		try {
			scriptsResource.get(bogusName);
			isNonExistentGotten = true;
		}
		catch (Exception ex) {
			assertIsWebAppException(notFoundMessage(bogusName), 404, ex);
			isNonExistentGotten = false;
		}

		assertFalse(isNonExistentGotten);
	}

	public void testDeletePositive() {

		// Positive test: can get an existing script

		testPut();

		scriptsResource.delete(garbageName);
	}

	public void testDeleteNegative() {

		// Negative test: attempt to delete a non-existent script fails as expected

		try { scriptsResource.delete(bogusName); } catch (Exception ex) {}

		boolean isNonExistentDeleted;
		try {
			scriptsResource.delete(bogusName);
			isNonExistentDeleted = true;
		}
		catch (Exception ex) {
			assertIsWebAppException(notFoundMessage(bogusName), 404, ex);
			isNonExistentDeleted = false;
		}

		assertFalse(isNonExistentDeleted);
	}

	public void testGetNames() {

		// Create a bunch of like-named scripts and a not-like-named script.

		final int scriptCount = 9;
		for (int i = 1; i <= scriptCount; i++) {
			scriptsResource.put(garbageName + String.valueOf(i), garbageBody);
		}
		scriptsResource.put(differentName, garbageBody);

		// Confirm all like named are found.  There may be other similarly named scripts too.

		List<String> likeNames = scriptsResource.getNames(Optional.of(garbageName + "*"));

		assertTrue(scriptCount <= likeNames.size());
		for (String name : likeNames) {
			assertTrue(name.startsWith(garbageName));
		}

		// Confirm that an all-names list is bigger than the like-named list.

		List<String> allNames = scriptsResource.getNames(Optional.empty());

		assertTrue(likeNames.size() < allNames.size());
	}

	public void testValidatePositive() {

		ScriptValidation validation;

		testPut();

		validation = scriptsResource.validate(garbageName);
		assertTrue(validation.isValid());
		assertEquals(0, validation.getParameters().size());
	}

	public void testValidateNegative() {

		ScriptValidation validation;

		scriptsResource.put(invalidName, invalidBody);

		validation = scriptsResource.validate(invalidName);
		assertFalse(validation.isValid());
		assertNotNull(validation.getValidationMessage());
	}

	public void testValidateNonExistent() {

		try { scriptsResource.delete(bogusName); } catch (Exception ex) {}

		boolean isNonExistentValidated;
		try {
			scriptsResource.validate(bogusName);
			isNonExistentValidated = true;
		}
		catch (Exception ex) {
			assertIsWebAppException(notFoundMessage(bogusName), 404, ex);
			isNonExistentValidated = false;
		}

		assertFalse(isNonExistentValidated);
	}

	private String notFoundMessage(String name) {
		return ScriptsResource.scriptNotFoundMessageStem + name;
	}

	private void assertIsWebAppException(String message, int status, Exception ex) {
		assertTrue(ex instanceof WebApplicationException);
		WebApplicationException wex = (WebApplicationException)ex;
		assertEquals(message, wex.getMessage());
		assertEquals(status, wex.getResponse().getStatus());
	}

}
