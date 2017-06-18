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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.sql.SQLException;
import java.util.List;

import com.hauldata.dbpa.manage.JobManager;
import com.hauldata.dbpa.manage_control.api.ScriptValidation;

import junit.framework.TestCase;

public class ScriptsResourceTest extends TestCase {

	private static final String garbageName = "garbage";
	private static final String garbageBody = "TASK LOG 'It may be garbage, but it''s MY garbage' END TASK";

	private static final String differentName = "NotGarbage";
	private static final String bogusName = "DoesNotExist";

	private static final String invalidName = "invalid";
	private static final String invalidBody = "This is not a valid script";

	private static final String hasParamsName = "HasParams";
	private static final String hasParamsBody = "PARAMETERS one int, two VARCHAR(50), three DATE END PARAMETERS\n TASK GO END TASK\n";

	private ScriptsResource scriptsResource;

	public ScriptsResourceTest(String name) {
		super(name);
	}

	protected void setUp() throws SQLException {
		JobManager.instantiate(false).startup();
		scriptsResource = new ScriptsResource();
	}

	protected void tearDown() throws InterruptedException {
		JobManager.getInstance().shutdown();
		JobManager.killInstance();
	}

	public void testPut() throws IOException {

		scriptsResource.put(garbageName, garbageBody);
	}

	public void testGetPostive() throws IOException {

		// Positive test: can get an existing script

		testPut();

		String checkBody = scriptsResource.get(garbageName);

		assertEquals(garbageBody, checkBody);
	}

	public void testGetNegative() throws IOException {

		// Negative test: attempt to get a non-existent script fails as expected

		try { scriptsResource.delete(bogusName); } catch (Exception ex) {}

		boolean isNonExistentGotten;
		try {
			scriptsResource.get(bogusName);
			isNonExistentGotten = true;
		}
		catch (FileNotFoundException ex) {
			assertEquals(notFoundMessage(bogusName), ex.getMessage());
			isNonExistentGotten = false;
		}

		assertFalse(isNonExistentGotten);
	}

	public void testDeletePositive() throws IOException {

		// Positive test: can get an existing script

		testPut();

		scriptsResource.delete(garbageName);
	}

	public void testDeleteNegative() throws IOException {

		// Negative test: attempt to delete a non-existent script fails as expected

		try { scriptsResource.delete(bogusName); } catch (Exception ex) {}

		boolean isNonExistentDeleted;
		try {
			scriptsResource.delete(bogusName);
			isNonExistentDeleted = true;
		}
		catch (NoSuchFileException ex) {
			assertEquals(notFoundMessage(bogusName), ex.getMessage());
			isNonExistentDeleted = false;
		}

		assertFalse(isNonExistentDeleted);
	}

	public void testGetNames() throws IOException {

		// Create a bunch of like-named scripts and a not-like-named script.

		final int scriptCount = 9;
		for (int i = 1; i <= scriptCount; i++) {
			scriptsResource.put(garbageName + String.valueOf(i), garbageBody);
		}
		scriptsResource.put(differentName, garbageBody);

		// Confirm all like named are found.  There may be other similarly named scripts too.

		List<String> likeNames = scriptsResource.getNames(garbageName + "*");

		assertTrue(scriptCount <= likeNames.size());
		for (String name : likeNames) {
			assertTrue(name.startsWith(garbageName));
		}

		// Confirm that an all-names list is bigger than the like-named list.

		List<String> allNames = scriptsResource.getNames(null);

		assertTrue(likeNames.size() < allNames.size());
	}

	public void testValidatePositive() throws IOException {

		ScriptValidation validation;

		testPut();

		validation = scriptsResource.validate(garbageName);
		assertTrue(validation.isValid());
		assertEquals(0, validation.getParameters().size());

		scriptsResource.put(hasParamsName, hasParamsBody);

		validation = scriptsResource.validate(hasParamsName);
		assertTrue(validation.isValid());
		assertEquals(3, validation.getParameters().size());
		assertEquals("ONE", validation.getParameters().get(0).getName());
		assertEquals("INTEGER", validation.getParameters().get(0).getTypeName());
		assertEquals("TWO", validation.getParameters().get(1).getName());
		assertEquals("VARCHAR", validation.getParameters().get(1).getTypeName());
		assertEquals("THREE", validation.getParameters().get(2).getName());
		assertEquals("DATETIME", validation.getParameters().get(2).getTypeName());
	}

	public void testValidateNegative() throws IOException {

		ScriptValidation validation;

		scriptsResource.put(invalidName, invalidBody);

		validation = scriptsResource.validate(invalidName);
		assertFalse(validation.isValid());
		assertNotNull(validation.getValidationMessage());
	}

	public void testValidateNonExistent() throws IOException {

		try { scriptsResource.delete(bogusName); } catch (Exception ex) {}

		boolean isNonExistentValidated = true;
		try {
			scriptsResource.validate(bogusName);
		}
		catch (FileNotFoundException ex) {
			assertEquals(notFoundMessage(bogusName), ex.getMessage());
			isNonExistentValidated = false;
		}

		assertFalse(isNonExistentValidated);
	}

	private String notFoundMessage(String name) {
		return ScriptsResource.scriptNotFoundMessageStem + name;
	}
}
