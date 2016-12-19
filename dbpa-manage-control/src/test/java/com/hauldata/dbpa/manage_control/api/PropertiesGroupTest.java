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

package com.hauldata.dbpa.manage_control.api;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.hauldata.dbpa.DBPA;

import junit.framework.TestCase;

public class PropertiesGroupTest extends TestCase {

	private static final String sampleGroupName = "garbage";
	private static final String otherGroupName = "an_appalling_dump_heap";

	private Map<String, Properties> sampleMap;

	public PropertiesGroupTest(String name) {
		super(name);
	}

	protected void setUp() throws IOException {

		Properties fredProperties = new Properties();
		fredProperties.setProperty("first", "whatever");
		fredProperties.setProperty("second", "whatever else");

		Properties ethelProperties = new Properties();
		ethelProperties.setProperty("one", "something");
		ethelProperties.setProperty("two", "something else");

		Properties rickyProperties = new Properties();
		rickyProperties.setProperty("uno", "cosa");
		rickyProperties.setProperty("due", "un'altra cosa");

		sampleMap = new HashMap<String, Properties>();
		sampleMap.put("fred", fredProperties);
		sampleMap.put("ethel", ethelProperties);
		sampleMap.put("ricket", rickyProperties);
		
		storeProperties(sampleGroupName, sampleMap);
	}

	public void testLoad() throws IOException {
		PropertiesGroup group = PropertiesGroup.load(sampleGroupName);
		assertEquals(sampleMap, group.getGroup());
	}

	public void testStore() throws IOException {
		PropertiesGroup group = PropertiesGroup.load(sampleGroupName);
		group.store(otherGroupName);
		PropertiesGroup otherGroup = PropertiesGroup.load(otherGroupName);
		assertEquals(group, otherGroup);
	}

	public void testGetNames() throws IOException {
		List<String> names = PropertiesGroup.getNames(null);
		System.out.println(names.toString());
		assertTrue(names.contains(sampleGroupName));
	}

	public void testDelete() throws IOException {
		PropertiesGroup.delete(sampleGroupName);
		List<String> names = PropertiesGroup.getNames(null);
		assertFalse(names.contains(sampleGroupName));
	}

	private void storeProperties(String groupName, Map<String, Properties> propertiesMap) throws IOException {

		Path groupPath = DBPA.homePath.resolve(groupName);

		Files.createDirectories(groupPath);

		for (Map.Entry<String, Properties> entry : propertiesMap.entrySet()) {
			storeProperties(groupPath, entry.getKey(), entry.getValue());
		}
	}

	private void storeProperties(Path groupPath, String usage, Properties props) throws IOException {

		Path propertiesPath = groupPath.resolve(DBPA.runProgramName + "." + usage + ".properties");
		FileOutputStream out = new FileOutputStream(propertiesPath.toString());

		String comments = this.getClass().getName() + ":" + groupPath.getFileName().toString();
		props.store(out, comments);

		out.close();
	}
}
