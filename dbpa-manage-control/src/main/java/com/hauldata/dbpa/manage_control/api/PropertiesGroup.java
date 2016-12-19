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

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.hauldata.dbpa.DBPA;

public class PropertiesGroup {

	static final String propertiesExtension = "properties";

	private Map<String, Properties> group;

	public PropertiesGroup() {
		// Jackson deserialization
	}

	private PropertiesGroup(Map<String, Properties> group) {
		this.group = group;
	}

	@Override
	public boolean equals(Object obj) {
		return (obj != null) && (obj instanceof PropertiesGroup) && (((PropertiesGroup)obj).getGroup().equals(this.getGroup()));
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + ":" + String.valueOf(group);
	}

	@JsonProperty
	public Map<String, Properties> getGroup() {
		return group;
	}

	/**
	 * Instantiate properties group loaded from DBPA properties files with the indicated group name
	 * <p>
	 * @throws NoSuchFileException if the group directory does not exist
	 * @throws IOException if any other I/O error occurs
	 */
	public static PropertiesGroup load(String name) throws IOException {

		final Pattern pattern = Pattern.compile(DBPA.runProgramName + "\\.(.+)\\." + propertiesExtension);

		Path groupPath = DBPA.homePath.resolve(name);
		if (!Files.isDirectory(groupPath)) {
			throw new NoSuchFileException("Not a directory: " + groupPath.toString());
		}

		Map<String, Properties> group = new HashMap<String, Properties>();

		DirectoryStream<Path> propertiesPaths = null;
		FileInputStream in = null;
		try {
			propertiesPaths = Files.newDirectoryStream(groupPath, DBPA.runProgramName + ".*." + propertiesExtension);

			for (Path propertiesPath : propertiesPaths) {
				if (Files.isRegularFile(propertiesPath)) {

					String usage = null;
					Matcher matcher = pattern.matcher(propertiesPath.getFileName().toString());
					if (matcher.find()) {
						usage = matcher.group(1);

						Properties props = new Properties();
						in = new FileInputStream(propertiesPath.toString());

						props.load(in);

						in.close();
						in = null;

						group.put(usage, props);
					}
				}
			}
		}
		finally {
			if (in != null) try { in.close(); } catch (Exception ex) {}
			if (propertiesPaths != null) try { propertiesPaths.close(); } catch (Exception ex) {}
		}

		return new PropertiesGroup(group);
	}

	/**
	 * Save properties group to properties files with the indicated group name
	 * @throws IOException
	 */
	public void store(String name) throws IOException {
	
		Path groupPath = DBPA.homePath.resolve(name);

		Files.createDirectories(groupPath);

		FileOutputStream out = null;
		try {
			for (Entry<String, Properties> entry : group.entrySet()) {

				String usage = entry.getKey();
				Properties props = entry.getValue();

				Path propertiesPath = groupPath.resolve(DBPA.runProgramName + "." + usage + "." + propertiesExtension);
				out = new FileOutputStream(propertiesPath.toString());
				
				String comments = this.getClass().getName() + ":" + name;
				props.store(out, comments);

				out.close();
				out = null;
			}
		}
		finally {
			if (out != null) try { out.close(); } catch (Exception ex) {}
		}
	}

	/**
	 * Delete properties files with the indicated group name
	 * <p>
	 * Deletes all the DBPA properties files within the group directory.
	 * If the directory becomes empty, the directory itself is deleted.
	 * <p>
	 * @throws NoSuchFileException if the group directory does not exist
	 * @throws IOException if any other I/O error occurs
	 */
	public static void delete(String name) throws IOException {

		Path groupPath = DBPA.homePath.resolve(name);
		if (!Files.isDirectory(groupPath)) {
			throw new NoSuchFileException("Not a directory: " + groupPath.toString());
		}

		DirectoryStream<Path> propertiesPaths = null;
		try {
			propertiesPaths = Files.newDirectoryStream(groupPath, DBPA.runProgramName + ".*." + propertiesExtension);

			for (Path propertiesPath : propertiesPaths) {
				if (Files.isRegularFile(propertiesPath)) {
					Files.delete(propertiesPath);
				}
			}

			try {
				Files.delete(groupPath);
			}
			catch (DirectoryNotEmptyException ex) {
				// Ignore this.
			}
		}
		finally {
			if (propertiesPaths != null) try { propertiesPaths.close(); } catch (Exception ex) {}
		}
	}

	/**
	 * Get a list of the properties group names
	 * <p>
	 * @throws IOException if an I/O error occurs
	 */
	public static List<String> getNames(String likeName) throws IOException {
		
		List<String> result = new LinkedList<String>();

		DirectoryStream<Path> propertiesPaths = null;
		try {
			String glob = (likeName != null) ? likeName : "*";
			propertiesPaths = Files.newDirectoryStream(DBPA.homePath, glob);

			for (Path propertiesPath : propertiesPaths) {
				if (Files.isDirectory(propertiesPath)) {
					result.add(propertiesPath.getFileName().toString());
				}
			}
		}
		finally {
			if (propertiesPaths != null) try { propertiesPaths.close(); } catch (Exception ex) {}
		}

		return result;
	}

}
