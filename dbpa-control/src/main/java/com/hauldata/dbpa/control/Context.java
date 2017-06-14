/*
 * Copyright (c) 2017, Ronald DeSantis
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

package com.hauldata.dbpa.control;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.hauldata.dbpa.DBPA;

public class Context {

	public enum Usage { PROCESS, READ, WRITE, PROPERTIES, SCHEDULE };

	private static final String runProgramName = "RunDbp";
	private static final String controlProgramName = "ControlDbp";

	private Map<Usage, Path> paths;

	public Context() {
		Properties runPathProperties = getPathProperties(runProgramName);
		Properties controlPathProperties = getPathProperties(controlProgramName);

		paths = new HashMap<Usage, Path>();

		for (Usage usage : Usage.values()) {
			paths.put(usage, getPath(usage, controlPathProperties, runPathProperties));
		}
	}

	private Properties getPathProperties(String programName) {

		// Adapted from com.hauldata.dbpa.process.ContextProperties.getProperties(String, Properties) 

		String fileName = programName + ".path.properties";
		Path path = DBPA.homePath.resolve(fileName).toAbsolutePath().normalize();

		Properties props = new Properties();

		FileInputStream in = null;
		try {
			in = new FileInputStream(path.toString());
			props.load(in);
		}
		catch (FileNotFoundException ex) {
			// Ignore this; default properties will be used.
		}
		catch (Exception ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error reading path properties from file \"" + fileName + "\": " + message, ex);
		}
		finally {
			try { if (in != null) in.close(); } catch (Exception ex) {}
		}

		return props;
	}

	private Path getPath(Usage usage, Properties controlPathProperties, Properties runPathProperties) {
		String propertyName = usage.name().toLowerCase();
		String pathName = controlPathProperties.getProperty(propertyName, runPathProperties.getProperty(propertyName, DBPA.homeName));
		return Paths.get(pathName);
	}

	public Path getPath(Usage usage, String fileName) {
		return paths.get(usage).resolve(fileName).toAbsolutePath().normalize();
	}
}
