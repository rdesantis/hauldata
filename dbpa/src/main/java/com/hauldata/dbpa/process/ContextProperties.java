/*
 * Copyright (c) 2016 - 2018, Ronald DeSantis
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

package com.hauldata.dbpa.process;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.hauldata.dbpa.DBPA;
import com.hauldata.dbpa.loader.FileLoader;
import com.hauldata.dbpa.loader.Loader;
import com.hauldata.dbpa.log.ConsoleAppender;
import com.hauldata.dbpa.log.FileAppender;
import com.hauldata.dbpa.log.Logger;
import com.hauldata.dbpa.log.Logger.Level;
import com.hauldata.dbpa.log.NullLogger;
import com.hauldata.dbpa.log.RootLogger;
import com.hauldata.dbpa.log.TableAppender;

public class ContextProperties {

	private String contextName;

	private Properties connectionProps;
	private Properties sessionProps;
	private Properties ftpProps;
	private Properties pathProps;
	private Properties logProps;

	private Map<String, Properties> properties;

	private ContextProperties() {

		contextName = null;

		connectionProps = null;
		sessionProps = null;
		ftpProps = null;
		pathProps = getDefaultPathsProperties();
		logProps = null;
	}

	private Properties getDefaultPathsProperties() {

		Properties props = new Properties();
		// Don't set "read" or "write" defaults.  If either of these is missing, the "data" default will be used.
		props.setProperty("data", DBPA.home);
		props.setProperty("process", DBPA.home);
		props.setProperty("properties", DBPA.home);
		props.setProperty("schedule", DBPA.home);
		props.setProperty("log", DBPA.home);
		return props;
	}

	private static final ContextProperties nullContextProperties = new ContextProperties();

	public ContextProperties(String contextName) {
		this(contextName, nullContextProperties);
	}

	public ContextProperties(String contextName, ContextProperties defaults) {
		this(contextName, defaults, true);
	}

	public ContextProperties(String contextName, ContextProperties defaults, boolean respectLogDefaults) {

		this.contextName = contextName;
		properties = new HashMap<String, Properties>();

		connectionProps = putProperties(properties, "jdbc", defaults.connectionProps);
		sessionProps = putProperties(properties, "mail", defaults.sessionProps);
		ftpProps = putProperties(properties, "ftp", defaults.ftpProps);
		pathProps = putProperties(properties, "path", defaults.pathProps);
		logProps = putProperties(properties, "log", respectLogDefaults ? defaults.logProps : null);

		putProperties(properties, "statsd", null);
	}

	private Properties putProperties(Map<String, Properties> properties, String usage, Properties defaults) {
		Properties result = getProperties(usage, defaults);
		properties.put(usage, result);
		return result;
	}

	public Context createContext(String processId) {
		return createContext(processId, null, null);
	}

	public Context createContext(String processId, Context parentContext, String parentTaskId) {

		Context context = null;

		try {
			// TODO: Make loader type configurable through properties.

			Loader loader = (parentContext == null) ? new FileLoader(getPathname("process")) : parentContext.loader;

			context = new Context(connectionProps, sessionProps, ftpProps, pathProps, loader);

			context.logger = (parentContext == null) ? setupLog(processId, context) : parentContext.logger.nestProcess(parentTaskId, processId);
		}
		catch (Exception ex) {
			try { if (context != null) context.close(); }
			catch (Exception exx) {}

			throw ex;
		}

		return context;
	}

	/**
	 * Return properties that apply to the indicated usage in this context.
	 *
	 * @param usage is the usage for the properties
	 * @return the properties
	 */

	public Properties getProperties(String usage) {
		return properties.get(usage);
	}

	private Properties getProperties(String usage, Properties defaults) {

		Properties props = (defaults != null) ? new Properties(defaults) : new Properties();

		String fileName = contextName + "." + usage + ".properties";
		Path path = Files.getPath(DBPA.home, fileName);

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
			throw new RuntimeException("Error reading " + usage + " properties from file \"" + fileName + "\": " + message, ex);
		}
		finally {
			try { if (in != null) in.close(); }
			catch (Exception ex) {}
		}

		return props;
	}

	private String getPathname(String usage) {
		// Defensive: The == null path should never execute because of how ContextProperties() default constructor is defined.
		return (pathProps != null) ? pathProps.getProperty(usage, ".") : DBPA.home;
	}

	private Logger setupLog(String processID, Context context) {

		String logTypeList = (logProps != null) ? logProps.getProperty("type") : null;
		if ((logTypeList == null) || logTypeList.equals("null")) {
			return NullLogger.logger;
		}

		try {
			Level defaultLevel = getLoggerLevel("level", Level.info);

			RootLogger log = new RootLogger(processID);

			String[] logTypes = logTypeList.split(",");
			for (String logType : logTypes) {
				if (logType.equals("null")) {
					// Do nothing.
				}
				else if (logType.equals("console")) {
					Level level = getLoggerLevel("consoleLevel", defaultLevel);
					log.add(new ConsoleAppender(level));
				}
				else if (logType.equals("file")) {
					String fileName = Files.getPath(getPathname("log"), logProps.getProperty("fileName")).toString();
					String rolloverSchedule = logProps.getProperty("fileRollover");
					Level level = getLoggerLevel("fileLevel", defaultLevel);

					log.add(new FileAppender(fileName, rolloverSchedule, level));
				}
				else if (logType.equals("table")) {
					Level level = getLoggerLevel("tableLevel", defaultLevel);
					log.add(new TableAppender(context, logProps.getProperty("tableName"), level));
				}
				else {
					throw new RuntimeException("Unrecognized type \"" + logType + "\"");
				}
			}

			return log;
		}
		catch (Exception ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Failed attempting to set up log : " + message, ex);
		}
	}

	private Logger.Level getLoggerLevel(String propertyName, Logger.Level defaultLevel) {

		String loggerLevelName = (logProps != null) ? logProps.getProperty(propertyName) : null;
		Logger.Level level = (loggerLevelName != null) ? Logger.Level.valueOf(loggerLevelName) : defaultLevel;
		return level;
	}
}
