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

package com.hauldata.dbpa.process;

import java.io.FileInputStream;
import java.nio.file.Path;
import java.util.Properties;

import com.hauldata.dbpa.loader.FileLoader;
import com.hauldata.dbpa.loader.Loader;
import com.hauldata.dbpa.log.ConsoleAppender;
import com.hauldata.dbpa.log.FileAppender;
import com.hauldata.dbpa.log.Logger;
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

	private String dataPath;
	private String processPath;
	private String logPath;

	private static final String dbpaHomeName = "DBPA_HOME";
	private static final String dbpaHome;

	static {
		String dbpaHomeValue = System.getenv(dbpaHomeName);
		dbpaHome = (dbpaHomeValue != null) ? dbpaHomeValue : ".";
	}

	private ContextProperties() {

		contextName = null;

		connectionProps = null;
		sessionProps = null;
		ftpProps = null;
		pathProps = getDefaultPathsProperties();
		logProps = null;
	}

	private static final ContextProperties nullContextProperties = new ContextProperties();

	public ContextProperties(String contextName) {
		this(contextName, nullContextProperties);
	}

	public ContextProperties(String contextName, ContextProperties defaults) {

		this.contextName = contextName;

		connectionProps = getProperties("jdbc", defaults.connectionProps);
		sessionProps = getProperties("mail", defaults.sessionProps);
		ftpProps = getProperties("ftp", defaults.ftpProps);
		pathProps = getProperties("path", defaults.pathProps);
		logProps = getProperties("log", defaults.logProps);

		String[] paths = getPaths(pathProps);
		dataPath = paths[0];
		processPath = paths[1];
		logPath = paths[2];
	}

	public Context createContext(String processId) {
		return createContext(processId, null);
	}

	public Context createContext(String processId, Context parentContext) {

		Context context = null;

		try {
			// TODO: Make loader type configurable through properties.

			Loader loader = (parentContext == null) ? new FileLoader(processPath) : parentContext.loader;
			
			context = new Context(connectionProps, sessionProps, ftpProps, dataPath, loader);
			
			context.logger = (parentContext == null) ? setupLog(processId, context) : parentContext.logger.nestProcess(processId);
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
	 * If this ContextProperties object was created with the constructor that
	 * specifies defaults, the defaults are ignored by this function.
	 * 
	 * @param usage is the usage for the properties
	 * @return the properties
	 */
	public Properties getProperties(String usage) {
		return getProperties(usage, null);
	}

	private Properties getProperties(String usage, Properties defaults) {

		Properties props = (defaults != null) ? new Properties(defaults) : new Properties();

		String fileName = contextName + "." + usage + ".properties";
		Path path = Files.getPath(dbpaHome, fileName); 

		try {
			FileInputStream in = new FileInputStream(path.toString());
			props.load(in);
			in.close();
		}
		catch (Exception ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error reading " + usage + " properties from file \"" + fileName + "\": " + message, ex);
		}

		return props;
	}

	private Properties getDefaultPathsProperties() {

		Properties props = new Properties();
		props.setProperty("data", ".");
		props.setProperty("process", ".");
		props.setProperty("log", ".");
		return props;
	}

	private String[] getPaths(Properties pathProps) {

		if (pathProps == null) {
			// Defensive: This path should never execute because of how ContextProperties() default constructor is defined.
			pathProps = getDefaultPathsProperties();
		}

		String dataPath = pathProps.getProperty("data");
		String processPath = pathProps.getProperty("process");
		String logPath = pathProps.getProperty("log");

		return new String[] { dataPath, processPath, logPath };
	}

	private Logger setupLog(String processID, Context context) {
		
		String logTypeList = (logProps != null) ? logProps.getProperty("type") : null; 
		if ((logTypeList == null) || logTypeList.equals("null")) {
			return NullLogger.logger;
		}

		try {
			String loggerLevelName = (logProps != null) ? logProps.getProperty("level") : null;
			Logger.Level level = (loggerLevelName != null) ? Logger.Level.valueOf(loggerLevelName) : Logger.Level.values()[0];
			
			RootLogger log = new RootLogger(processID, level);

			String[] logTypes = logTypeList.split(",");
			for (String logType : logTypes) {
				if (logType.equals("null")) {
					// Do nothing.
				}
				else if (logType.equals("console")) {
					log.add(new ConsoleAppender());
				}
				else if (logType.equals("file")) {
					String fileName = Files.getPath(logPath, logProps.getProperty("fileName")).toString();
					String rolloverSchedule = logProps.getProperty("fileRollover");

					log.add(new FileAppender(fileName, rolloverSchedule));
				}
				else if (logType.equals("table")) {
					log.add(new TableAppender(context, logProps.getProperty("tableName")));
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
	
	public Path getProcessPath() {
		return Files.getPath(processPath);
	}
}
