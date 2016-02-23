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

package com.hauldata.dbpa.control;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.AbstractMap.SimpleEntry;

import com.hauldata.dbpa.control.ProcessRun.Status;
import com.hauldata.dbpa.loader.FileLoader;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.process.ContextProperties;
import com.hauldata.dbpa.process.DbProcess;
import com.hauldata.dbpa.variable.VariableBase;

public class Controller {

//	private static final String programName = "ControlDbp";
	private static final String programName = "RunDbp";		// To simplify testing

	private static final String configTableSuffix = "Config";
	private final static String createConfigTableSql = "CREATE TABLE %1$s " +
			"( id INTEGER, name VARCHAR(255) UNIQUE, scriptName VARCHAR(255), propName VARCHAR(255), PRIMARY KEY (id) )";
	private final static String insertConfigSql = "INSERT INTO %1$s VALUES (?,?,?,?)";
	private final static String selectLastConfigIdSql = "SELECT MAX(id) FROM %1$s";
	private final static String selectConfigSql = "SELECT id, name, scriptName, propName FROM %1$s";
	private final static String whereNameSql = " WHERE name = ?";
	private final static String orderByNameSql = " ORDER BY name";
	private final static String deleteConfigSql = "DELETE FROM %1$s WHERE id = ?";

	private static final String argTableSuffix = "ConfigArgument";
	private final static String createArgTableSql = "CREATE TABLE %1$s " +
			"( configId INTEGER, argIndex INTEGER, argName VARCHAR(255), argValue VARCHAR(255), PRIMARY KEY (configId, argIndex) )";
	private final static String insertArgSql = "INSERT INTO %1$s VALUES (?,?,?,?)";
	private final static String selectArgsSql = "SELECT argName, argValue FROM %1$s WHERE configId = ? ORDER BY argIndex";
	private final static String deleteArgsSql = "DELETE FROM %1$s WHERE configId = ?";

	private static final String runTableSuffix = "ConfigRun";
	private final static String createRunTableSql = "CREATE TABLE %1$s " +
			"( configId INTEGER, runIndex INTEGER, status TINYINT, startTime DATETIME, endTime DATETIME, PRIMARY KEY (configId, runIndex) )";
	private final static String insertRunSql = "INSERT INTO %1$s (configId, runIndex, status) VALUES (?,?,?)";
	private final static String selectLastRunIndexSql = "SELECT MAX(runIndex) FROM %1$s WHERE configId = ?";
	private final static String updateRunSql = "UPDATE %1$s SET status = ?, startTime = ?, endTime = ? WHERE configId = ? AND runIndex = ?";

	private final static String selectRunSql =	// runTableName, configTableName
			"SELECT c.name, c.id, cr.runIndex, cr.status, cr.startTime, cr.endTime " +
			"FROM %1$s AS cr INNER JOIN %2$s AS c ON c.id = cr.configId";
	private final static String selectAllLastRunIndexSql =	// runTableName
			"SELECT configId, MAX(runIndex) AS maxRunIndex FROM %1$s GROUP BY configId";
	private final static String selectLastRunSql =	// selectRun, selectAllLastRunIndex
			"%1$s INNER JOIN (%2$s) AS mr ON mr.configId = cr.configId AND cr.runIndex = mr.maxRunIndex";
	private final static String whereConfigNameSql =
			" WHERE c.name = ?";

	private static Controller controller = null;
	
	private ContextProperties contextProps;
	private Context context;

	private String configTableName;
	private String argTableName;
	private String runTableName;
	
	private ProcessExecutor executor;
	private Thread monitorThread;

	/**
	 * Get the singleton DBPA process controller instance
	 * @return the controller
	 */
	public static Controller getInstance() {
		if (controller == null) {
			controller = new Controller();
		}
		return controller;
	}

	private Controller() {

		contextProps = new ContextProperties(programName);
		context = contextProps.createContext(null);
		
		// Get schema specifications.

		Properties schemaProps = contextProps.getProperties("schema");
		if (schemaProps == null) {
			throw new RuntimeException("Schema properties not found");
		}

		String tablePrefix = schemaProps.getProperty("tablePrefix");
		if (tablePrefix == null) {
			throw new RuntimeException("Schema tablePrefix property not found");
		}

		configTableName = tablePrefix + configTableSuffix;
		argTableName = tablePrefix + argTableSuffix;
		runTableName = tablePrefix + runTableSuffix;

		executor = null;
	}

	/**
	 * Retrieve a list of all scripts available in the process directory.
	 *
	 * @return the list of scripts if any exists;
	 * or an empty list if no scripts exist;
	 * or null if an error occurred.
	 */
	public List<String> listScripts() {

		final String scriptSuffix = "." + FileLoader.processFileExt;
		final int scriptSuffixLength = scriptSuffix.length();

		List<String> scriptNames = new LinkedList<String>();

		DirectoryStream<Path> scriptPaths = null;
		try {
			scriptPaths = Files.newDirectoryStream(contextProps.getProcessPath(), "*" + scriptSuffix);

			for (Path scriptPath : scriptPaths) {
				if (Files.isRegularFile(scriptPath)) {

					String scriptFileName = scriptPath.getFileName().toString();
					String scriptName = scriptFileName.substring(0, scriptFileName.length() - scriptSuffixLength);

					scriptNames.add(scriptName);
				}
			}
		}
		catch (Exception ex) {
			scriptNames = null;
		}
		finally {
			try { scriptPaths.close(); } catch (Exception ex) {}
		}

		return scriptNames;
	}

	/**
	 * Read a script and validate it for syntax.
	 * 
	 * @return the validation results.  Fields are:
	 * 
	 *	isValid returns true if the script is valid syntactically, other false;
	 *	validationMessage returns an error message if the validation failed, or null if it succeeded;
	 * 	parameters returns the list of parameters that can be passed to the script.
 	 */
	public ScriptValidation validateScript(String name) {

		DbProcess process = null;
		String validationMessage = null;
		List<VariableBase> parameters = null;

		try {
			process = context.loader.load(name);
			parameters = process.getParameters();
		}
		catch (Exception ex) {
			validationMessage = ex.getMessage();
		}

		return new ScriptValidation(process != null, validationMessage, parameters);
	}

	public String deleteScript(String name) {
		//TODO
		return null;
	}

	public List<String> listPropertiesFiles() {
		//TODO
		return null;
	}

	public String deletePropertiesFile(String name) {
		//TODO
		return null;
	}

	/**
	 * Create the set of database tables used to store process configurations
	 * and run results.
	 * 
	 * @return an error message if the operation failed, or null if it succeeded. 
	 */
	public String createSchema() {

		String message = null;

		Connection conn = null;
		Statement stmt = null;

		try {
			// Create the tables in the schema.

			conn = context.getConnection();

			String createConfigTable = String.format(createConfigTableSql, configTableName);
			String createArgTable = String.format(createArgTableSql, argTableName);
			String createRunTable = String.format(createRunTableSql, runTableName);

			stmt = conn.createStatement();

			stmt.executeUpdate(createConfigTable);
		    stmt.executeUpdate(createArgTable);
		    stmt.executeUpdate(createRunTable);
		}
		catch (Exception ex) {
			message = ex.getMessage();
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection();
		}

		return message;
	}

	/**
	 * Store a configuration in the database.
	 * 
	 * @param config is the configuration to store.
	 * @return an error message if the operation failed, or null if it succeeded. 
	 */
	public String storeConfiguration(ProcessConfiguration config) {

		String message = null;

		Connection conn = null;
		PreparedStatement stmt = null;

		try {
			conn = context.getConnection();

			String insertConfig = String.format(insertConfigSql, configTableName);

			stmt = conn.prepareStatement(insertConfig);

			stmt.setString(2, config.processName);
			stmt.setString(3, config.scriptName);
			stmt.setString(4, config.propName);

			config.id = -1;
			synchronized (this) {

				config.id = getNextConfigId();
				stmt.setInt(1, config.id);

				stmt.executeUpdate();
			}

			stmt.close();
			stmt = null;

			String insertArg = String.format(insertArgSql, argTableName);

			stmt = conn.prepareStatement(insertArg);

			int argIndex = 1;
			for (SimpleEntry<String, String> argument : config.arguments) {

				stmt.setInt(1, config.id);
				stmt.setInt(2, argIndex++);
				stmt.setString(3, argument.getKey());
				stmt.setString(4, argument.getValue());

				stmt.addBatch();
			}

			stmt.executeBatch();
		}
		catch (Exception ex) {
			message = ex.getMessage();
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection();
		}

		return message;
	}

	private int getNextConfigId() throws Exception {
		
		int nextConfigId = -1;

		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;

		try {
			conn = context.getConnection();

			String selectLastConfigId = String.format(selectLastConfigIdSql, configTableName);

			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			rs = stmt.executeQuery(selectLastConfigId);

			rs.next();

			nextConfigId = rs.getInt(1) + 1;
		}
		catch (Exception ex) {
			throw ex;
		}
		finally {
			try { if (rs != null) rs.close(); } catch (Exception exx) {}
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection();
		}

		return nextConfigId;
	}

	/**
	 * Retrieve all configurations from the database
	 *
	 * @return the list of configurations if any exists;
	 * or an empty list if no configuration exist;
	 * or if an error occurred, error information appears in the final,
	 * possibly the only, list element formatted as in loadConfiguration(String)
	 */
	public List<ProcessConfiguration> listConfigurations() {

		List<ProcessConfiguration> configs;

		try {
			configs = getConfigurations(null);
		}
		catch (Exception ex) {
			configs = messageAsConfigurations(ex.getLocalizedMessage());
		}

		return configs;
	}

	private List<ProcessConfiguration> messageAsConfigurations(String message) {

		List<ProcessConfiguration> configs = new LinkedList<ProcessConfiguration>();
		configs.add(messageAsConfiguration(message));
		return configs;
	}

	/**
	 * Load a configuration from the database
	 *
	 * @param name is the name of the configuration to load
	 * @return the configuration if it exists, which will have a positive
	 * id member; or, null if the configuration does not exist;
	 * or if an error occurred, error information formatted as a configuration
	 * as follows:
	 * 	id			is a non-positive integer,
	 * 	processName	is the detailed error message. 
	 */
	public ProcessConfiguration loadConfiguration(String name) {

		ProcessConfiguration config;

		try {
			config = getConfiguration(name);
		}
		catch (Exception ex) {
			config = messageAsConfiguration(ex.getLocalizedMessage());
		}

		return config;
	}

	private ProcessConfiguration messageAsConfiguration(String message) {
		return new ProcessConfiguration(-1, message, null, null, null);
	}

	/**
	 * Retrieve a set of configuration(s) from the database.
	 * 
	 * @param name is the name of a single configuration or null to get all configurations
	 * @return a list of configuration objects retrieved from the database
	 * @throws Exception if an error occurs 
	 */
	private List<ProcessConfiguration> getConfigurations(String name) throws Exception {

		List<ProcessConfiguration> configs = new LinkedList<ProcessConfiguration>();

		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			conn = context.getConnection();

			String selectConfig = String.format(selectConfigSql, configTableName);
			
			if (name != null) {
				selectConfig += whereNameSql;
			}
			else {
				selectConfig += orderByNameSql;
			}

			stmt = conn.prepareStatement(selectConfig, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			if (name != null) {
				stmt.setString(1, name);
			}

			rs = stmt.executeQuery();

			while (rs.next()) {
				
				int id = rs.getInt(1);
				String configName = rs.getString(2);
				String scriptName = rs.getString(3);
				String propName = rs.getString(4);
				if ((propName != null) && propName.isEmpty()) {
					propName = null;
				}

				List<SimpleEntry<String, String>> arguments = getArguments(id);

				configs.add(new ProcessConfiguration(id, configName, scriptName, propName, arguments));
			}
		}
		catch (Exception ex) {
			throw ex;
		}
		finally {
			try { if (rs != null) rs.close(); } catch (Exception exx) {}
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection();
		}

		return configs;
	}

	private List<SimpleEntry<String, String>> getArguments(int configId) throws Exception {

		List<SimpleEntry<String, String>> arguments = new LinkedList<SimpleEntry<String, String>>();

		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			conn = context.getConnection();

			String selectArgs = String.format(selectArgsSql, argTableName);

			stmt = conn.prepareStatement(selectArgs, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			stmt.setInt(1, configId);

			rs = stmt.executeQuery();

			while (rs.next()) {
				
				String argName = rs.getString(1);
				String argValue = rs.getString(2);

				arguments.add(new SimpleEntry<String, String>(argName, argValue));
			}
		}
		catch (Exception ex) {
			throw ex;
		}
		finally {
			try { if (rs != null) rs.close(); } catch (Exception exx) {}
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection();
		}

		return arguments;
	}

	/**
	 * Return a configuration
	 *
	 * @param name is the name of the configuration
	 * @return the configuration if it exists or null if it does not
	 * @throws Exception if an error occurs 
	 */
	private ProcessConfiguration getConfiguration(String name) throws Exception {

		List<ProcessConfiguration> configs = null;

		configs = getConfigurations(name);

		return ((configs != null) && (0 < configs.size())) ? configs.get(0) : null;
	}

	public String deleteConfiguration(String name) {

		String message = null;

		Connection conn = null;
		PreparedStatement stmt = null;

		try {
			ProcessConfiguration config = getConfiguration(name);

			if (config == null) {
				return "Configuration not found";
			}

			conn = context.getConnection();

			String deleteArgs = String.format(deleteArgsSql, argTableName);

			stmt = conn.prepareStatement(deleteArgs);

			stmt.setInt(1, config.id);

			stmt.executeUpdate();

			stmt.close();
			stmt = null;

			String deleteConfig = String.format(deleteConfigSql, configTableName);

			stmt = conn.prepareStatement(deleteConfig);

			stmt.setInt(1, config.id);

			stmt.executeUpdate();
		}
		catch (Exception ex) {
			message = ex.getMessage();
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection();
		}

		return message;
	}

	/**
	 * Retrieve a list of configuration runs from the database.
	 * 
	 * @param configName is the name of a single configuration or null to get runs for all configurations.
	 * @param latest is true to only get the latest run for each configuration; otherwise, all runs are retrieved.
	 * @return the list of runs, which will all have a positive configId value;
	 * or if an error occurred, error information will appear in the final, possibly the only,
	 * list element as follows:
	 * 	configId	is a non-positive integer,
	 * 	name		is the detailed error message. 
	 */
	public List<ProcessRun> listRuns(
			String configName,
			boolean latest) {

		List<ProcessRun> runs = new LinkedList<ProcessRun>();

		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			conn = context.getConnection();

			String selectRun = String.format(selectRunSql, runTableName, configTableName);

			if (latest) {
				String selectAllLastRunIndex = String.format(selectAllLastRunIndexSql, runTableName);

				String selectLastRun = String.format(selectLastRunSql, selectRun, selectAllLastRunIndex);
				
				selectRun = selectLastRun;
			}
			
			if (configName != null) {
				selectRun += whereConfigNameSql;
			}

			stmt = conn.prepareStatement(selectRun, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			if (configName != null) {
				stmt.setString(1, configName);
			}

			rs = stmt.executeQuery();

			while (rs.next()) {
				
				String name = rs.getString(1);
				int configId = rs.getInt(2);
				int runIndex = rs.getInt(3);
				Status status = Status.of(rs.getInt(4));
				LocalDateTime startTime = getLocalDateTime(rs, 5);
				LocalDateTime endTime = getLocalDateTime(rs, 6);

				runs.add(new ProcessRun(name, configId, runIndex, status, startTime, endTime));
			}
		}
		catch (Exception ex) {
			runs.add(messageAsRun(ex.getLocalizedMessage()));
		}
		finally {
			try { if (rs != null) rs.close(); } catch (Exception exx) {}
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection();
		}

		return runs;
	}

	private LocalDateTime getLocalDateTime(ResultSet rs, int columnIndex) throws SQLException {
		
		Timestamp timestamp = rs.getTimestamp(columnIndex);
		return (timestamp != null) ? timestamp.toLocalDateTime() : null;
	}

	private ProcessRun messageAsRun(String message) {
		return new ProcessRun(message, -1, null, null, null, null);
	}

	/**
	 * @return true if the controller has been started to run processes. 
	 */
	public boolean isStarted() {
		return executor != null;
	}

	/**
	 * Start the controller so that it can subsequently run and stop processes.
	 * 
	 * @return an error message if startup failed or null if it succeeded.
	 */
	public String startup() {

		if (isStarted()) {
			return "Controller is already started.";
		}
		
		// Instantiate a ProcessExecutor and create a process monitor thread
		// that loops calling executor.getCompleted() to update the database
		// with process completion status.

		executor = new ProcessExecutor();
		monitorThread = new Thread(new ProcessMonitor()); 
		monitorThread.start();

		return null;
	}

	private class ProcessMonitor implements Runnable {

		/**
		 * Monitor the completion of processes and update database status.
		 */
		@Override
		public void run() {
			
			try {
				while (!Thread.interrupted()) {
					ProcessRun result = executor.getCompleted();
					try {
						updateRun(result);
					}
					catch (InterruptedException iex) {
						break;
					}
					catch (Exception ex) {
						// Nothing to be done about SQL exceptions and such.
						// Want to keep running even though the run table may not be getting updated.
					}
				}
			}
			catch (InterruptedException iex) {
				// InterruptedException terminates the loop and ends the thread as desired.
			}
		}
	}

	/**
	 * Start a process from a configuration.
	 * 
	 * @return the run object, which will all have a positive configId value;
	 * or if an error occurred, error information will appear in the object as follows:
	 * 	configId	is a non-positive integer,
	 * 	name		is the detailed error message. 
	 */
	public ProcessRun run(String configName) {

		if (!isStarted()) {
			return messageAsRun("Must startup controller before running processes.");
		}

		// Instantiate the process, arguments, and context,
		// submit the process to the executor,
		// and update the database with run status.

		DbProcess process;
		String[] args;
		Context configContext;
		ProcessRun run;

		try {
			ProcessConfiguration config = getConfiguration(configName);
			if (config == null) {
				return messageAsRun("Process configuration does not exist");
			}

			process = context.loader.load(config.scriptName);

			args = new String[config.arguments.size()];
			int i = 0;
			for (SimpleEntry<String, String> argument : config.arguments) {
				args[i++] = argument.getValue(); 
			}

			ContextProperties props =
					(config.propName == null) ? contextProps :
					new ContextProperties(config.propName, contextProps);  

			configContext = props.createContext(configName, context);

			run = new ProcessRun(configName, config.id);

			putRun(run);

			executor.submit(run, process, args, configContext);

			updateRun(run);
		}
		catch (Exception ex) {
			return messageAsRun(ex.getMessage());
		}

		return run;
	}

	/**
	 * Store a process run row in the database.
	 */
	private void putRun(ProcessRun run) throws Exception {

		Connection conn = null;
		PreparedStatement stmt = null;

		try {
			conn = context.getConnection();

			String insertRun = String.format(insertRunSql, runTableName);

			stmt = conn.prepareStatement(insertRun);

			stmt.setInt(1, run.configId);
			stmt.setInt(3, run.status.value());

			run.runIndex = -1;
			synchronized (this) {

				run.runIndex = getNextRunIndex(run.configId);
				stmt.setInt(2, run.runIndex);

				stmt.executeUpdate();
			}
		}
		catch (Exception ex) {
			throw ex;
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection();
		}
	}

	private int getNextRunIndex(int configId) throws Exception {
		
		int nextRunIndex = -1;

		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			String selectLastRunIndex = String.format(selectLastRunIndexSql, runTableName);

			conn = context.getConnection();

			stmt = conn.prepareStatement(selectLastRunIndex);

			stmt.setInt(1, configId);

			rs = stmt.executeQuery();

			rs.next();

			nextRunIndex = rs.getInt(1) + 1;
		}
		catch (Exception ex) {
			throw ex;
		}
		finally {
			try { if (rs != null) rs.close(); } catch (Exception exx) {}
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection();
		}

		return nextRunIndex;
	}

	/**
	 * Update a process run row in the database.
	 */
	private void updateRun(ProcessRun run) throws Exception {

		Connection conn = null;
		PreparedStatement stmt = null;

		try {
			conn = context.getConnection();

			String updateRun = String.format(updateRunSql, runTableName);

			stmt = conn.prepareStatement(updateRun);

			stmt.setInt(1, run.status.value());
			setTimestamp(stmt, 2, run.startTime);
			setTimestamp(stmt, 3, run.endTime);

			stmt.setInt(4, run.configId);
			stmt.setInt(5, run.runIndex);

			stmt.executeUpdate();
		}
		catch (Exception ex) {
			throw ex;
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection();
		}
	}

	private void setTimestamp(PreparedStatement stmt, int parameterIndex, LocalDateTime time) throws SQLException {
		
		if (time != null) {
			stmt.setTimestamp(parameterIndex, Timestamp.valueOf(time));
		}
		else {
			stmt.setNull(parameterIndex, Types.TIMESTAMP);
		}
	}

	/**
	 * Stop a process run by interrupting its thread.
	 * 
	 * @param run is the process run to stop.
	 * @return an error message if stop failed or null if it succeeded.
	 */
	public String stop(ProcessRun run) {
		
		if (!isStarted()) {
			return "Controller is not started; no processes are running.";
		}

		boolean cancelled;
		try {
			cancelled = executor.stop(run);
		}
		catch (Exception ex) {
			return ex.getLocalizedMessage();
		}

		return cancelled ? null : "Could not stop the process";
	}

	private void stopAll() throws Exception  {
		
		executor.stopAll();

		while (!executor.allCompleted()) {
			ProcessRun result = executor.getCompleted();
			updateRun(result);
		}
	}

	/**
	 * Shutdown process control, stopping any running processes.
	 * The controller still retains resources needed for
	 * creating, listing, and validating entities. 
	 * @return
	 */
	public String shutdown() {

		if (!isStarted()) {
			return "Controller was not started.";
		}

		// Interrupt the process monitor thread to terminate it,
		// stop any running processes, and shut down the executor.

		String message = null;

		try {
			long waitMillis = 10000;
	
			monitorThread.interrupt();
			monitorThread.join(waitMillis);

			stopAll();

			executor.close();
		}
		catch (Exception ex) {
			message = ex.getLocalizedMessage();
		}

		executor = null;

		return message;
	}

	/**
	 * Release any resources being used by this controller.
	 */
	public void close() {

		if (isStarted()) shutdown();

		try { if (context != null) context.close(); } catch (Exception ex) {}
		
		controller = null;
	}
}
