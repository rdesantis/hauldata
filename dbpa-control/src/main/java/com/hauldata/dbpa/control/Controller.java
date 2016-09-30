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
import java.util.NoSuchElementException;
import java.util.Properties;

import javax.naming.NameNotFoundException;
import javax.naming.NamingException;

import com.hauldata.dbpa.control.api.ProcessConfiguration;
import com.hauldata.dbpa.control.api.ProcessRun;
import com.hauldata.dbpa.control.api.ScriptValidation;
import com.hauldata.dbpa.control.api.ProcessRun.Status;
import com.hauldata.dbpa.control.api.ScriptArgument;
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
	 * or an empty list if no scripts exist
	 * @throws Exception if an error occurred
	 */
	public List<String> listScripts() throws Exception {

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
			throw ex;
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
	 * @throws Exception if validation fails for a reason other than bad syntax
 	 */
	public ScriptValidation validateScript(String name) throws Exception {

		DbProcess process = null;
		String validationMessage = null;
		List<VariableBase> parameters = null;

		try {
			process = context.loader.load(name);
			parameters = process.getParameters();
		}
		catch (/*InputMismatchException |*/ NoSuchElementException | /*NameNotFoundException | NameAlreadyBoundException |*/ NamingException ex) {
			// See TaskSetParser.parseTasks() for parse exceptions.
			// TODO: Define a SyntaxError exception that collects all syntax errors
			validationMessage = ex.getMessage();
		}
		catch (Exception ex) {
			throw ex;
		}

		return new ScriptValidation(process != null, validationMessage, parameters);
	}

	public void deleteScript(String name) throws Exception {
		//TODO
	}

	public List<String> listPropertiesFiles() throws Exception {
		//TODO
		return null;
	}

	public void deletePropertiesFile(String name) throws Exception {
		//TODO
	}

	/**
	 * Create the set of database tables used to store process configurations
	 * and run results.
	 * @throws Exception if schema creation fails for any reason
	 */
	public void createSchema() throws Exception {

		Connection conn = null;
		Statement stmt = null;

		try {
			// Create the tables in the schema.

			conn = context.getConnection(null);

			String createConfigTable = String.format(createConfigTableSql, configTableName);
			String createArgTable = String.format(createArgTableSql, argTableName);
			String createRunTable = String.format(createRunTableSql, runTableName);

			stmt = conn.createStatement();

			stmt.executeUpdate(createConfigTable);
		    stmt.executeUpdate(createArgTable);
		    stmt.executeUpdate(createRunTable);
		}
		catch (Exception ex) {
			throw ex;
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (Exception ex) {}

			if (conn != null) context.releaseConnection(null);
		}
	}

	/**
	 * Store a configuration in the database.
	 * 
	 * @param config is the configuration to store.
	 * @throws Exception if the configuration cannot be stored for any reason
	 */
	public void storeConfiguration(ProcessConfiguration config)throws Exception {

		Connection conn = null;
		PreparedStatement stmt = null;

		try {
			conn = context.getConnection(null);

			String insertConfig = String.format(insertConfigSql, configTableName);

			stmt = conn.prepareStatement(insertConfig);

			stmt.setString(2, config.getProcessName());
			stmt.setString(3, config.getScriptName());
			stmt.setString(4, config.getPropName());

			config.setId(-1);
			synchronized (this) {

				int nextConfigId = getNextConfigId();
				config.setId(nextConfigId);

				stmt.setInt(1, nextConfigId);

				stmt.executeUpdate();
			}

			stmt.close();
			stmt = null;

			String insertArg = String.format(insertArgSql, argTableName);

			stmt = conn.prepareStatement(insertArg);

			int argIndex = 1;
			for (ScriptArgument argument : config.getArguments()) {

				stmt.setInt(1, config.getId());
				stmt.setInt(2, argIndex++);
				stmt.setString(3, argument.getName());
				stmt.setString(4, argument.getValue());

				stmt.addBatch();
			}

			stmt.executeBatch();
		}
		catch (Exception ex) {
			throw ex;
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection(null);
		}
	}

	private int getNextConfigId() throws Exception {
		
		int nextConfigId = -1;

		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;

		try {
			conn = context.getConnection(null);

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

			if (conn != null) context.releaseConnection(null);
		}

		return nextConfigId;
	}

	/**
	 * Retrieve all configurations from the database
	 *
	 * @return the list of configurations if any exists;
	 * or an empty list if no configuration exist
	 * @throws Exception if any error occurs
	 */
	public List<ProcessConfiguration> listConfigurations() throws Exception {

		return getConfigurations(null);
	}

	/**
	 * Load a configuration from the database
	 *
	 * @param name is the name of the configuration to load
	 * @return the configuration if it exists, which will have a positive
	 * id member; or, null if the configuration does not exist
	 * @throws Exception if any error occurs
	 */
	public ProcessConfiguration loadConfiguration(String name) throws Exception {

		return getConfiguration(name);
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
			conn = context.getConnection(null);

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

				List<ScriptArgument> arguments = getArguments(id);

				configs.add(new ProcessConfiguration(id, configName, scriptName, propName, arguments));
			}
		}
		catch (Exception ex) {
			throw ex;
		}
		finally {
			try { if (rs != null) rs.close(); } catch (Exception exx) {}
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection(null);
		}

		return configs;
	}

	private List<ScriptArgument> getArguments(int configId) throws Exception {

		List<ScriptArgument> arguments = new LinkedList<ScriptArgument>();

		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			conn = context.getConnection(null);

			String selectArgs = String.format(selectArgsSql, argTableName);

			stmt = conn.prepareStatement(selectArgs, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			stmt.setInt(1, configId);

			rs = stmt.executeQuery();

			while (rs.next()) {
				
				String argName = rs.getString(1);
				String argValue = rs.getString(2);

				arguments.add(new ScriptArgument(argName, argValue));
			}
		}
		catch (Exception ex) {
			throw ex;
		}
		finally {
			try { if (rs != null) rs.close(); } catch (Exception exx) {}
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection(null);
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

	/**
	 * Delete a configuration
	 *
	 * @param name is the name of the configuration to delete
	 * @throws NameNotFoundException if the configuration does not exist 
	 * @throws Exception if any other error occurs
	 */
	public void deleteConfiguration(String name) throws Exception {

		Connection conn = null;
		PreparedStatement stmt = null;

		try {
			ProcessConfiguration config = getConfiguration(name);

			if (config == null) {
				throw new NameNotFoundException("Configuration not found");
			}

			conn = context.getConnection(null);

			String deleteArgs = String.format(deleteArgsSql, argTableName);

			stmt = conn.prepareStatement(deleteArgs);

			stmt.setInt(1, config.getId());

			stmt.executeUpdate();

			stmt.close();
			stmt = null;

			String deleteConfig = String.format(deleteConfigSql, configTableName);

			stmt = conn.prepareStatement(deleteConfig);

			stmt.setInt(1, config.getId());

			stmt.executeUpdate();
		}
		catch (Exception ex) {
			throw ex;
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection(null);
		}
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
	 * @throws RuntimeException if the controller is already started
	 * @throws Exception if any error occurs
	 */
	public void startup() throws Exception{

		if (isStarted()) {
			throw new RuntimeException("Controller is already started.");
		}
		
		// Instantiate a ProcessExecutor and create a process monitor thread
		// that loops calling executor.getCompleted() to update the database
		// with process completion status.

		executor = new ProcessExecutor();
		monitorThread = new Thread(new ProcessMonitor()); 
		monitorThread.start();
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
	 * @return the run object, which will all have a positive configId value
	 * @throws Exception if any error occurs
	 */
	public ProcessRun run(String configName) throws Exception {

		if (!isStarted()) {
			throw new RuntimeException("Must startup controller before running processes.");
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
				throw new NameNotFoundException("Process configuration does not exist");
			}

			process = context.loader.load(config.getScriptName());

			args = new String[config.getArguments().size()];
			int i = 0;
			for (ScriptArgument argument : config.getArguments()) {
				args[i++] = argument.getValue(); 
			}

			ContextProperties props =
					(config.getPropName() == null) ? contextProps :
					new ContextProperties(config.getPropName(), contextProps);  

			configContext = props.createContext(configName, context);

			run = new ProcessRun(configName, config.getId());

			putRun(run);

			executor.submit(run, process, args, configContext);

			updateRun(run);
		}
		catch (Exception ex) {
			throw ex;
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
			conn = context.getConnection(null);

			String insertRun = String.format(insertRunSql, runTableName);

			stmt = conn.prepareStatement(insertRun);

			stmt.setInt(1, run.getConfigId());
			stmt.setInt(3, run.getStatus().value());

			run.setRunIndex(-1);
			synchronized (this) {

				int nextRunIndex = getNextRunIndex(run.getConfigId());
				run.setRunIndex(nextRunIndex);

				stmt.setInt(2, nextRunIndex);

				stmt.executeUpdate();
			}
		}
		catch (Exception ex) {
			throw ex;
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection(null);
		}
	}

	private int getNextRunIndex(int configId) throws Exception {
		
		int nextRunIndex = -1;

		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			String selectLastRunIndex = String.format(selectLastRunIndexSql, runTableName);

			conn = context.getConnection(null);

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

			if (conn != null) context.releaseConnection(null);
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
			conn = context.getConnection(null);

			String updateRun = String.format(updateRunSql, runTableName);

			stmt = conn.prepareStatement(updateRun);

			stmt.setInt(1, run.getStatus().value());
			setTimestamp(stmt, 2, run.getStartTime());
			setTimestamp(stmt, 3, run.getEndTime());

			stmt.setInt(4, run.getConfigId());
			stmt.setInt(5, run.getRunIndex());

			stmt.executeUpdate();
		}
		catch (Exception ex) {
			throw ex;
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection(null);
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
	 * @return true if the run was stopped; false otherwise
	 * @throws Exception if any error occurs
	 */
	public boolean stop(ProcessRun run) throws Exception {

		if (!isStarted()) {
			throw new RuntimeException("Controller is not started; no processes are running.");
		}

		return executor.stop(run);
	}

	/**
	 * Retrieve a list of currently running processes
	 * @return the list of currently running processes
	 * @throws Exception if any error occurs
	 */
	public List<ProcessRun> listRunning() throws Exception {

		if (!isStarted()) {
			throw new RuntimeException("Controller is not started; no processes are running.");
		}

		return executor.getRunning();
	}
	
	/**
	 * Retrieve a list of process runs from the database.
	 * 
	 * @param configName is the name of a single process configuration or null to get runs for all configurations.
	 * @param latest is true to only get the latest run for each configuration; otherwise, all runs are retrieved.
	 * @return the list of runs, which will all have a positive configId value
	 * @throws Exception if any error occurs
	 */
	public List<ProcessRun> listRuns(String configName, boolean latest) throws Exception {

		List<ProcessRun> runs = new LinkedList<ProcessRun>();

		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			conn = context.getConnection(null);

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
			throw ex;
		}
		finally {
			try { if (rs != null) rs.close(); } catch (Exception ex) {}
			try { if (stmt != null) stmt.close(); } catch (Exception ex) {}

			if (conn != null) context.releaseConnection(null);
		}

		return runs;
	}

	private LocalDateTime getLocalDateTime(ResultSet rs, int columnIndex) throws SQLException {
		
		Timestamp timestamp = rs.getTimestamp(columnIndex);
		return (timestamp != null) ? timestamp.toLocalDateTime() : null;
	}

	/**
	 * Stop all processes currently running.
	 * @throws Exception if any error occurs
	 */
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
	 * @throws Exception if any error occurs
	 */
	public void shutdown() throws Exception {

		if (!isStarted()) {
			throw new RuntimeException("Controller was not started.");
		}

		// Interrupt the process monitor thread to terminate it,
		// stop any running processes, and shut down the executor.

		try {
			long waitMillis = 10000;
	
			monitorThread.interrupt();
			monitorThread.join(waitMillis);

			stopAll();

			executor.close();
		}
		catch (Exception ex) {
			throw ex;
		}

		executor = null;
	}

	/**
	 * Release any resources being used by this controller.
	 */
	public void close() {

		try { if (isStarted()) shutdown(); } catch (Exception ex) {}

		try { if (context != null) context.close(); } catch (Exception ex) {}
		
		controller = null;
	}
}
