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

package com.hauldata.dbpa.manage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.naming.NameNotFoundException;

import com.hauldata.dbpa.manage.api.Job;
import com.hauldata.dbpa.manage.api.JobRun;
import com.hauldata.dbpa.manage.api.ScriptArgument;
import com.hauldata.dbpa.manage.api.JobRun.Status;
import com.hauldata.dbpa.manage.sql.JobSql;
import com.hauldata.dbpa.manage.sql.ArgumentSql;
import com.hauldata.dbpa.manage.sql.CommonSql;
import com.hauldata.dbpa.manage.sql.ScheduleSql;
import com.hauldata.dbpa.manage.sql.JobScheduleSql;
import com.hauldata.dbpa.manage.sql.RunSql;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.process.ContextProperties;
import com.hauldata.dbpa.process.DbProcess;

public class JobManager {

//	private static final String programName = "ManageDbp";
	private static final String programName = "RunDbp";		// To simplify testing

	private static JobManager manager = new JobManager();
	
	private ContextProperties contextProps;
	private Context context;

	private JobSql jobSql;
	private ArgumentSql argumentSql;
	private ScheduleSql scheduleSql;
	private JobScheduleSql jobScheduleSql;
	private RunSql runSql;

	private JobExecutor executor;
	private Thread monitorThread;

	/**
	 * Get the singleton DBPA job manager instance
	 * @return the manager
	 */
	public static JobManager getInstance() {
		if (manager == null) {
			throw new RuntimeException("Manager is not available");
		}
		return manager;
	}

	public static void killInstance() {
		if (manager != null) {
			throw new RuntimeException("Manager is already unavailable");
		}
		manager = null;
	}

	private JobManager() {

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

		jobSql = new JobSql(tablePrefix);
		argumentSql = new ArgumentSql(tablePrefix);
		scheduleSql = new ScheduleSql(tablePrefix);
		jobScheduleSql = new JobScheduleSql(tablePrefix);
		runSql = new RunSql(tablePrefix);

		executor = null;
	}

	public Context getContext() {
		return context;
	}

	public JobSql getJobSql() {
		return jobSql;
	}
	public ArgumentSql getArgumentSql() {
		return argumentSql;
	}
	public ScheduleSql getScheduleSql() {
		return scheduleSql;
	}
	public JobScheduleSql getJobScheduleSql() {
		return jobScheduleSql;
	}
	public RunSql getRunSql() {
		return runSql;
	}

	/**
	 * Store a job in the database.
	 * 
	 * @param name is the job name.
	 * @param job is the job to store.
	 * @return the unique job ID created for the job.
	 * @throws Exception if the job cannot be stored for any reason
	 */
	public int putJob(String name, Job job)throws Exception {

		Connection conn = null;
		PreparedStatement stmt = null;

		try {
			conn = context.getConnection(null);

			// Validate the schedules.

			List<Integer> scheduleIds = getScheduleIds(conn, job.getScheduleNames());

			// Write the job header.

			String insertJob = jobSql.insert;

			stmt = conn.prepareStatement(insertJob);

			stmt.setString(2, name);
			stmt.setString(3, job.getScriptName());
			stmt.setString(4, job.getPropName());
			stmt.setInt(5, job.isEnabled() ? 1 : 0);

			int jobId;
			synchronized (this) {

				jobId = getNextJobId(conn);

				stmt.setInt(1, jobId);

				stmt.executeUpdate();
			}

			stmt.close();
			stmt = null;

			// Write the arguments.

			String insertArg = argumentSql.insert;

			stmt = conn.prepareStatement(insertArg);

			int argIndex = 1;
			for (ScriptArgument argument : job.getArguments()) {

				stmt.setInt(1, jobId);
				stmt.setInt(2, argIndex++);
				stmt.setString(3, argument.getName());
				stmt.setString(4, argument.getValue());

				stmt.addBatch();
			}

			stmt.executeBatch();

			stmt.close();
			stmt = null;

			// Write the schedules.

			String insertSchedule = jobScheduleSql.insert;

			stmt = conn.prepareStatement(insertSchedule);

			for (int scheduleId : scheduleIds) {

				stmt.setInt(1, jobId);
				stmt.setInt(2, scheduleId);

				stmt.addBatch();
			}

			stmt.executeBatch();

			return jobId;
		}
		catch (Exception ex) {
			throw ex;
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection(null);
		}
	}

	private List<Integer> getScheduleIds(Connection conn, List<String> names) throws Exception {

		List<Integer> ids = new LinkedList<Integer>();

		if (names.isEmpty()) {
			return ids;
		}

		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			Stream<String> distinctNames = names.stream().distinct();
			String paramList = distinctNames.map(name -> "?").collect(Collectors.joining(","));

			String selectIds = String.format(scheduleSql.selectIds, paramList);

			stmt = conn.prepareStatement(selectIds, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			int paramIndex = 1;
			Iterator<String> nameIterator = distinctNames.iterator();
			while (nameIterator.hasNext()) {

				stmt.setString(paramIndex++, nameIterator.next());
			}

			rs = stmt.executeQuery();

			while (rs.next()) {
				ids.add(rs.getInt(1));
			}

			if (ids.size() != distinctNames.count()) {
				throw new RuntimeException("Some schedule names not found");
			}
		}
		catch (Exception ex) {
			throw ex;
		}
		finally {
			try { if (rs != null) rs.close(); } catch (Exception exx) {}
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}
		}

		return ids;
	}

	private int getNextJobId(Connection conn) throws Exception {
		return CommonSql.getNextId(conn, jobSql.selectLastId);
	}

	/**
	 * Retrieve a set of job(s) from the database
	 * whose names match an optional SQL wildcard pattern
	 * 
	 * @param likeName is the name wildcard pattern or null to get all jobs
	 * @return a map of job names to job objects retrieved from the database
	 * or an empty map if no job with a matching name is found
	 * @throws Exception if an error occurs 
	 */
	public Map<String, Job> getJobs(String likeName) throws Exception {

		if (likeName == null) {
			likeName = "%";
		}

		Map<String, Job> jobs = new HashMap<String, Job>();

		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			conn = context.getConnection(null);

			String selectJob = jobSql.select;
			
			stmt = conn.prepareStatement(selectJob, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			stmt.setString(1, likeName);

			rs = stmt.executeQuery();

			while (rs.next()) {
				
				int id = rs.getInt(1);
				String jobName = rs.getString(2);
				String scriptName = rs.getString(3);
				String propName = rs.getString(4);
				if ((propName != null) && propName.isEmpty()) {
					propName = null;
				}
				boolean enabled = (rs.getInt(5) == 1);

				List<ScriptArgument> arguments = getArguments(conn, id);

				List<String> scheduleNames = getJobScheduleNames(conn, id);

				jobs.put(jobName, new Job(scriptName, propName, arguments, scheduleNames, enabled));
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

		return jobs;
	}

	private List<ScriptArgument> getArguments(Connection conn, int jobId) throws Exception {

		List<ScriptArgument> arguments = new LinkedList<ScriptArgument>();

		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			String selectArgs = argumentSql.select;

			stmt = conn.prepareStatement(selectArgs, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			stmt.setInt(1, jobId);

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
		}

		return arguments;
	}

	private List<String> getJobScheduleNames(Connection conn, int jobId) throws Exception {

		List<String> names = new LinkedList<String>();

		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			String selectNames = jobScheduleSql.selectScheduleNamesByJobId;

			stmt = conn.prepareStatement(selectNames, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			stmt.setInt(1, jobId);

			rs = stmt.executeQuery();

			while (rs.next()) {

				names.add(rs.getString(1));
			}
		}
		catch (Exception ex) {
			throw ex;
		}
		finally {
			try { if (rs != null) rs.close(); } catch (Exception exx) {}
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}
		}

		return names;
	}

	/**
	 * Return a job
	 *
	 * @param name is the name of the job
	 * @return the job if it exists or null if it does not
	 * @throws Exception if an error occurs 
	 */
	public Job getJob(String name) throws Exception {

		Map<String, Job> jobs = getJobs(name);

		return (jobs.size() == 1) ? jobs.get(0) : null;
	}

	/**
	 * Retrieve a set of job name(s) from the database
	 * whose names match an optional SQL wildcard pattern
	 *
	 * @param likeName is the name wildcard pattern or null to get all job names
	 * @return a list of job names or an empty list if no job with a matching name is found
	 * @throws Exception if an error occurs
	 */
	public List<String> getJobNames(String likeName) throws Exception {

		if (likeName == null) {
			likeName = "%";
		}

		List<String> names = new LinkedList<String>();

		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			conn = context.getConnection(null);

			String selectJob = jobSql.selectNames;

			stmt = conn.prepareStatement(selectJob, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			stmt.setString(1, likeName);

			rs = stmt.executeQuery();

			while (rs.next()) {

				String name = rs.getString(1);

				names.add(name);
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

		return names;
	}

	/**
	 * Delete a job
	 *
	 * @param name is the name of the job to delete
	 * @throws NameNotFoundException if the job does not exist
	 * @throws Exception if any other error occurs
	 */
	public void deleteJob(String name) throws Exception {

		Connection conn = null;
		PreparedStatement stmt = null;

		try {
			conn = context.getConnection(null);

			int jobId = getJobId(conn, name);

			String deleteArgs = argumentSql.delete;

			stmt = conn.prepareStatement(deleteArgs);

			stmt.setInt(1, jobId);

			stmt.executeUpdate();

			stmt.close();
			stmt = null;

			String deleteSchedules = jobScheduleSql.delete;

			stmt = conn.prepareStatement(deleteSchedules);

			stmt.setInt(1, jobId);

			stmt.executeUpdate();

			stmt.close();
			stmt = null;

			String deleteJob = jobSql.delete;

			stmt = conn.prepareStatement(deleteJob);

			stmt.setInt(1, jobId);

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

	private int getJobId(Connection conn, String jobName) throws Exception {
		return CommonSql.getId(conn, jobSql.selectId, jobName, "Job");
	}

	/**
	 * @return true if the manager has been started to run jobs.
	 */
	public boolean isStarted() {
		return executor != null;
	}

	/**
	 * Start the manager so that it can subsequently run and stop jobs.
	 * 
	 * @throws RuntimeException if the manager is already started
	 * @throws Exception if any error occurs
	 */
	public void startup() throws Exception{

		if (isStarted()) {
			throw new RuntimeException("Manager is already started");
		}
		
		// Instantiate a JobExecutor and create a job monitor thread
		// that loops calling executor.getCompleted() to update the database
		// with job completion status.

		executor = new JobExecutor();
		monitorThread = new Thread(new JobMonitor());
		monitorThread.start();
	}

	private class JobMonitor implements Runnable {

		/**
		 * Monitor the completion of jobs and update database status.
		 */
		@Override
		public void run() {
			
			try {
				while (!Thread.interrupted()) {
					JobRun result = executor.getCompleted();
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
	 * Start a job by name.
	 * 
	 * @return the run object, which will all have a positive configId value
	 * @throws Exception if any error occurs
	 */
	public JobRun run(String jobName) throws Exception {

		if (!isStarted()) {
			throw new RuntimeException("Must startup manager before running jobs");
		}

		// Instantiate the process, arguments, and context,
		// submit the process to the executor,
		// and update the database with run status.

		DbProcess process;
		String[] args;
		Context jobContext;
		JobRun run;

		try {
			Job job = getJob(jobName);
			if (job == null) {
				throw new NameNotFoundException("Job does not exist");
			}

			process = context.loader.load(job.getScriptName());

			args = new String[job.getArguments().size()];
			int i = 0;
			for (ScriptArgument argument : job.getArguments()) {
				args[i++] = argument.getValue(); 
			}

			ContextProperties props =
					(job.getPropName() == null) ? contextProps :
					new ContextProperties(job.getPropName(), contextProps);

			jobContext = props.createContext(jobName, context);

			run = putRun(jobName);

			executor.submit(run, process, args, jobContext);

			updateRun(run);
		}
		catch (Exception ex) {
			throw ex;
		}

		return run;
	}

	/**
	 * Store a job run row in the database for a job that is being started.
	 */
	private JobRun putRun(String jobName) throws Exception {

		JobRun run = null;

		Connection conn = null;
		PreparedStatement stmt = null;

		try {
			conn = context.getConnection(null);

			int jobId = getJobId(conn, jobName);
			run = new JobRun(jobName);

			String insertRun = runSql.insert;

			stmt = conn.prepareStatement(insertRun);

			stmt.setInt(2, jobId);
			stmt.setString(3, jobName);
			stmt.setInt(4, run.getState().getStatus().getId());

			int runId;
			synchronized (this) {

				runId = getNextRunId(conn);

				stmt.setInt(1, runId);

				stmt.executeUpdate();
			}

			run.setRunId(runId);
		}
		catch (Exception ex) {
			throw ex;
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection(null);
		}

		return run;
	}

	private int getNextRunId(Connection conn) throws Exception {
		return CommonSql.getNextId(conn, runSql.selectLastId);
	}

	/**
	 * Update a job run row in the database.
	 */
	private void updateRun(JobRun run) throws Exception {

		Connection conn = null;
		PreparedStatement stmt = null;

		try {
			conn = context.getConnection(null);

			String updateRun = runSql.update;

			stmt = conn.prepareStatement(updateRun);

			JobRun.State state = run.getState();
			stmt.setInt(1, state.getStatus().getId());
			setTimestamp(stmt, 2, state.getStartTime());
			setTimestamp(stmt, 3, state.getEndTime());

			stmt.setInt(4, run.getRunId());

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
	 * Stop a job run by interrupting its thread.
	 * 
	 * @param run is the job run to stop.
	 * @return true if the run was stopped; false otherwise
	 * @throws Exception if any error occurs
	 */
	public boolean stop(JobRun run) throws Exception {

		if (!isStarted()) {
			throw new RuntimeException("Manager is not started; no jobs are running");
		}

		return executor.stop(run);
	}

	public boolean stop(int runId) throws Exception {
		return stop(executor.getRunning(runId));
	}

	/**
	 * Retrieve a list of currently running jobs
	 * @return the list of currently running jobs
	 * @throws Exception if any error occurs
	 */
	public List<JobRun> getRunning() throws Exception {

		if (!isStarted()) {
			throw new RuntimeException("Manager is not started; no jobs are running");
		}

		return executor.getRunning();
	}
	
	/**
	 * Retrieve a list of job runs from the database
	 * whose job names match an optional SQL wildcard pattern
	 * 
	 * @param likeName is the name wildcard pattern or null to get all job runs
	 * @param latest is true to only get the latest run for each job; otherwise, all runs are retrieved.
	 * @return the list of runs
	 * @throws Exception if any error occurs
	 */
	public List<JobRun> getRuns(String likeName, boolean latest) throws Exception {

		if (likeName == null) {
			likeName = "%";
		}

		List<JobRun> runs = new LinkedList<JobRun>();

		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			conn = context.getConnection(null);

			String selectRun = runSql.select;

			if (latest) {
				String selectAllLastRunIndex = runSql.selectAllLastId;

				String selectLastRun = String.format(runSql.selectLast, selectRun, selectAllLastRunIndex);

				selectRun = selectLastRun;
			}

			selectRun += runSql.whereJobName;

			stmt = conn.prepareStatement(selectRun, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			stmt.setString(1, likeName);

			rs = stmt.executeQuery();

			while (rs.next()) {

				int runId = rs.getInt(1);
				String jobName = rs.getString(2);
				Status status = Status.valueOf(rs.getInt(3));
				LocalDateTime startTime = getLocalDateTime(rs, 4);
				LocalDateTime endTime = getLocalDateTime(rs, 5);

				runs.add(new JobRun(runId, jobName, new JobRun.State(status, startTime, endTime)));
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
	 * Stop all jobs currently running.
	 * @throws Exception if any error occurs
	 */
	private void stopAll() throws Exception  {
		
		executor.stopAll();

		while (!executor.allCompleted()) {
			JobRun result = executor.getCompleted();
			updateRun(result);
		}
	}

	/**
	 * Shutdown job manager, stopping any running jobs.
	 * The manager still retains resources needed for
	 * creating, listing, and validating entities. 
	 * @throws Exception if any error occurs
	 */
	public void shutdown() throws Exception {

		if (!isStarted()) {
			throw new RuntimeException("Manager was not started.");
		}

		// Interrupt the job monitor thread to terminate it,
		// stop any running jobs, and shut down the executor.

		try {
			int jobTerminateWaitSeconds = 10;
	
			monitorThread.interrupt();
			monitorThread.join(jobTerminateWaitSeconds * 1000L);

			stopAll();

			executor.close();
		}
		catch (Exception ex) {
			throw ex;
		}

		executor = null;
	}

	/**
	 * Release any resources being used by this manager.
	 */
	@Override
	public void finalize() {

		try { if (isStarted()) shutdown(); } catch (Exception ex) {}

		try { if (context != null) context.close(); } catch (Exception ex) {}
	}
}
