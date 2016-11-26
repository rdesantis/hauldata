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

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;

import javax.naming.NameNotFoundException;
import javax.naming.NamingException;

import com.hauldata.dbpa.log.Analyzer;
import com.hauldata.dbpa.manage.JobManagerException;
import com.hauldata.dbpa.manage.api.Job;
import com.hauldata.dbpa.manage.api.JobRun;
import com.hauldata.dbpa.manage.api.ScriptArgument;
import com.hauldata.dbpa.manage.api.JobState;
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

	private static JobManager manager;

	private Analyzer analyzer;

	private ContextProperties contextProps;
	private Context context;

	private JobSql jobSql;
	private ArgumentSql argumentSql;
	private ScheduleSql scheduleSql;
	private JobScheduleSql jobScheduleSql;
	private RunSql runSql;

	private JobExecutor executor;
	private Thread monitorThread;

	private JobScheduler scheduler;

	/**
	 * Instantiate the singleton job manager instance
	 * <p>
	 * @param withLogAnalyzer is true to instantiate an Analyzer and add it as an
	 * appender to the log of the context of every job that is run.  This is for debugging.
	 * The analyzer can be retrieved with JobManager.getInstance.getAnalyzer();
	 * @return the manager
	 */
	public static JobManager instantiate(boolean withLogAnalyzer) {

		manager = new JobManager(withLogAnalyzer);

		return manager;
	}

	/**
	 * Get the singleton job manager instance
	 * @return the manager
	 */
	public static JobManager getInstance() throws JobManagerException.NotAvailable {
		if (manager == null) {
			throw new JobManagerException.NotAvailable();
		}
		return manager;
	}

	/**
	 * Prevent the job manager instance from being retrieved
	 * by any subsequent calls to getInstance().
	 * <p>
	 * Shuts down the manager if it is running.  Does this
	 * in such a way that no thread that already holds a
	 * manager reference can start up the manager after
	 * this function shuts it down.
	 */
	public static void killInstance() {
		if (manager == null) {
			throw new JobManagerException.AlreadyUnavailable();
		}

		synchronized (manager) {
			if (manager.isStarted()) {
				try {
					manager.shutdown();
				}
				catch (InterruptedException e) {
					// Don't let this prevent shutdown.
				}
			}
			manager = null;
		}
	}

	private JobManager(boolean withLogAnalyzer) {

		this.analyzer = withLogAnalyzer ? new Analyzer() : null;

		contextProps = new ContextProperties(programName);
		context = contextProps.createContext(null);

		// Get schema specifications.

		Properties schemaProps = contextProps.getProperties("schema");
		if (schemaProps == null) {
			throw new JobManagerException.SchemaPropertiesNotFound();
		}

		String tablePrefix = schemaProps.getProperty("tablePrefix");
		if (tablePrefix == null) {
			throw new JobManagerException.SchemaTablePrefixPropertyNotFound();
		}

		jobSql = new JobSql(tablePrefix);
		argumentSql = new ArgumentSql(tablePrefix);
		scheduleSql = new ScheduleSql(tablePrefix);
		jobScheduleSql = new JobScheduleSql(tablePrefix);
		runSql = new RunSql(tablePrefix);

		executor = null;

		scheduler = null;
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

	public JobScheduler getScheduler() {
		if (scheduler == null) {
			throw new JobManagerException.SchedulerNotAvailable();
		}
		return scheduler;
	}

	public Analyzer getAnalyzer() {
		return analyzer;
	}

	/**
	 * @return true if the manager has been started to run jobs.
	 */
	public boolean isStarted() {
		return executor != null;
	}

	/**
	 * Start the manager so that it can subsequently run, stop, and schedule jobs.
	 *
	 * @throws NotAvailable
	 * @throws AlreadyStarted if the manager is already started
	 * @throws SQLException if an error occurs reading job schedules
	 */
	public void startup() throws SQLException {

		synchronized (this) {

			if (manager == null) {
				throw new JobManagerException.NotAvailable();
			}
			else if (isStarted()) {
				throw new JobManagerException.AlreadyStarted();
			}

			// Instantiate a JobExecutor and create a job monitor thread
			// that loops calling executor.getCompleted() to update the database
			// with job completion status.

			executor = new JobExecutor();
			monitorThread = new Thread(new JobMonitor());
			monitorThread.start();

			// Start job scheduling.

			scheduler = new JobScheduler();
			scheduler.startAll();
		}
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
					catch (Exception ex) {
						// updateRun() does not throw InterruptedException.
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
	 * @return the run object.  Use the getRunId() member to retrieve the unique run ID.
	 * @throws JobManagerException.NotStarted
	 * @throws NamingException
	 * @throws IOException
	 * @throws SQLException
	 */
	public JobRun run(String jobName, Job job) throws IOException, NamingException, SQLException {

		if (!isStarted()) {
			throw new JobManagerException.NotStarted(JobManagerException.mustStartupBeforeJobRunMessage);
		}

		// Instantiate the process, arguments, and context,
		// submit the process to the executor,
		// and update the database with run status.

		DbProcess process;
		String[] args;
		Context jobContext;
		JobRun run;

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

		if (analyzer != null) {
			jobContext.logger.addAppender(analyzer);
		}

		run = putRun(jobName);

		executor.submit(run, process, args, jobContext);

		updateRun(run);

		return run;
	}

	/**
	 * Store a job run row in the database for a job that is being started.
	 * @throws SQLException
	 * @throws NameNotFoundException
	 */
	private JobRun putRun(String jobName) throws NameNotFoundException, SQLException  {

		JobRun run = null;

		Connection conn = null;
		PreparedStatement stmt = null;

		try {
			conn = context.getConnection(null);

			int jobId = CommonSql.getId(conn, jobSql.selectId, jobName, "Job");
			run = new JobRun(jobName);

			stmt = conn.prepareStatement(runSql.insert);

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
		finally {
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection(null);
		}

		return run;
	}

	private int getNextRunId(Connection conn) throws SQLException  {
		return CommonSql.getNextId(conn, runSql.selectLastId);
	}

	/**
	 * Update a job run row in the database.
	 * @throws SQLException
	 */
	private void updateRun(JobRun run) throws SQLException  {

		Connection conn = null;
		PreparedStatement stmt = null;

		try {
			conn = context.getConnection(null);

			stmt = conn.prepareStatement(runSql.update);

			JobState state = run.getState();
			stmt.setInt(1, state.getStatus().getId());
			setTimestamp(stmt, 2, state.getStartTime());
			setTimestamp(stmt, 3, state.getEndTime());

			stmt.setInt(4, run.getRunId());

			stmt.executeUpdate();
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
	 * Retrieve a currently running job
	 *
	 * @param runId is the ID of the job run to retrieve
	 * @return the job
	 * @throws JobManagerException.NotStarted
	 * @throws NoSuchElementException if the job is not running
	 */
	public JobRun getRunning(int runId) {

		if (!isStarted()) {
			throw new JobManagerException.NotStarted(JobManagerException.notStartedNoJobRunningMessage);
		}

		return executor.getRunning(runId);
	}

	/**
	 * Stop a job run by interrupting its thread.
	 *
	 * @param runId is the ID of the job run to stop.
	 * @return true if the run was stopped; false otherwise
	 * @throws JobManagerException.NotStarted
	 * @throws NoSuchElementException if the job is not running
	 */
	public boolean stopRun(int runId) throws NoSuchElementException {

		if (!isStarted()) {
			throw new JobManagerException.NotStarted(JobManagerException.notStartedNoJobRunningMessage);
		}

		return executor.stop(runId);
	}

	/**
	 * Retrieve a list of currently running jobs
	 * @return the list of currently running jobs
	 * @throws JobManagerException.NotStarted
	 */
	public List<JobRun> getRunning() {

		if (!isStarted()) {
			throw new JobManagerException.NotStarted(JobManagerException.notStartedNoJobRunningMessage);
		}

		return executor.getRunning();
	}

	/**
	 * Stop all jobs currently running.
	 * @throws SQLException if an error occurs recording job status to the database
	 * @throws InterruptedException
	 */
	private void stopAllRuns() throws InterruptedException   {

		executor.stopAll();

		while (!executor.allCompleted()) {
			JobRun result = executor.getCompleted();
			try {
				updateRun(result);
			}
			catch (SQLException e) {
				// Failed trying to update job status in the database.
				// Don't let this prevent orderly shutdown.

				// TODO: Log failure to update job status in database.
			}
		}
	}

	/**
	 * Shutdown job manager, stopping any running jobs and preventing
	 * scheduled job runs.
	 * <p>
	 * The manager still retains resources needed for
	 * creating, listing, and validating entities.
	 * @throws JobManagerException.NotStarted
	 * @throws InterruptedException
	 */
	public void shutdown() throws InterruptedException  {

		if (!isStarted()) {
			throw new JobManagerException.NotStarted();
		}

		// Prevent any more scheduled jobs from kicking off.

		scheduler.stopAll();
		scheduler = null;

		// Interrupt the job monitor thread to terminate it,
		// stop any running jobs, and shut down the executor.

		int jobTerminateWaitSeconds = 10;

		monitorThread.interrupt();
		monitorThread.join(jobTerminateWaitSeconds * 1000L);

		stopAllRuns();

		executor.close();
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
