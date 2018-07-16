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

package com.hauldata.dbpa.manage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import javax.naming.NameNotFoundException;
import javax.naming.NamingException;

import org.slf4j.LoggerFactory;

import com.hauldata.dbpa.DBPA;
import com.hauldata.dbpa.log.Analyzer;
import com.hauldata.dbpa.log.Logger;
import com.hauldata.dbpa.manage.JobManagerException;
import com.hauldata.dbpa.manage.resources.JobsResource;
import com.hauldata.dbpa.manage.resources.SchemaResource;
import com.hauldata.dbpa.manage_control.api.Job;
import com.hauldata.dbpa.manage_control.api.JobRun;
import com.hauldata.dbpa.manage_control.api.ScriptArgument;
import com.hauldata.dbpa.manage_control.api.JobState;
import com.hauldata.dbpa.manage_control.api.JobStatus;
import com.hauldata.dbpa.manage.sql.JobSql;
import com.hauldata.dbpa.manage.sql.ManagerRunSql;
import com.hauldata.dbpa.manage.sql.ArgumentSql;
import com.hauldata.dbpa.manage.sql.CommonSql;
import com.hauldata.dbpa.manage.sql.ScheduleSql;
import com.hauldata.dbpa.manage.sql.JobScheduleSql;
import com.hauldata.dbpa.manage.sql.RunSql;
import com.hauldata.dbpa.process.Alert;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.process.ContextProperties;
import com.hauldata.dbpa.process.DbProcess;
import com.timgroup.statsd.StatsDClient;

public class JobManager {

	private static final String managerProgramName = "ManageDbp";

	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(JobManager.class);

	private static JobManager manager;

	private Analyzer analyzer;

	private ContextProperties contextProps;
	private Context context;
	private StatsDClient statsd;

	private ManagerRunSql managerRunSql;
	private JobSql jobSql;
	private ArgumentSql argumentSql;
	private ScheduleSql scheduleSql;
	private JobScheduleSql jobScheduleSql;
	private RunSql runSql;

	private JobExecutor executor;
	private Thread monitorThread;

	private JobScheduler scheduler;

	private ContextProperties jobContextProps;

	/**
	 * Instantiate the singleton job manager instance
	 * <p>
	 * @param withLogAnalyzer is true to instantiate an Analyzer and add it as an
	 * appender to the log of the context of every job that is run.  This is for debugging.
	 * The analyzer can be retrieved with JobManager.getInstance.getAnalyzer();
	 * @return the manager
	 */
	public static JobManager instantiate(boolean withLogAnalyzer) {
		if (manager == null) {
			manager = new JobManager(withLogAnalyzer);
		}
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

		this.analyzer = withLogAnalyzer ? new Analyzer(Logger.Level.info) : null;

		// Job default context properties.

		jobContextProps = new ContextProperties(DBPA.runProgramName);

		// Manager context.

		contextProps = new ContextProperties(managerProgramName, jobContextProps, false);
		context = contextProps.createContext(null);

		JobStatisticsCollector.WithWarning statsdWithWarning = JobStatisticsCollector.create(contextProps.getProperties("log"));
		statsd = statsdWithWarning.statsd;
		if (statsdWithWarning.warning != null) {
			LOGGER.warn("Not collecting statistics: " + statsdWithWarning.warning);
		}

		String tablePrefix = context.connectionProps.getProperty("managerTablePrefix", "");

		managerRunSql = new ManagerRunSql(tablePrefix);
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

	public ManagerRunSql getManagerRunSql() {
		return managerRunSql;
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
	 * @return true if the manager can be started to run jobs.
	 */
	public boolean canStartup() {

		// The database must be available and the schema must exist in order to startup.

		boolean schemaExists = false;
		try {
			SchemaResource schema = new SchemaResource();
			schemaExists = schema.confirm();
		}
		catch (Exception ex) {}

		return schemaExists;
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

			// Reserve the database schema to prevent other JobManager instances from using it.

			Connection conn = null;

			try {
				conn = context.getConnection(null);

				int lastSequence = CommonSql.getNextId(conn, manager.getManagerRunSql().selectLastSequence) - 1;
				int updateCount = CommonSql.executeUpdateNow(conn, manager.getManagerRunSql().updateStart, lastSequence);

				if (updateCount != 1) {
					throw new JobManagerException.SchemaInUse();
				}
			}
			finally {
				if (conn != null) context.releaseConnection(null, conn);
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

	/**
	 * Reset the manager's persistent state from a previous crash.
	 * <p>
	 * TODO: Take argument boolean hard, which distinguishes a hard reset from a soft reset.
	 * Hard versus soft requires the manager to write a unique server identifier to the
	 * ManagerRun table when it updates startTime to reserve the database schema.
	 * Hard reset would reset the ManagerRun table regardless of which server reserved
	 * the schema, e.g., when starting a server on a new host after a crash on a different host.
	 * Soft reset would only reset the ManagerRun table if it was reserved by the
	 * server attempting the reset.
	 * <p>
	 * Each host that runs ManageDbp should include a soft reset in the host startup
	 * to clean up after a previous crash on the same host.  The soft reset prevents
	 * erroneous reset by an old host after a new instance of ManageDbp has been
	 * stood up on a new host.
	 * <p>
	 * The implementation here is a hard reset.
	 *
	 * @throws SQLException if an error occurs resetting the database schema
	 */
	public void reset() throws SQLException {

		synchronized (this) {

			if (manager == null) {
				throw new JobManagerException.NotAvailable();
			}
			else if (isStarted()) {
				throw new JobManagerException.AlreadyStarted();
			}

			Connection conn = null;
			PreparedStatement stmt = null;

			try {
				conn = context.getConnection(null);

				// If the database schema is reserved, free it.

				int lastSequence = CommonSql.getNextId(conn, manager.getManagerRunSql().selectLastSequence) - 1;
				int updateCount = CommonSql.executeUpdateNow(conn, manager.getManagerRunSql().updateEndIfStartedNotEnded, lastSequence);

				if (updateCount == 1) {

					// Prepare data schema for next use.

					CommonSql.execute(conn, manager.getManagerRunSql().insertSequence, lastSequence + 1);

					// Because the last manager instance crashed, job runs may remain persisted in "in progress" status.
					// Move those runs to a terminal status.

					stmt = conn.prepareStatement(runSql.updateByStatus);

					stmt.setInt(1, JobStatus.controllerShutdown.getId());
					setTimestamp(stmt, 2, LocalDateTime.now());
					setMessage(stmt, 3, managerProgramName + " abnormal termination");

					stmt.setInt(4, JobStatus.runInProgress.getId());

					stmt.executeUpdate();
				}
			}
			finally {
				try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

				if (conn != null) context.releaseConnection(null, conn);
			}
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
						statsd.incrementCounter(result.getState().getStatus().name());

						if (result.getState().getStatus() == JobStatus.runFailed) {

							logAndAlert("Job run failed: {} - {}", result.getJobName(), result.getState().getMessage(), "unspecified failure");
						}
					}
					catch (Exception ex) {
						// updateRun() does not throw InterruptedException.
						// Nothing to be done about SQL exceptions and such.
						// Want to keep running even though the run table may not be getting updated.

						logAndAlert("Error recording job completion: {} - {}", result.getJobName(), ex.getMessage(), ex.getClass().getName());
						statsd.incrementCounter("databaseError");
					}
				}
			}
			catch (InterruptedException iex) {
				// InterruptedException terminates the loop and ends the thread as desired.
			}
		}
	}

	/**
	 * Start a job with alerting for failed job start.
	 *
	 * @param name
	 * @return
	 * @throws SQLException
	 * @throws IOException
	 * @throws NamingException
	 */
	public void run(String name) throws SQLException, IOException, NamingException {

		Exception ex = null;
		String defaultMessage = null;
		try {
			run(name, new JobsResource().get(name));
		}
		catch (FileNotFoundException fex) {
			ex = fex; defaultMessage = "Script file not found";
			throw fex;
		}
		catch (IOException | NamingException | RuntimeException lex) {
			ex = lex; defaultMessage = "Error loading script";
			throw lex;
		}
		catch (SQLException sex) {
			ex = sex; defaultMessage = "Database error during job start";
			statsd.incrementCounter("databaseError");
			throw sex;
		}
		finally {
			if (ex != null) {
				logAndAlert("Failed to start job: {} - {}", name, ex.getMessage(), defaultMessage);
				statsd.incrementCounter(JobStatus.loadFailed.name());
			}
		}
	}

	private void logAndAlert(String logIntroMessage, String processID, String failMessage, String defaultMessage) {

		String message = Optional.ofNullable(failMessage).orElse(defaultMessage);

		LOGGER.warn(logIntroMessage, processID, message);

		try {
			Alert.send(processID, contextProps, message);
		}
		catch (Exception ex) {
			LOGGER.error("Cannot send alerts: {}", Optional.ofNullable(ex.getMessage()).orElse(ex.getClass().getName()));
			statsd.incrementCounter("alertError");
		}
	}

	/**
	 * Start a job by name.
	 * Do not start a new instance of a job if a previous instance is still running.
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

		boolean isJobRunning = executor.getRunning().stream().anyMatch(r -> r.getJobName().equals(jobName));
		if (isJobRunning) {
			throw new JobManagerException.AlreadyStarted(JobManagerException.alreadyRunningMessage);
		}

		LOGGER.info("Running job: {}", jobName);
		statsd.incrementCounter(JobStatus.runInProgress.name());

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

		jobContext = jobContextProps.createContext(jobName);

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
			setMessage(stmt, 5, run.getState().getMessage());

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

			if (conn != null) context.releaseConnection(null, conn);
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

			stmt = conn.prepareStatement(runSql.updateById);

			JobState state = run.getState();
			stmt.setInt(1, state.getStatus().getId());
			setTimestamp(stmt, 2, state.getStartTime());
			setTimestamp(stmt, 3, state.getEndTime());
			setMessage(stmt, 4, state.getMessage());

			stmt.setInt(5, run.getRunId());

			stmt.executeUpdate();
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection(null, conn);
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

	private void setMessage(PreparedStatement stmt, int parameterIndex, String message) throws SQLException {

		if (message != null) {
			String trimmedMessage = (message.length() <= RunSql.maxMessageLength) ? message : message.substring(0, RunSql.maxMessageLength);
			stmt.setString(parameterIndex, trimmedMessage);
		}
		else {
			stmt.setNull(parameterIndex, Types.VARCHAR);
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

		LOGGER.info("Stop job run ID {}", runId);

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

		LOGGER.info("Stopping all job runs");

		executor.stopAll();

		while (!executor.allCompleted()) {
			JobRun result = executor.getCompleted();
			try {
				updateRun(result);
			}
			catch (SQLException se) {
				// Failed trying to update job status in the database.
				// Don't let this prevent orderly shutdown.

				LOGGER.warn("Failed updating job status in database", se);
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

		// Prepare data schema for next use and release it.

		Connection conn = null;
		try {
			conn = context.getConnection(null);

			int nextSequence = CommonSql.getNextId(conn, manager.getManagerRunSql().selectLastSequence);
			CommonSql.execute(conn, manager.getManagerRunSql().insertSequence, nextSequence);

			int lastSequence = nextSequence - 1;
			CommonSql.executeUpdateNow(conn, manager.getManagerRunSql().updateEnd, lastSequence);
		}
		catch (SQLException sex) {
			LOGGER.error("Failed releasing data schema", sex);
		}
		finally {
			if (conn != null) context.releaseConnection(null, conn);
		}
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
