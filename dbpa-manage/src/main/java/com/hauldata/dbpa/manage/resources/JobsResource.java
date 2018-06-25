/*
 * Copyright (c) 2016, 2017, Ronald DeSantis
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

package com.hauldata.dbpa.manage.resources;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;
import com.hauldata.dbpa.manage.JobExecutor;
import com.hauldata.dbpa.manage.JobManager;
import com.hauldata.dbpa.manage.JobManagerException;
import com.hauldata.dbpa.manage.sql.ArgumentSql;
import com.hauldata.dbpa.manage.sql.CommonSql;
import com.hauldata.dbpa.manage.sql.JobScheduleSql;
import com.hauldata.dbpa.manage.sql.JobSql;
import com.hauldata.dbpa.manage.sql.RunSql;
import com.hauldata.dbpa.manage.sql.ScheduleSql;
import com.hauldata.dbpa.manage_control.api.Job;
import com.hauldata.dbpa.manage_control.api.JobRun;
import com.hauldata.dbpa.manage_control.api.ScriptArgument;
import com.hauldata.dbpa.manage_control.api.JobStatus;
import com.hauldata.dbpa.manage_control.api.JobState;
import com.hauldata.dbpa.process.Context;

@Path("/jobs")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class JobsResource {

	public static final String jobNotFoundMessageStem = CommonSql.getNotFoundMessageStem("Job");
	public static final String scheduleNotFoundMessageStem = "One or more schedules not found for job: ";
	public static final String runningNotFoundMessageStem = JobExecutor.getRunningNotFoundMessageStem();

	public JobsResource() {}

	/**
	 * Store a job in the database.
	 *
	 * @param name is the job name.
	 * @param job is the job to store.
	 * @return the unique job ID created for the job.
	 * @throws NameNotFoundException if one or more of the specified schedules for the job does not exist
	 * @throws SQLException if the job cannot be stored for any reason
	 */
	@PUT
	@Path("{name}")
	@Timed
	public int put(@PathParam("name") String name, Job job) throws SQLException, NameNotFoundException {

		JobManager manager = JobManager.getInstance();
		Context context = manager.getContext();
		JobSql jobSql = manager.getJobSql();
		ArgumentSql argumentSql = manager.getArgumentSql();
		JobScheduleSql jobScheduleSql = manager.getJobScheduleSql();
		ScheduleSql scheduleSql = manager.getScheduleSql();

		Connection conn = null;
		PreparedStatement stmt = null;

		try {
			conn = context.getConnection(null);

			// Validate the schedules.

			List<Integer> scheduleIds = getScheduleIds(conn, scheduleSql, job.getScheduleNames());

			// Write the job header.

			int id = CommonSql.getId(conn, jobSql.selectId, name);

			if (id != -1) {
				// Job exists.  Delete the job schedules and arguments.

				deleteSchedules(conn, jobScheduleSql, id);
				CommonSql.execute(conn, argumentSql.delete, id);

				// Update existing job.

				stmt = conn.prepareStatement(jobSql.update);

				stmt.setString(1, job.getScriptName());
				stmt.setInt(2, job.isEnabled() ? 1 : 0);
				stmt.setInt(3, id);

				stmt.executeUpdate();

			}
			else {
				// Create new job.

				stmt = conn.prepareStatement(jobSql.insert);

				stmt.setString(2, name);
				stmt.setString(3, job.getScriptName());
				stmt.setInt(4, job.isEnabled() ? 1 : 0);

				synchronized (this) {

					id = CommonSql.getNextId(conn, jobSql.selectLastId);

					stmt.setInt(1, id);

					stmt.executeUpdate();
				}
			}

			stmt.close();
			stmt = null;

			// Write the arguments.

			if ((job.getArguments() != null) && (!job.getArguments().isEmpty())) {

				putArguments(conn, argumentSql, id, job.getArguments());
			}

			// Write the schedules.

			if (!scheduleIds.isEmpty()) {

				putSchedules(conn, jobScheduleSql, id, scheduleIds);
			}

			// Start the schedules if job is enabled.

			if (job.isEnabled()) {
				manager.getScheduler().start(scheduleIds);
			}

			return id;
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection(null, conn);
		}
	}

	private List<Integer> getScheduleIds(Connection conn, ScheduleSql scheduleSql, List<String> names) throws SQLException, NameNotFoundException {

		List<Integer> ids = new LinkedList<Integer>();

		if ((names == null) || names.isEmpty()) {
			return ids;
		}

		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			List<String> distinctNames = names.stream().distinct().collect(Collectors.toList());
			String paramList = distinctNames.stream().map(name -> "?").collect(Collectors.joining(","));

			String selectIds = String.format(scheduleSql.selectIds, paramList);

			stmt = conn.prepareStatement(selectIds, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			int paramIndex = 1;
			for (String name : distinctNames) {
				stmt.setString(paramIndex++, name);
			}

			rs = stmt.executeQuery();

			while (rs.next()) {
				ids.add(rs.getInt(1));
			}

			if (ids.size() != distinctNames.size()) {
				throw new NameNotFoundException("Some schedule names not found for job");
			}
		}
		finally {
			try { if (rs != null) rs.close(); } catch (Exception exx) {}
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}
		}

		return ids;
	}

	private void putArguments(Connection conn, ArgumentSql argumentSql, int id, List<ScriptArgument> arguments) throws SQLException {

		PreparedStatement stmt = null;

		try {
			stmt = conn.prepareStatement(argumentSql.insert);

			int argIndex = 1;
			for (ScriptArgument argument : arguments) {

				stmt.setInt(1, id);
				stmt.setInt(2, argIndex++);
				stmt.setString(3, argument.getName());
				stmt.setString(4, argument.getValue());

				stmt.addBatch();
			}

			stmt.executeBatch();
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}
		}
	}

	private void putSchedules(Connection conn, JobScheduleSql jobScheduleSql, int id, List<Integer> scheduleIds) throws SQLException {

		PreparedStatement stmt = null;

		try {
			stmt = conn.prepareStatement(jobScheduleSql.insert);

			for (int scheduleId : scheduleIds) {

				stmt.setInt(1, id);
				stmt.setInt(2, scheduleId);

				stmt.addBatch();
			}

			stmt.executeBatch();
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}
		}
	}

	/**
	 * Update the name of the script in a job in the database.
	 *
	 * @param name is the job name.
	 * @param scriptName is the new script name to store.
	 * @throws NameNotFoundException if the job does not exist
	 * @throws SQLException if the job cannot be stored for any reason
	 */
	@PUT
	@Path("{name}/script")
	@Timed
	public void putScriptName(@PathParam("name") String name, String scriptName) throws NameNotFoundException, SQLException {
		putJobString(name, JobSql.scriptNameColumn, scriptName);
	}

	/**
	 * Update a string-valued column in a job in the database.
	 *
	 * @param name is the job name.
	 * @param columnName is the name of the column in which to store the new value.
	 * @param value is the new value to store.
	 * @throws NameNotFoundException if the job does not exist
	 * @throws SQLException if the job cannot be stored for any reason
	 */
	private void putJobString(String name, String columnName, String value) throws NameNotFoundException, SQLException {

		JobManager manager = JobManager.getInstance();
		Context context = manager.getContext();
		JobSql jobSql = manager.getJobSql();

		Connection conn = null;

		try {
			conn = context.getConnection(null);

			int id = CommonSql.getId(conn, jobSql.selectId, name, "Job");

			CommonSql.execute(conn, jobSql.updateField, columnName, value, id);
		}
		finally {
			if (conn != null) context.releaseConnection(null, conn);
		}
	}

	/**
	 * Update the arguments in a job in the database.
	 *
	 * @param name is the job name.
	 * @param arguments are the new arguments to store or null to remove all arguments.
	 * @throws NameNotFoundException if the job does not exist
	 * @throws SQLException if the job cannot be updated for any reason
	 */
	@PUT
	@Path("{name}/arguments")
	@Timed
	public void putArguments(@PathParam("name") String name, List<ScriptArgument> arguments) throws NameNotFoundException, SQLException {

		JobManager manager = JobManager.getInstance();
		Context context = manager.getContext();
		JobSql jobSql = manager.getJobSql();
		ArgumentSql argumentSql = manager.getArgumentSql();

		Connection conn = null;

		try {
			conn = context.getConnection(null);

			// Delete the existing arguments and insert the new arguments.

			int id = CommonSql.getId(conn, jobSql.selectId, name, "Job");

			CommonSql.execute(conn, argumentSql.delete, id);

			if (arguments != null) {
				putArguments(conn, argumentSql, id, arguments);
			}
		}
		finally {
			if (conn != null) context.releaseConnection(null, conn);
		}
	}

	/**
	 * Update the schedules in a job in the database.
	 * This may cause the job to become scheduled.
	 *
	 * @param name is the job name.
	 * @param scheduleNames are the names of the new schedules to store or null to remove all schedules.
	 * Schedules are stored by ID.
	 * @throws NameNotFoundException if the job does not exist
	 * @throws SQLException if the job cannot be updated for any reason
	 */
	@PUT
	@Path("{name}/schedules")
	@Timed
	public void putSchedules(@PathParam("name") String name, List<String> scheduleNames) throws NameNotFoundException, SQLException {

		JobManager manager = JobManager.getInstance();
		Context context = manager.getContext();
		JobSql jobSql = manager.getJobSql();
		JobScheduleSql jobScheduleSql = manager.getJobScheduleSql();
		ScheduleSql scheduleSql = manager.getScheduleSql();

		Connection conn = null;

		try {
			conn = context.getConnection(null);

			// Validate the schedules.

			List<Integer> scheduleIds = null;
			if (scheduleNames != null) {
				scheduleIds = getScheduleIds(conn, scheduleSql, scheduleNames);
			}

			// Find the job ID and whether it is enabled for scheduling.

			SimpleEntry<Integer, Boolean> idEnabled = getIdEnabled(conn, jobSql, name);

			int id = idEnabled.getKey();
			boolean enabled = idEnabled.getValue();

			// Delete the existing schedules and insert the new schedules.

			deleteSchedules(conn, jobScheduleSql, id);

			if (scheduleIds != null) {

				putSchedules(conn, jobScheduleSql, id, scheduleIds);

				// Start the schedules if job is enabled.

				if (enabled) {
					manager.getScheduler().start(scheduleIds);
				}
			}
		}
		finally {
			if (conn != null) context.releaseConnection(null, conn);
		}
	}

	/**
	 * Delete the arguments in a job in the database.
	 *
	 * @param name is the job name.
	 * @throws NameNotFoundException if the job does not exist
	 * @throws SQLException if the job cannot be updated for any reason
	 */
	@DELETE
	@Path("{name}/arguments")
	@Timed
	public void deleteArguments(@PathParam("name") String name) throws NameNotFoundException, SQLException {
		putArguments(name, null);
	}

	/**
	 * Delete the schedules in a job in the database.
	 * If the job was scheduled, it becomes unscheduled.
	 *
	 * @param name is the job name.
	 * @throws NameNotFoundException if the job does not exist
	 * @throws SQLException if the job cannot be updated for any reason
	 */
	@DELETE
	@Path("{name}/schedules")
	@Timed
	public void deleteScheduleNames(@PathParam("name") String name) throws NameNotFoundException, SQLException {
		putSchedules(name, null);
	}

	/**
	 * Update the enabled status in a job in the database.
	 * This may cause the job to become scheduled.
	 *
	 * @param name is the job name.
	 * @param enabled is the new value to store.
	 * @throws NameNotFoundException if the job does not exist
	 * @throws SQLException if the job cannot be stored for any reason
	 */
	@PUT
	@Path("{name}/enabled")
	@Timed
	public void putEnabled(@PathParam("name") String name, boolean enabled) throws NameNotFoundException, SQLException {

		JobManager manager = JobManager.getInstance();
		Context context = manager.getContext();
		JobSql jobSql = manager.getJobSql();
		JobScheduleSql jobScheduleSql = manager.getJobScheduleSql();

		Connection conn = null;

		try {
			conn = context.getConnection(null);

			// Find the job ID and whether it is currently enabled for scheduling.
			// Then update the job.

			SimpleEntry<Integer, Boolean> idEnabled = getIdEnabled(conn, jobSql, name);

			int id = idEnabled.getKey();
			boolean wasEnabled = idEnabled.getValue();

			CommonSql.execute(conn, jobSql.updateField, JobSql.enabledColumn, enabled ? 1 : 0, id);

			// Start the schedules if job is enabled; stop unused schedules if job is disabled.

			if (enabled != wasEnabled) {

				List<Integer> scheduleIds = getScheduleIds(conn, jobScheduleSql, id);

				if (enabled) {
					manager.getScheduler().start(scheduleIds);
				}
				else {
					stopUnusedSchedules(conn, jobScheduleSql, scheduleIds);
				}
			}
		}
		finally {
			if (conn != null) context.releaseConnection(null, conn);
		}
	}

	private SimpleEntry<Integer, Boolean> getIdEnabled(Connection conn, JobSql jobSql, String name) throws SQLException, NameNotFoundException {

		int id = -1;
		boolean enabled = false;

		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			stmt = conn.prepareStatement(jobSql.selectIdEnabled, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			stmt.setString(1, name);

			rs = stmt.executeQuery();

			if (rs.next()) {
				id = rs.getInt(1);
				enabled = (rs.getInt(2) == 1);
			}
			else {
				throw new NameNotFoundException(jobNotFoundMessageStem + name);
			}
		}
		finally {
			try { if (rs != null) rs.close(); } catch (Exception exx) {}
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}
		}

		return new SimpleEntry<Integer, Boolean>(id, enabled);
	}

	private List<Integer> getScheduleIds(Connection conn, JobScheduleSql jobScheduleSql, int id) throws SQLException {

		List<Integer> ids = new LinkedList<Integer>();

		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			stmt = conn.prepareStatement(jobScheduleSql.select, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			stmt.setInt(1, id);

			rs = stmt.executeQuery();

			while (rs.next()) {
				ids.add(rs.getInt(1));
			}
		}
		finally {
			try { if (rs != null) rs.close(); } catch (Exception exx) {}
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}
		}

		return ids;
	}

	/**
	 * Retrieve a set of job(s) from the database
	 * whose names match an optional SQL wildcard pattern
	 *
	 * @param likeName is the name wildcard pattern or null to get all jobs
	 * @return a map of job names to job objects retrieved from the database
	 * or an empty map if no job with a matching name is found
	 * @throws SQLException
	 * @throws Exception if an error occurs
	 */
	@GET
	@Timed
	public Map<String, Job> getJobs(@QueryParam("like") String likeName) throws SQLException {

		JobManager manager = JobManager.getInstance();
		Context context = manager.getContext();
		JobSql jobSql = manager.getJobSql();
		ArgumentSql argumentSql = manager.getArgumentSql();
		JobScheduleSql jobScheduleSql = manager.getJobScheduleSql();

		if (likeName == null) {
			likeName = "%";
		}

		Map<String, Job> jobs = new HashMap<String, Job>();

		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			conn = context.getConnection(null);

			stmt = conn.prepareStatement(jobSql.select, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			stmt.setString(1, likeName);

			rs = stmt.executeQuery();

			while (rs.next()) {

				int id = rs.getInt(1);
				String jobName = rs.getString(2);
				String scriptName = rs.getString(3);
				boolean enabled = (rs.getInt(4) == 1);

				List<ScriptArgument> arguments = getArguments(conn, argumentSql, id);

				List<String> scheduleNames = getJobScheduleNames(conn, jobScheduleSql, id);

				jobs.put(jobName, new Job(scriptName, arguments, scheduleNames, enabled));
			}
		}
		finally {
			try { if (rs != null) rs.close(); } catch (Exception exx) {}
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection(null, conn);
		}

		return jobs;
	}

	private List<ScriptArgument> getArguments(Connection conn, ArgumentSql argumentSql, int jobId) throws SQLException {

		List<ScriptArgument> arguments = new LinkedList<ScriptArgument>();

		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			stmt = conn.prepareStatement(argumentSql.select, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			stmt.setInt(1, jobId);

			rs = stmt.executeQuery();

			while (rs.next()) {

				String argName = rs.getString(1);
				String argValue = rs.getString(2);

				arguments.add(new ScriptArgument(argName, argValue));
			}
		}
		finally {
			try { if (rs != null) rs.close(); } catch (Exception exx) {}
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}
		}

		return arguments;
	}

	private List<String> getJobScheduleNames(Connection conn, JobScheduleSql jobScheduleSql, int jobId) throws SQLException {

		List<String> names = new LinkedList<String>();

		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			stmt = conn.prepareStatement(jobScheduleSql.selectScheduleNamesByJobId, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			stmt.setInt(1, jobId);

			rs = stmt.executeQuery();

			while (rs.next()) {

				names.add(rs.getString(1));
			}
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
	 * @return the job
	 * @throws NameNotFoundException if the job does not exist
	 * @throws IllegalArgumentException if the name contains wildcards and matches multiple job names
	 * @throws SQLException if the job cannot be retrieved
	 */
	@GET
	@Path("{name}")
	@Timed
	public Job get(@PathParam("name") String name) throws SQLException, NameNotFoundException {

		Map<String, Job> jobs = getJobs(name);

		if (jobs.size() == 0) {
			throw new NameNotFoundException(jobNotFoundMessageStem + name);
		}
		else if (1 < jobs.size()) {
			throw new IllegalArgumentException();
		}

		return jobs.values().stream().findFirst().orElse(null);
	}

	/**
	 * Retrieve a set of job name(s) from the database
	 * whose names match an optional SQL wildcard pattern
	 *
	 * @param likeName is the name wildcard pattern or null to get all job names
	 * @return a list of job names or an empty list if no job with a matching name is found
	 * @throws SQLException if an error occurs
	 */
	@GET
	@Path("-/names")
	@Timed
	public List<String> getNames(@QueryParam("like") String likeName) throws SQLException {

		JobManager manager = JobManager.getInstance();
		Context context = manager.getContext();
		JobSql jobSql = manager.getJobSql();

		if (likeName == null) {
			likeName = "%";
		}

		List<String> names = new LinkedList<String>();

		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			conn = context.getConnection(null);

			stmt = conn.prepareStatement(jobSql.selectNames, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			stmt.setString(1, likeName);

			rs = stmt.executeQuery();

			while (rs.next()) {

				String name = rs.getString(1);

				names.add(name);
			}
		}
		finally {
			try { if (rs != null) rs.close(); } catch (Exception exx) {}
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection(null, conn);
		}

		return names;
	}

	/**
	 * Delete a job
	 *
	 * @param name is the name of the job to delete
	 * @throws NameNotFoundException if the job does not exist
	 * @throws SQLException if any other error occurs
	 */
	@DELETE
	@Path("{name}")
	@Timed
	public void delete(@PathParam("name") String name) throws NameNotFoundException, SQLException {

		JobManager manager = JobManager.getInstance();
		Context context = manager.getContext();
		JobSql jobSql = manager.getJobSql();
		ArgumentSql argumentSql = manager.getArgumentSql();
		JobScheduleSql jobScheduleSql = manager.getJobScheduleSql();

		Connection conn = null;

		try {
			conn = context.getConnection(null);

			int jobId = CommonSql.getId(conn, jobSql.selectId, name, "Job");

			deleteSchedules(conn, jobScheduleSql, jobId);
			CommonSql.execute(conn, argumentSql.delete, jobId);
			CommonSql.execute(conn, jobSql.delete, jobId);
		}
		finally {
			if (conn != null) context.releaseConnection(null, conn);
		}
	}

	/**
	 * Delete the schedules for a job.  Stop each schedule that no longer has any enabled jobs on it.
	 * <p>
	 * WARNING: This is not thread safe.  If an enabled job is added as the only job on a schedule
	 * that is deleted from this job while this method is executing, the schedule may be stopped
	 * and the new job will not run on schedule.
	 *
	 * @param jobId is the job ID
	 * @throws SQLException
	 */
	private void deleteSchedules(Connection conn, JobScheduleSql jobScheduleSql, int jobId) throws SQLException {

		List<Integer> scheduleIds = getScheduleIds(conn, jobScheduleSql, jobId);

		CommonSql.execute(conn, jobScheduleSql.delete, jobId);

		stopUnusedSchedules(conn, jobScheduleSql, scheduleIds);
	}

	/**
	 * Stop each of a list of schedules that has no enabled jobs on it.
	 * <p>
	 * WARNING: This is not thread safe.  See deleteSchedules().
	 *
	 * @param scheduleIds is the list of schedule ID
	 * @throws SQLException
	 */
	private void stopUnusedSchedules(Connection conn, JobScheduleSql jobScheduleSql, List<Integer> scheduleIds) throws SQLException {
		for (int scheduleId : scheduleIds) {
			stopUnusedSchedule(conn, jobScheduleSql, scheduleId);
		}
	}

	/**
	 * Stop a schedule if it has no enabled jobs on it.
	 * <p>
	 * WARNING: This is not thread safe.  See deleteSchedules().
	 *
	 * @param scheduleId is the schedule ID
	 * @throws SQLException
	 */
	private void stopUnusedSchedule(Connection conn, JobScheduleSql jobScheduleSql, int scheduleId) throws SQLException {

		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			stmt = conn.prepareStatement(jobScheduleSql.selectCountEnabledJobsByScheduleId);

			stmt.setInt(1, scheduleId);

			rs = stmt.executeQuery();

			rs.next();

			if (rs.getInt(1) == 0) {
				JobManager.getInstance().getScheduler().stop(scheduleId);
			}
		}
		catch (InterruptedException e) {
			throw new JobManagerException.SchedulerNotAvailable();
		}
		finally {
			try { if (rs != null) rs.close(); } catch (Exception exx) {}
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}
		}
	}

	/**
	 * Retrieve a list of job runs from the database
	 * whose job names match an optional SQL wildcard pattern
	 *
	 * @param likeName is the name wildcard pattern or null to get all job runs
	 * @param latest is true to only get the latest run for each job; otherwise, all runs are retrieved.
	 * @return the list of runs
	 * @throws SQLException if any error occurs
	 */
	@GET
	@Path("-/runs")
	@Timed
	public List<JobRun> getRuns(@QueryParam("like") String likeName, @QueryParam("latest") Boolean latest, @QueryParam("ascending") Boolean ascending) throws SQLException {

		JobManager manager = JobManager.getInstance();
		Context context = manager.getContext();
		RunSql runSql = manager.getRunSql();

		if (likeName == null) {
			likeName = "%";
		}

		if (latest == null) {
			latest = false;
		}

		List<JobRun> runs = new LinkedList<JobRun>();

		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			conn = context.getConnection(null);

			String selectRun;
			if (!latest) {
				selectRun = runSql.select;
			}
			else {
				selectRun = String.format(runSql.selectLast, runSql.select, runSql.selectAllLastId);
			}

			selectRun += runSql.whereJobName + String.format(runSql.orderById, CommonSql.orderBy((ascending == null) || ascending));

			stmt = conn.prepareStatement(selectRun, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			stmt.setString(1, likeName);

			rs = stmt.executeQuery();

			while (rs.next()) {

				int runId = rs.getInt(1);
				String jobName = rs.getString(2);
				JobStatus status = JobStatus.valueOf(rs.getInt(3));
				LocalDateTime startTime = getLocalDateTime(rs, 4);
				LocalDateTime endTime = getLocalDateTime(rs, 5);
				String message = rs.getString(6);

				runs.add(new JobRun(runId, jobName, new JobState(status, startTime, endTime, message)));
			}
		}
		finally {
			try { if (rs != null) rs.close(); } catch (Exception ex) {}
			try { if (stmt != null) stmt.close(); } catch (Exception ex) {}

			if (conn != null) context.releaseConnection(null, conn);
		}

		return runs;
	}

	@GET
	@Path("-/running")
	@Timed
	public List<JobRun> getRunning() {
		return JobManager.getInstance().getRunning();
	}

	/**
	 * Run a job optionally changing the value(s) of argument(s) passed to the job
	 *
	 * @param name is the name of the job to run
	 * @param arguments is a list of (name, value) pairs where each "name" matches an argument
	 * in the job definition and the corresponding "value" replaces the argument value in the
	 * job definition when invoking the job
	 * @return the run ID of the started job
	 * @throws NameNotFoundException if the job does not exist
	 * @throws IllegalArgumentException if a provided argument name does not match an argument
	 * @throws SQLException if the job cannot be retrieved
	 * @throws IOException if an error occurs retrieving the job's script
	 * @throws NamingException if an error occurs parsing the job's script
	 */
	@POST
	@Path("-/running/{name}")
	@Timed
	public int run(@PathParam("name") String name, List<ScriptArgument> arguments) throws SQLException, IOException, NamingException {

		Job job = get(name);

		if (arguments != null) {
			for (ScriptArgument newArgument : arguments) {

				ScriptArgument oldArgument = job.getArguments().stream().filter(oA -> oA.getName().equals(newArgument.getName())).findAny().
						orElseThrow(() -> new IllegalArgumentException("No such argument: " + newArgument.getName()));

				int index = job.getArguments().indexOf(oldArgument);
				job.getArguments().set(index, newArgument);
			}
		}

		return JobManager.getInstance().run(name, job).getRunId();
	}

	@GET
	@Path("-/running/{id}")
	@Timed
	public JobRun getRunning(@PathParam("id") int id) {
		return JobManager.getInstance().getRunning(id);
	}

	@DELETE
	@Path("-/running/{id}")
	@Timed
	public void stop(@PathParam("id") int id) {
		JobManager.getInstance().stopRun(id);
	}

	private LocalDateTime getLocalDateTime(ResultSet rs, int columnIndex) throws SQLException {

		Timestamp timestamp = rs.getTimestamp(columnIndex);
		return (timestamp != null) ? timestamp.toLocalDateTime() : null;
	}
}
