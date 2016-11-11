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

package com.hauldata.dbpa.manage.resources;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.naming.NameNotFoundException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;
import com.hauldata.dbpa.manage.JobManager;
import com.hauldata.dbpa.manage.api.Job;
import com.hauldata.dbpa.manage.api.JobRun;
import com.hauldata.dbpa.manage.api.ScriptArgument;
import com.hauldata.dbpa.manage.api.JobRun.Status;
import com.hauldata.dbpa.manage.sql.ArgumentSql;
import com.hauldata.dbpa.manage.sql.CommonSql;
import com.hauldata.dbpa.manage.sql.JobScheduleSql;
import com.hauldata.dbpa.manage.sql.JobSql;
import com.hauldata.dbpa.manage.sql.RunSql;
import com.hauldata.dbpa.manage.sql.ScheduleSql;
import com.hauldata.dbpa.process.Context;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class JobsResource {

	public static final String jobNotFoundMessageStem = "Job not found: ";

	public JobsResource() {}

	@PUT
	@Path("/jobs/{name}")
	@Timed
	public void put(@PathParam("name") String name, Job job) {
		try {
			putJob(name, job);
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@GET
	@Path("/jobs/{name}")
	@Timed
	public Job get(@PathParam("name") String name) {
		try {
			return getJob(name);
		}
		catch (NameNotFoundException ex) {
			throw new WebApplicationException(jobNotFoundMessageStem + name, 404);
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@DELETE
	@Path("/jobs/{name}")
	@Timed
	public void delete(@PathParam("name") String name) {
		try {
			deleteJob(name);
		}
		catch (NameNotFoundException ex) {
			throw new WebApplicationException(jobNotFoundMessageStem + name, 404);
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@PUT
	@Path("/jobs/{name}/script")
	@Timed
	public void putScriptName(@PathParam("name") String name, String scriptName) {
		try {
			// TODO!!!
			return;
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@PUT
	@Path("/jobs/{name}/propfile")
	@Timed
	public void putPropName(@PathParam("name") String name, String propName) {
		try {
			// TODO!!!
			return;
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@PUT
	@Path("/jobs/{name}/arguments")
	@Timed
	public void putArguments(@PathParam("name") String name, List<ScriptArgument> arguments) {
		try {
			// TODO!!!
			return;
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@PUT
	@Path("/jobs/{name}/schedules")
	@Timed
	public void putScheduleNames(@PathParam("name") String name, List<String> scheduleNames) {
		try {
			// TODO!!!
			return;
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@PUT
	@Path("/jobs/{name}/enabled")
	@Timed
	public void putEnabled(@PathParam("name") String name, boolean enabled) {
		try {
			// TODO!!!
			return;
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@GET
	@Path("/job/names")
	@Timed
	public List<String> getNames(@QueryParam("like") Optional<String> likeName) {
		try {
			return getJobNames(likeName.orElse(null));
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@GET
	@Path("/job/runs")
	@Timed
	public List<JobRun> getRuns(@QueryParam("like") Optional<String> likeName, @QueryParam("latest") Optional<Boolean> latest) {
		try {
			return getJobRuns(likeName.orElse(null), latest.orElse(false));
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@GET
	@Path("/job/running")
	@Timed
	public List<JobRun> getRunning() {
		try {
			return JobManager.getInstance().getRunning();
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@POST
	@Path("/job/running/{name}")
	@Timed
	public Integer run(@PathParam("name") String name) {
		try {
			Job job = getJob(name);
			return JobManager.getInstance().run(name, job).getRunId();
		}
		catch (NameNotFoundException ex) {
			throw new WebApplicationException(jobNotFoundMessageStem + name, 404);
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@DELETE
	@Path("/job/running/{id}")
	@Timed
	public void stop(@PathParam("id") int id) {
		try {
			JobManager.getInstance().stop(id);
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
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

			stmt = conn.prepareStatement(jobSql.insert);

			stmt.setString(2, name);
			stmt.setString(3, job.getScriptName());
			stmt.setString(4, job.getPropName());
			stmt.setInt(5, job.isEnabled() ? 1 : 0);

			int jobId;
			synchronized (this) {

				jobId = CommonSql.getNextId(conn, jobSql.selectLastId);

				stmt.setInt(1, jobId);

				stmt.executeUpdate();
			}

			stmt.close();
			stmt = null;

			// Write the arguments.

			stmt = conn.prepareStatement(argumentSql.insert);

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

			stmt = conn.prepareStatement(jobScheduleSql.insert);

			for (int scheduleId : scheduleIds) {

				stmt.setInt(1, jobId);
				stmt.setInt(2, scheduleId);

				stmt.addBatch();
			}

			stmt.executeBatch();

			return jobId;
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection(null);
		}
	}

	private List<Integer> getScheduleIds(Connection conn, ScheduleSql scheduleSql, List<String> names) throws Exception {

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
	 * @throws Exception if an error occurs
	 */
	public Map<String, Job> getJobs(String likeName) throws Exception {

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
				String propName = rs.getString(4);
				if ((propName != null) && propName.isEmpty()) {
					propName = null;
				}
				boolean enabled = (rs.getInt(5) == 1);

				List<ScriptArgument> arguments = getArguments(conn, argumentSql, id);

				List<String> scheduleNames = getJobScheduleNames(conn, jobScheduleSql, id);

				jobs.put(jobName, new Job(scriptName, propName, arguments, scheduleNames, enabled));
			}
		}
		finally {
			try { if (rs != null) rs.close(); } catch (Exception exx) {}
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection(null);
		}

		return jobs;
	}

	private List<ScriptArgument> getArguments(Connection conn, ArgumentSql argumentSql, int jobId) throws Exception {

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

	private List<String> getJobScheduleNames(Connection conn, JobScheduleSql jobScheduleSql, int jobId) throws Exception {

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
	 * @return the job if it exists or null if it does not
	 * @throws Exception if an error occurs
	 */
	public Job getJob(String name) throws Exception {

		Map<String, Job> jobs = getJobs(name);

		return (jobs.size() == 1) ? jobs.get(name) : null;
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

		JobManager manager = JobManager.getInstance();
		Context context = manager.getContext();
		JobSql jobSql = manager.getJobSql();
		ArgumentSql argumentSql = manager.getArgumentSql();
		JobScheduleSql jobScheduleSql = manager.getJobScheduleSql();

		Connection conn = null;

		try {
			conn = context.getConnection(null);

			int jobId = CommonSql.getId(conn, jobSql.selectId, name, "Job");

			execute(conn, jobScheduleSql.delete, jobId);
			execute(conn, argumentSql.delete, jobId);
			execute(conn, jobSql.delete, jobId);
		}
		finally {
			if (conn != null) context.releaseConnection(null);
		}
	}

	private void execute(Connection conn, String sql, int jobId) throws SQLException {

		PreparedStatement stmt = null;

		try {
			stmt = conn.prepareStatement(sql);

			stmt.setInt(1, jobId);

			stmt.executeUpdate();
		}
		finally {
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
	 * @throws Exception if any error occurs
	 */
	public List<JobRun> getJobRuns(String likeName, boolean latest) throws Exception {

		JobManager manager = JobManager.getInstance();
		Context context = manager.getContext();
		RunSql runSql = manager.getRunSql();

		if (likeName == null) {
			likeName = "%";
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
}
