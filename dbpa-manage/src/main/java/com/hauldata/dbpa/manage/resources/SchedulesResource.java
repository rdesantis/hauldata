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

package com.hauldata.dbpa.manage.resources;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.naming.NameNotFoundException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;
import com.hauldata.dbpa.manage.JobManager;
import com.hauldata.dbpa.manage.sql.CommonSql;
import com.hauldata.dbpa.manage.sql.JobScheduleSql;
import com.hauldata.dbpa.manage.sql.ScheduleSql;
import com.hauldata.dbpa.manage_control.api.ScheduleValidation;
import com.hauldata.dbpa.process.Context;
import com.hauldata.util.schedule.ScheduleSet;

@Path("/schedules")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class SchedulesResource {

	public static final String scheduleNotFoundMessageStem = CommonSql.getNotFoundMessageStem( "Schedule");

	public SchedulesResource() {}

	/**
	 * Store a schedule in the database.
	 *
	 * @param name is the schedule name.
	 * @param schedule is the schedule to store.
	 * @return the unique schedule ID created for the schedule.  If a schedule by the same name
	 * already existed, it is overwritten and the ID is not changed.
	 * @throws SQLException if the schedule cannot be stored
	 */
	@PUT
	@Path("{name}")
	@Timed
	public int put(@PathParam("name") String name, String schedule) throws SQLException {

		try {
			return put(name, schedule, false);
		}
		catch (NameNotFoundException ex) {
			// This exception can't happen.
			throw new RuntimeException("Internal error in SchedulesResource.put(String,String): " + ex.toString());
		}
	}

	/**
	 * Update the definition of a schedule in the database.
	 *
	 * @param name is the schedule name.
	 * @param body is the schedule to store.
	 * @throws NameNotFoundException if the schedule does not exist
	 * @throws SQLException if the schedule cannot be updated for any reason
	 */
	@PUT
	@Path("{name}/body")
	@Timed
	public void putBody(@PathParam("name") String name, String body) throws NameNotFoundException, SQLException {

		put(name, body, true);
	}

	private int put(String name, String schedule, boolean mustExist) throws NameNotFoundException, SQLException {

		// Validate schedule; throws RuntimeException if not valid.

		ScheduleSet.parse(schedule);

		JobManager manager = JobManager.getInstance();
		Context context = manager.getContext();
		ScheduleSql sql = manager.getScheduleSql();

		Connection conn = null;
		PreparedStatement stmt = null;

		try {
			conn = context.getConnection(null);

			int id = CommonSql.getId(conn, sql.selectId, name);

			if (id != -1) {
				// Update existing schedule

				CommonSql.execute(conn, sql.update, "schedule", schedule, id);

				// Let the job scheduler know that the schedule has been revised.

				manager.getScheduler().revise(id, name, schedule);
			}
			else if (!mustExist) {
				// Create new schedule.

				stmt = conn.prepareStatement(sql.insert);

				stmt.setString(2, name);
				stmt.setString(3, schedule);

				synchronized (this) {

					id = CommonSql.getNextId(conn, sql.selectLastId);

					stmt.setInt(1, id);

					stmt.executeUpdate();
				}
			}
			else {
				throw new NameNotFoundException(CommonSql.getNotFoundMessageStem("schedule") + name);
			}

			return id;
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection(null, conn);
		}
	}

	/**
	 * Rename a schedule
	 * @param name is the original name
	 * @param newName is the new name
	 */
	@PUT
	@Path("{name}/rename")
	public void rename(@PathParam("name") String name, String newName)  throws NameNotFoundException, SQLException {

		JobManager manager = JobManager.getInstance();
		Context context = manager.getContext();
		ScheduleSql sql = manager.getScheduleSql();

		Connection conn = null;

		try {
			conn = context.getConnection(null);

			int id = CommonSql.getId(conn, sql.selectId, name);

			if (id == -1) {
				throw new NameNotFoundException(CommonSql.getNotFoundMessageStem("schedule") + name);
			}

			CommonSql.execute(conn, sql.update, "name", newName, id);
		}
		finally {
			if (conn != null) context.releaseConnection(null, conn);
		}
	}

	/**
	 * Retrieve a set of schedule(s) from the database
	 * whose names match an optional SQL wildcard pattern
	 *
	 * @param likeName is the name wildcard pattern or null to get all schedules
	 * @return a map of schedule names to schedule strings retrieved from the database
	 * or an empty list if no schedule with a matching name is found
	 * @throws SQLException if the schedules cannot be retrieved
	 */
	@GET
	@Timed
	public Map<String, String> getAll(@QueryParam("like") String likeName) throws SQLException {

		JobManager manager = JobManager.getInstance();
		Context context = manager.getContext();
		ScheduleSql sql = manager.getScheduleSql();

		if (likeName == null) {
			likeName = "%";
		}

		Map<String, String> schedules = new HashMap<String, String>();

		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			conn = context.getConnection(null);

			stmt = conn.prepareStatement(sql.select, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			stmt.setString(1, likeName);

			rs = stmt.executeQuery();

			while (rs.next()) {

				String name = rs.getString(1);
				String schedule = rs.getString(2);

				schedules.put(name, schedule);
			}
		}
		finally {
			try { if (rs != null) rs.close(); } catch (Exception exx) {}
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection(null, conn);
		}

		return schedules;
	}

	/**
	 * Retrieve a list of schedule names from the database
	 * that match an optional SQL wildcard pattern
	 *
	 * @param likeName is the name wildcard pattern or null to get all schedule names
	 * @return a list of schedule names retrieved from the database
	 * or an empty list if no schedule with a matching name is found
	 * @throws SQLException if the schedule names cannot be retrieved
	 */
	@GET
	@Path("-/names")
	@Timed
	public List<String> getNames(@QueryParam("like") String likeName) throws SQLException {

		Map<String, String> schedules = getAll(likeName);
		if (schedules == null) {
			return null;
		}
		return schedules.keySet().stream().sorted().collect(Collectors.toList());
	}

	/**
	 * Return a schedule
	 *
	 * @param name is the name of the schedule
	 * @return the schedule
	 * @throws NameNotFoundException if the schedule does not exist
	 * @throws IllegalArgumentException if the name contains wildcards and matches multiple schedule names
	 * @throws SQLException if the schedule cannot be retrieved
	 */
	@GET
	@Path("{name}")
	@Timed
	public String get(@PathParam("name") String name) throws SQLException, NameNotFoundException, IllegalArgumentException {

		Map<String, String> schedules = getAll(name);

		if (schedules.size() == 0) {
			throw new NameNotFoundException(scheduleNotFoundMessageStem + name);
		}
		else if (1 < schedules.size()) {
			throw new IllegalArgumentException("Schedule name matches multiple schedules: " + name);
		}

		return schedules.values().stream().findFirst().orElse(null);
	}

	/**
	 * Delete a schedule
	 *
	 * @param name is the name of the schedule to delete
	 * @throws SQLException
	 * @throws NameNotFoundException if the schedule does not exist
	 * @throws SQLException if the schedule cannot be deleted
	 */
	@DELETE
	@Path("{name}")
	@Timed
	public void delete(@PathParam("name") String name) throws NameNotFoundException, SQLException {

		JobManager manager = JobManager.getInstance();
		Context context = manager.getContext();
		ScheduleSql sql = manager.getScheduleSql();

		Connection conn = null;
		PreparedStatement stmt = null;

		try {
			conn = context.getConnection(null);

			int id = CommonSql.getId(conn, sql.selectId, name, "Schedule");

			if (!getScheduleJobNames(conn, manager.getJobScheduleSql(), id).isEmpty()) {
				throw new RuntimeException("Cannot delete schedule that is in use by a job");
			}

			stmt = conn.prepareStatement(sql.delete);

			stmt.setInt(1, id);

			stmt.executeUpdate();
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection(null, conn);
		}
	}

	private List<String> getScheduleJobNames(Connection conn, JobScheduleSql jobScheduleSql, int scheduleId) throws SQLException {

		List<String> names = new LinkedList<String>();

		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			stmt = conn.prepareStatement(jobScheduleSql.selectJobNamesByScheduleId, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			stmt.setInt(1, scheduleId);

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
	 * Validate a schedule for syntax.
	 *
	 * @return the validation results.  Fields are:
	 *
	 *	isValid returns true if the schedule is valid syntactically, otherwise false;
	 *	validationMessage returns an error message if the validation failed, or null if it succeeded.
	 *
	 * @throws NameNotFoundException if the named schedule does not exist in the database
	 * @throws IllegalArgumentException if the name contains wildcards and matches multiple schedule names
	 * @throws SQLException if the schedule cannot be retrieved
	 */
	@GET
	@Path("-/validations/{name}")
	@Timed
	public ScheduleValidation validate(@PathParam("name") String name) throws SQLException, NameNotFoundException, IllegalArgumentException {

		String schedule = get(name);

		boolean valid = false;
		String validationMessage = null;

		try {
			ScheduleSet.parse(schedule);
			valid = true;
		}
		catch (RuntimeException ex) {
			validationMessage = ex.getMessage();
		}

		return new ScheduleValidation(valid, validationMessage);
	}

	/**
	 * Validate text for schedule syntax.  No schedule is saved or retrieved.
	 *
	 * @return the validation results.  Fields are:
	 *
	 *	isValid returns true if the text is a valid schedule syntactically, otherwise false;
	 *	validationMessage returns an error message if the validation failed, or null if it succeeded.
	 */
	@PUT
	@Path("validate")
	@Timed
	public ScheduleValidation validateBody(String body) throws SQLException, NameNotFoundException, IllegalArgumentException {

		boolean valid = false;
		String validationMessage = null;

		try {
			ScheduleSet.parse(body);
			valid = true;
		}
		catch (RuntimeException ex) {
			validationMessage = ex.getMessage();
		}

		return new ScheduleValidation(valid, validationMessage);
	}

	/**
	 * Return a list of the running schedules.
	 * <p>
	 * @return a list of names of running schedules.
	 * @throws SQLException
	 */
	@GET
	@Path("-/running")
	@Timed
	public List<String> getRunning() throws SQLException {

		JobManager manager = JobManager.getInstance();
		Context context = manager.getContext();
		ScheduleSql sql = manager.getScheduleSql();

		Connection conn = null;

		List<String> names = new LinkedList<String>();

		try {
			List<Integer> ids = manager.getScheduler().getRunning();

			if (!ids.isEmpty()) {

				conn = context.getConnection(null);

				names = getScheduleNames(conn, sql, ids);
			}
		}
		finally {
			if (conn != null) context.releaseConnection(null, conn);
		}

		return names;
	}

	private List<String> getScheduleNames(Connection conn, ScheduleSql scheduleSql, List<Integer> ids) throws SQLException {

		List<String> names = new LinkedList<String>();

		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			String paramList = ids.stream().map(id -> "?").collect(Collectors.joining(","));

			String selectNames = String.format(scheduleSql.selectNames, paramList);

			stmt = conn.prepareStatement(selectNames, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			int paramIndex = 1;
			for (int id : ids) {
				stmt.setInt(paramIndex++, id);
			}

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
}
