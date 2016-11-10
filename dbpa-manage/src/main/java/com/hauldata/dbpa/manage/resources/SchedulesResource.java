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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.naming.NameNotFoundException;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;

import com.codahale.metrics.annotation.Timed;
import com.hauldata.dbpa.manage.JobManager;
import com.hauldata.dbpa.manage.sql.CommonSql;
import com.hauldata.dbpa.manage.sql.JobScheduleSql;
import com.hauldata.dbpa.manage.sql.ScheduleSql;
import com.hauldata.dbpa.process.Context;

public class SchedulesResource {

	public SchedulesResource() {}

	@PUT
	@Path("/schedules/{name}")
	@Timed
	public void put(@PathParam("name") String name, String body) {
		try {
			putSchedule(name, body);
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@GET
	@Path("/schedules/{name}")
	@Timed
	public String get(@PathParam("name") String name) {
		try {
			return getSchedule(name);
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@DELETE
	@Path("/schedules/{name}")
	@Timed
	public void delete(@PathParam("name") String name) {
		try {
			deleteSchedule(name);
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@GET
	@Path("/schedules")
	@Timed
	public Map<String, String> get(@QueryParam("like") Optional<String> likeName) {
		try {
			return getSchedules(likeName.orElse(null));
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@GET
	@Path("/schedule/names")
	@Timed
	public List<String> getNames(@QueryParam("like") Optional<String> likeName) {
		try {
			Map<String, String> schedules = getSchedules(likeName.orElse(null));
			if (schedules == null) {
				return null;
			}
			return schedules.keySet().stream().sorted().collect(Collectors.toList());
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	/**
	 * Store a schedule in the database.
	 *
	 * @param name is the schedule name.
	 * @param schedule is the schedule to store.
	 * @return the unique schedule ID created for the schedule.
	 * @throws Exception if the job cannot be stored for any reason
	 */
	public int putSchedule(String name, String schedule)throws Exception {

		JobManager manager = JobManager.getInstance();
		Context context = manager.getContext();
		ScheduleSql sql = manager.getScheduleSql();

		Connection conn = null;
		PreparedStatement stmt = null;

		try {
			conn = context.getConnection(null);

			String insertSchedule = sql.insert;

			stmt = conn.prepareStatement(insertSchedule);

			stmt.setString(2, name);
			stmt.setString(3, schedule);

			int scheduleId;
			synchronized (this) {

				scheduleId = CommonSql.getNextId(conn, sql.selectLastId);

				stmt.setInt(1, scheduleId);

				stmt.executeUpdate();
			}

			stmt.close();
			stmt = null;

			return scheduleId;
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
	 * Retrieve a set of schedule(s) from the database
	 * whose names match an optional SQL wildcard pattern
	 *
	 * @param likeName is the name wildcard pattern or null to get all schedules
	 * @return a map of schedule names to schedule strings retrieved from the database
	 * or an empty list if no schedule with a matching name is found
	 * @throws Exception if an error occurs
	 */
	public Map<String, String> getSchedules(String likeName) throws Exception {

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

			String selectSchedule = sql.select;

			stmt = conn.prepareStatement(selectSchedule, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			stmt.setString(1, likeName);

			rs = stmt.executeQuery();

			while (rs.next()) {

				String name = rs.getString(1);
				String schedule = rs.getString(2);

				schedules.put(name, schedule);
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

		return schedules;
	}

	/**
	 * Return a schedule
	 *
	 * @param name is the name of the schedule
	 * @return the schedule if it exists or null if it does not
	 * @throws Exception if an error occurs
	 */
	public String getSchedule(String name) throws Exception {

		Map<String, String> schedules = getSchedules(name);

		return (schedules.size() == 1) ? schedules.get(0) : null;
	}

	/**
	 * Delete a schedule
	 *
	 * @param name is the name of the schedule to delete
	 * @throws NameNotFoundException if the job does not exist
	 * @throws Exception if any other error occurs
	 */
	public void deleteSchedule(String name) throws Exception {

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

			String deleteSchedule = sql.delete;

			stmt = conn.prepareStatement(deleteSchedule);

			stmt.setInt(1, id);

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

	private List<String> getScheduleJobNames(Connection conn, JobScheduleSql jobScheduleSql, int scheduleId) throws Exception {

		List<String> names = new LinkedList<String>();

		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			String selectNames = jobScheduleSql.selectJobNamesByScheduleId;

			stmt = conn.prepareStatement(selectNames, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			stmt.setInt(1, scheduleId);

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
}
