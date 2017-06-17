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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.LoggerFactory;

import com.hauldata.dbpa.ManageDbp;
import com.hauldata.dbpa.manage.resources.JobsResource;
import com.hauldata.dbpa.manage.sql.JobScheduleSql;
import com.hauldata.dbpa.manage.sql.ScheduleSql;
import com.hauldata.dbpa.process.Context;
import com.hauldata.util.schedule.ScheduleSet;

public class JobScheduler {

	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ManageDbp.class);

	private static final int shutdownWaitSeconds = 10;
	private static final long shutdownWaitMillis = shutdownWaitSeconds * 1000L;

	private Map<Integer, Thread> scheduleThreads;
	private JobsResource jobsResource;

	private class RunningSchedule implements Runnable {

		private int scheduleId;
		private ScheduleSet schedules;

		public RunningSchedule(int scheduleId, ScheduleSet schedules) {
			this.scheduleId = scheduleId;
			this.schedules = schedules;
		}

		@Override
		public void run() {

			try {
				if (schedules.isImmediate()) {
					runJobs(scheduleId);
				}

				while (schedules.sleepUntilNext()) {
					runJobs(scheduleId);
				}
			}
			catch (InterruptedException ex) {
				// The schedule has been interrupted to shut it down.
			}

			// The thread remains in the map even though it is terminated.  This indicates
			// that jobs exist to run on this schedule even if the schedule has expired.
		}
	}

	public JobScheduler() {
		// Time-critical testing must be done on the thread map that cannot be protected using ConcurrentHashMap alone.
		// So use regular HashMap and explicitly synchronize access.

		scheduleThreads = new HashMap<Integer, Thread>();
		jobsResource = new JobsResource(); 
	}

	/**
	 * Start all schedules for all enabled jobs.
	 * <p>
	 * Starting a schedule means that a thread is created to run jobs on that schedule.
	 * The thread sleeps until a scheduled job start time.  When a start time occurs,
	 * the thread wakes, launches each scheduled job, and then the schedule thread goes
	 * back to sleep until the next scheduled start time.
	 *
	 * If the schedule has an end time, the schedule thread dies after the last start time.
	 * @throws SQLException 
	 */
	public void startAll() throws SQLException {

		JobManager manager = JobManager.getInstance();
		Context context = manager.getContext();
		JobScheduleSql jobScheduleSql = manager.getJobScheduleSql();

		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			conn = context.getConnection(null);

			stmt = conn.prepareStatement(jobScheduleSql.selectEnabledJobSchedules, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			rs = stmt.executeQuery();

			while (rs.next()) {

				int id = rs.getInt(1);
				String name = rs.getString(2);
				String schedule = rs.getString(3);

				start(id, name, schedule);
			}
		}
		finally {
			try { if (rs != null) rs.close(); } catch (Exception exx) {}
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection(null);
		}
	}

	/**
	 * Start the specific schedule.
	 * @param id
	 * @param name
	 * @param schedule
	 */
	private void start(int id, String name, String schedule) {

		ScheduleSet schedules;
		try {
			schedules = ScheduleSet.parse(schedule);
		}
		catch (Exception ex) {
			LOGGER.error("Cannot start schedule: " + name, ex.getMessage());
			return;
		}

		Thread scheduleThread = new Thread(new RunningSchedule(id, schedules));
		synchronized (scheduleThreads) {
			scheduleThreads.put(id, scheduleThread);
		}
		scheduleThread.start();
	}

	/**
	 * Revise a schedule.
	 * <p>
	 * If the schedule with the indicated id was previously started and not subsequently stopped by
	 * stopSchedule() or stopAllSchedules(), the scheduled is stopped if running and restarted as revised.
	 * If the schedule expired but was not explicitly stopped, it is restarted on the assumption that
	 * after the revised it may now run.
	 * @param id identifies the schedule that was revised
	 */
	public void revise(int id, String name, String schedule) {

		try {
			if (stop(id)) {
				start(id, name, schedule);
			}
		}
		catch (InterruptedException ex) {
			// A new interrupt came in while waiting for the old schedule to terminate.
		}
	}

	/**
	 * Return a list of the running schedules.
	 * <p>
	 * @return a list of IDs of running schedules.
	 */
	public List<Integer> getRunning() {
		return scheduleThreads.entrySet().stream().filter(e -> e.getValue().isAlive()).map(e -> e.getKey()).collect(Collectors.toList());
	}

	/**
	 * Stop the specific schedule if it is running.
	 * <p>
	 * @param id
	 * @return true if the schedule was previously started and not subsequently stopped by stopSchedule()
	 * or stopAllSchedules(), false otherwise.  Will return true if the schedule expired but was not explicitly stopped.
	 * @throws InterruptedException if this thread is interrupted while waiting for the schedule to stop
	 */
	private boolean stop(int id) throws InterruptedException {

		// If the thread is running, remove it from the map, interrupt it, and wait a modest time
		// for it to die so the caller has some likelihood that any jobs that happen to have been
		// running on the schedule have terminated.

		Thread scheduleThread;

		synchronized (scheduleThreads) {

			scheduleThread = scheduleThreads.get(id);

			if (scheduleThread == null) {
				return false;
			}

			scheduleThreads.remove(id, scheduleThread);
		}

		// Note that the thread may already have died, in which case interrupt() and join()
		// have no effect.

		scheduleThread.interrupt();

		scheduleThread.join(shutdownWaitMillis);

		return true;
	}

	/**
	 * Stop all running schedules.
	 * <p>
	 * This function interrupts each schedule thread which should cause it to
	 * promptly terminate, but it takes no action to stop jobs that may have been 
	 * launched by a schedule.  Caller is responsible for stopping the jobs.
	 */
	public void stopAll() {

		synchronized (scheduleThreads) {
			for (Thread scheduleThread : scheduleThreads.values()) {
				scheduleThread.interrupt();
			}
			scheduleThreads.clear();
		}
	}

	/**
	 * Start the indicated schedules if not running.
	 * <p>
	 * Some or all of the schedules may already be running.  If so, no action is taken on them.
	 * If any of indicated schedules have expired but were not explicitly stopped, they are restarted
	 * on the assumption that any of them may have been revised and will now run.
	 * @param scheduleIds is the list of IDs of schedules to run
	 * @throws SQLException 
	 */
	public void start(List<Integer> scheduleIds) throws SQLException {

		List<Integer> notRunningIds;
		synchronized (scheduleThreads) {
			notRunningIds = scheduleIds.stream()
					.filter(id -> (!scheduleThreads.containsKey(id) || !scheduleThreads.get(id).isAlive()))
					.collect(Collectors.toList());
		}
		if (notRunningIds.isEmpty()) {
			return;
		}

		JobManager manager = JobManager.getInstance();
		Context context = manager.getContext();
		ScheduleSql scheduleSql = manager.getScheduleSql();

		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			String paramList = notRunningIds.stream().map(id -> "?").collect(Collectors.joining(","));
			String selectAllColumnsByIds = String.format(scheduleSql.selectAllColumnsByIds, paramList);

			conn = context.getConnection(null);

			stmt = conn.prepareStatement(selectAllColumnsByIds, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			int paramIndex = 1;
			for (int id : notRunningIds) {
				stmt.setInt(paramIndex++, id);
			}

			rs = stmt.executeQuery();

			while (rs.next()) {

				int id = rs.getInt(1);
				String name = rs.getString(2);
				String schedule = rs.getString(3);

				start(id, name, schedule);
			}
		}
		finally {
			try { if (rs != null) rs.close(); } catch (Exception exx) {}
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection(null);
		}
	}

	/**
	 * Drop the schedules for the indicated job if necessary
	 * <p>
	 * For the running schedules of the indicated job, if it is the only job
	 * associated with those schedules, stop them.  If any other job(s) hold any
	 * of the schedule(s), allow those schedules to continue running. 
	 * @param jobId identifies the job whose schedules are to be dropped
	 */
//	public void dropJobSchedules(int jobId) {
		// DON'T IMPLEMENT THIS.
		// It's too much work, too resource intensive at runtime and gives very little benefit.
		// The only down side is that if the last job is removed from a schedule, the
		// schedule will continue to run, but when it wakes it won't start any jobs.  Who cares?
		// Could detect the "no enabled jobs upon wake" and drop the schedule at that time,
		// but that is vulnerable to race condition.  So just don't bother.
//	}

	/**
	 * Run the jobs on the indicated schedule
	 * @param scheduleId
	 */
	private void runJobs(int scheduleId) {

		JobManager manager = JobManager.getInstance();
		Context context = manager.getContext();
		JobScheduleSql jobScheduleSql = manager.getJobScheduleSql();

		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;

		String name = "[not retrieved]";

		try {
			conn = context.getConnection(null);

			stmt = conn.prepareStatement(jobScheduleSql.selectEnabledJobNamesByScheduleId, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			stmt.setInt(1, scheduleId);

			rs = stmt.executeQuery();

			while (rs.next()) {

				name = rs.getString(1);

				jobsResource.run(name);
			}
		}
		catch (SQLException se) {
			LOGGER.error("Database query to retrieve scheduled job names failed", se.getMessage());
		}
		catch (RuntimeException re) {
			LOGGER.error("Startup of scheduled job failed: " + name, re.getMessage());
		}
		finally {
			try { if (rs != null) rs.close(); } catch (Exception exx) {}
			try { if (stmt != null) stmt.close(); } catch (Exception exx) {}

			if (conn != null) context.releaseConnection(null);
		}
	}
}
