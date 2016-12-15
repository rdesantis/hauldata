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

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.List;

import javax.ws.rs.WebApplicationException;

import com.hauldata.dbpa.log.Analyzer;
import com.hauldata.dbpa.manage.JobManager;
import com.hauldata.dbpa.manage_control.api.Job;
import com.hauldata.dbpa.manage_control.api.JobRun;
import com.hauldata.dbpa.manage_control.api.ScriptArgument;
import com.hauldata.dbpa.manage_control.api.JobStatus;
import com.hauldata.dbpa.manage_control.api.JobState;

import junit.framework.TestCase;

public class JobsResourceTest extends TestCase {

	private static final String mondayScheduleName = "Monday";
	private static final String mondayScheduleBody = "Every Monday from '11/9/2015' until '11/16/2015' at '4:00 PM', '11/13/2015' every 2 hours from '10:30 AM' until '8:30 PM'";

	private static final String dailyScheduleName = "Daily";
	private static final String dailyScheduleBody = "Daily";

	private static final String crapScriptName = "crap";
	private static final String crapScriptBody = "TASK LOG 'It may be crap, but it''s MY crap' END TASK";

	private static final String invalidScriptName = "invalid";
	private static final String invalidScriptBody = "This is not a valid script";

	private static final String hasParamsScriptName = "HasParams";
	private static final String hasParamsScriptBody = "PARAMETERS one int, two VARCHAR(50), three DATE END PARAMETERS" + "\r\n" + "TASK GO END TASK" + "\r\n";

	private static final String detritusName = "detritus";
	private static final String differentName = "NotGarbage";
	private static final String bogusName = "DoesNotExist";

	private static final String sleepJobName = "sleep5";
	private static final String sleepScript = "TASK" + "\r\n\t" + "WAITFOR DELAY '00:00:05'" + "\r\n" + "END TASK" + "\r\n";

	JobsResource jobsResource;
	Job detritusJob;
	List<ScriptArgument> someArguments;
	List<String> someSchedules;

	public JobsResourceTest(String name) {
		super(name);
	}

	protected void setUp() throws SQLException {

		JobManager.instantiate(true).startup();

		jobsResource = new JobsResource();

		ScriptsResource scriptsResource = new ScriptsResource();

		scriptsResource.put(crapScriptName, crapScriptBody);
		scriptsResource.put(invalidScriptName, invalidScriptBody);
		scriptsResource.put(hasParamsScriptName, hasParamsScriptBody);

		someArguments = new LinkedList<ScriptArgument>();
		someArguments.add(new ScriptArgument("first", "value1"));
		someArguments.add(new ScriptArgument("second", "value2"));
		someArguments.add(new ScriptArgument("third", "value3"));

		SchedulesResource schedulesResource = new SchedulesResource();

		schedulesResource.put(mondayScheduleName, mondayScheduleBody);
		schedulesResource.put(dailyScheduleName, dailyScheduleBody);

		someSchedules = new LinkedList<String>();
		someSchedules.add("Monday");
		someSchedules.add("Daily");

		detritusJob = new Job(crapScriptName, "fred", someArguments, someSchedules, true);
	}

	protected void tearDown() throws InterruptedException {

		// Delete any job with schedules so those jobs don't get scheduled at next setUp().

		deleteNoError(detritusName);

		final int scriptCount = 9;
		for (int i = 1; i <= scriptCount; i++) {
			String garbageNameN = detritusName + String.valueOf(i);
			deleteNoError(garbageNameN);
		}

		// Stop the scheduler, etc.

		JobManager.getInstance().shutdown();
		JobManager.killInstance();
	}

	public void testPut() {

		deleteNoError(detritusName);

		int id = jobsResource.put(detritusName, detritusJob);

		assertTrue(id != -1);
	}

	public void testPutOver() {

		Job job = new Job(crapScriptName, "ethel", someArguments, someSchedules, true);

		int id = jobsResource.put(detritusName, job);

		assertTrue(id != -1);

		int newId = jobsResource.put(detritusName, job);

		assertEquals(id, newId);
	}

	public void testGetPositive() {

		// Positive test: can get an existing job

		testPut();

		Job checkJob = jobsResource.get(detritusName);

		assertEquals(detritusJob.getScriptName(), checkJob.getScriptName());
		assertEquals(detritusJob.getPropName(), checkJob.getPropName());
		assertEquals(detritusJob.getArguments().size(), checkJob.getArguments().size());
		for (int i = 0; i < detritusJob.getArguments().size(); i++) {
			assertEquals(detritusJob.getArguments().get(i).getName(), checkJob.getArguments().get(i).getName());
			assertEquals(detritusJob.getArguments().get(i).getValue(), checkJob.getArguments().get(i).getValue());
		}
		assertEquals(detritusJob.getScheduleNames().size(), checkJob.getScheduleNames().size());
		for (int i = 0; i < detritusJob.getScheduleNames().size(); i++) {
			assertEquals(detritusJob.getScheduleNames().get(i), checkJob.getScheduleNames().get(i));
		}
		assertEquals(detritusJob.isEnabled(), checkJob.isEnabled());
	}

	public void testGetCaseMismatch() {

		// Try getting an existing job with the wrong case in the name.
		// The initial JobsResource.get(String) threw an exception for this case.
		// The actual result would depend on whether the database is case-sensitive
		// for string comparisons.  We don't care here whether the job is actually
		// found or not.  We just want to be sure the call doesn't throw an exception.

		testPut();

		String wrongCaseName = detritusName.toUpperCase();
		jobsResource.get(wrongCaseName);
	}

	public void testGetNegative() {

		// Negative test: attempt to get a non-existent job fails as expected

		deleteNoError(bogusName);

		boolean isNonExistentGotten;
		try {
			jobsResource.get(bogusName);
			isNonExistentGotten = true;
		}
		catch (Exception ex) {
			assertIsWebAppException(notFoundMessage(bogusName), 404, ex);
			isNonExistentGotten = false;
		}

		assertFalse(isNonExistentGotten);
	}

	public void testDeletePositive() {

		// Positive test: can get an existing job

		testPut();

		jobsResource.delete(detritusName);
	}

	public void testDeleteNegative() {

		// Negative test: attempt to delete a non-existent job fails as expected

		deleteNoError(bogusName);

		boolean isNonExistentDeleted;
		try {
			jobsResource.delete(bogusName);
			isNonExistentDeleted = true;
		}
		catch (Exception ex) {
			assertIsWebAppException(notFoundMessage(bogusName), 404, ex);
			isNonExistentDeleted = false;
		}

		assertFalse(isNonExistentDeleted);
	}

	public void testGetNames() {

		// Create a bunch of like-named jobs and a not-like-named job.

		final int scriptCount = 9;
		for (int i = 1; i <= scriptCount; i++) {
			String garbageNameN = detritusName + String.valueOf(i);

			deleteNoError(garbageNameN);
			Job job = new Job(crapScriptName, garbageNameN, someArguments, someSchedules, true);
			jobsResource.put(garbageNameN, job);
		}
		deleteNoError(differentName);
		Job differentJob = new Job(crapScriptName, differentName, null, null, false);
		jobsResource.put(differentName, differentJob);

		// Confirm all like named are found.  There may be other similarly named jobs too.

		List<String> likeNames = jobsResource.getNames(detritusName + "%");

		assertTrue(scriptCount <= likeNames.size());
		for (String name : likeNames) {
			assertTrue(name.startsWith(detritusName));
		}

		// Confirm that an all-names list is bigger than the like-named list.

		List<String> allNames = jobsResource.getNames(null);

		assertTrue(likeNames.size() < allNames.size());
	}

	public void testRun() {

		ScriptsResource scriptsResource = new ScriptsResource();
		scriptsResource.put(sleepJobName, sleepScript);
		assertTrue(scriptsResource.validate(sleepJobName).isValid());

		Job job = new Job(sleepJobName, null, null, null, true);
		jobsResource.put(sleepJobName, job);

		int id = jobsResource.run(sleepJobName);

		try { Thread.sleep(1000L); } catch (InterruptedException e) {}

		List<JobRun> running = jobsResource.getRunning();

		assertTrue(running.stream().anyMatch(r -> (r.getRunId() == id)));

		List<JobRun> runs = jobsResource.getRuns(sleepJobName, true);

		assertEquals(1, runs.size());

		try { Thread.sleep(5000L); } catch (InterruptedException e) {}

		running = jobsResource.getRunning();

		assertTrue(running.stream().noneMatch(r -> (r.getRunId() == id)));

		runs = jobsResource.getRuns(sleepJobName, true);

		assertEquals(1, runs.size());

		JobRun run = runs.get(0);
		JobState state = run.getState();

		assertEquals(id, run.getRunId());
		assertEquals(sleepJobName, run.getJobName());
		assertEquals(JobStatus.runSucceeded, state.getStatus());
		assertTrue(state.getEndTime().isBefore(LocalDateTime.now()));
		assertTrue(state.getStartTime().until(state.getEndTime(), ChronoUnit.SECONDS) >= 5);
	}

	public void testStopRun() {

		String stopJobName = sleepJobName + "stop";

		ScriptsResource scriptsResource = new ScriptsResource();
		scriptsResource.put(stopJobName, sleepScript);
		assertTrue(scriptsResource.validate(stopJobName).isValid());

		Job job = new Job(stopJobName, null, null, null, true);
		jobsResource.put(stopJobName, job);

		int id = jobsResource.run(stopJobName);

		try { Thread.sleep(1000L); } catch (InterruptedException e) {}

		jobsResource.stop(id);

		try { Thread.sleep(1000L); } catch (InterruptedException e) {}

		List<JobRun> running = jobsResource.getRunning();

		assertTrue(running.stream().noneMatch(r -> (r.getRunId() == id)));

		List<JobRun> runs = jobsResource.getRuns(stopJobName, true);

		assertEquals(1, runs.size());

		JobRun run = runs.get(0);
		JobState state = run.getState();

		assertEquals(id, run.getRunId());
		assertEquals(stopJobName, run.getJobName());
		assertEquals(JobStatus.runTerminated, state.getStatus());
		assertTrue(state.getEndTime().isBefore(LocalDateTime.now()));
		assertTrue(state.getStartTime().until(state.getEndTime(), ChronoUnit.SECONDS) < 5);
	}

	public void testScheduleRun() {

		final String scriptName = "DO_NOTHING";
		final String taskName = "SAY_NOT_MUCH";
		final String scheduleName = "Short and sweet";
		final String jobName = "Short scheduled job";

		final int iterations = 3;
		final int durationSeconds = 2;

		// Save the script.

		String script = "TASK " + taskName + " LOG 'ScheduledJob' END TASK";

		ScriptsResource scriptsResource = new ScriptsResource();
		scriptsResource.put(scriptName, script);
		assertTrue(scriptsResource.validate(scriptName).isValid());

		// Save the schedule.

		LocalTime start = LocalTime.now().plusSeconds(durationSeconds);
		LocalTime end = start.plusSeconds((iterations - 1) * durationSeconds);

		String schedule =
				"TODAY NOW, TODAY EVERY 2 SECONDS FROM '" +
				start.format(DateTimeFormatter.ofPattern("HH:mm:ss")) + "' UNTIL '" +
				end.format(DateTimeFormatter.ofPattern("HH:mm:ss")) + "'";

		SchedulesResource schedulesResource = new SchedulesResource();
		schedulesResource.put(scheduleName, schedule);
		assertTrue(schedulesResource.validate(scheduleName).isValid());

		List<String> scheduleNames = new LinkedList<String>();
		scheduleNames.add(scheduleName);

		// Save the job, which will schedule it.
		// Then wait long enough for it to complete.

		Job job = new Job(scriptName, null, null, scheduleNames, true);
		jobsResource.put(jobName, job);

		try {
			Thread.sleep((iterations * durationSeconds + 1) * 1000L);
		}
		catch (InterruptedException e) {}

		// Analyze the job log.
		// See TaskSetTest#testSchedule() for comprehensive log analysis code for this case.

		Analyzer analyzer = JobManager.getInstance().getAnalyzer();
		Analyzer.RecordIterator recordIterator = analyzer.recordIterator(jobName, taskName);

		recordIterator.next();
		recordIterator.next();
		recordIterator.next();
		recordIterator.next();

		assertFalse(recordIterator.hasNext());

		// Delete the job so it doesn't run the next time the job manager is started.

		jobsResource.delete(jobName);
	}

	private void deleteNoError(String name) {
		try { jobsResource.delete(name); } catch (Exception ex) {}
	}

	private String notFoundMessage(String name) {
		return JobsResource.jobNotFoundMessageStem + name;
	}

	private void assertIsWebAppException(String message, int status, Exception ex) {
		assertTrue(ex instanceof WebApplicationException);
		WebApplicationException wex = (WebApplicationException)ex;
		assertEquals(message, wex.getMessage());
		assertEquals(status, wex.getResponse().getStatus());
	}
}
