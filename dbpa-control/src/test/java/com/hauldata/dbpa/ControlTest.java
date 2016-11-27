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

package com.hauldata.dbpa;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;

import com.hauldata.dbpa.control.interfaces.Jobs;
import com.hauldata.dbpa.control.interfaces.Manager;
import com.hauldata.dbpa.control.interfaces.Schedules;
import com.hauldata.dbpa.control.interfaces.Schema;
import com.hauldata.dbpa.control.interfaces.Scripts;
import com.hauldata.dbpa.manage.api.Job;
import com.hauldata.dbpa.manage.api.JobRun;
import com.hauldata.dbpa.manage.api.ScheduleValidation;
import com.hauldata.ws.rs.client.WebClient;

import junit.framework.TestCase;

public class ControlTest extends TestCase {

	public ControlTest(String name) {
		super(name);
	}

	public void testEverything() throws ReflectiveOperationException {

		String baseUrl = "http://localhost:8080";

		WebClient client = new WebClient(baseUrl);

		Manager manager = (Manager)client.getResource(Manager.class);
		Schema schema = (Schema)client.getResource(Schema.class);
		Scripts scripts = (Scripts)client.getResource(Scripts.class);
		Schedules schedules = (Schedules)client.getResource(Schedules.class);
		Jobs jobs = (Jobs)client.getResource(Jobs.class);

		List<String> names;
		String name;
		String likeName;

		// Manager

		boolean isStarted = manager.isStarted();

		System.out.println();
		System.out.println("Manager is started? " + String.valueOf(isStarted));

		// Schema

		boolean isConfirmed = schema.confirm();

		System.out.println();
		System.out.println("Schema is confirmed? " + String.valueOf(isConfirmed));

		// Scripts

		// getNames

		likeName = null;
		names = null;

		System.out.println();

		try {
			names = scripts.getNames(likeName);
		}
		catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
		}

		System.out.println("List of script names:");
		if (names == null) {
			System.out.println("[empty]");
		}
		else {
			//System.out.println(names.toString());
			for (String scriptName : names) {
				System.out.println(scriptName);
			}
		}

		// Schedules

		// get

		name = "NotGarbage";
		String schedule = "[get failed]";

		System.out.println();

		try {
			schedule = schedules.get(name);
		}
		catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
		}

		System.out.println("Schedule '" + name + "' = " + schedule);

		// getNames

		likeName = "garbage%";

		System.out.println();

		try {
			names = schedules.getNames(likeName);
		}
		catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
		}

		System.out.println("List of schedule names:");
		if (names == null) {
			System.out.println("[empty]");
		}
		else {
			//System.out.println(names.toString());
			for (String scheduleName : names) {
				System.out.println(scheduleName);
			}
		}

		// validate

		String validateName = "invalid";
		ScheduleValidation validation = null;

		System.out.println();

		try {
			validation = schedules.validate(validateName);
		}
		catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
		}

		System.out.println("Schedule '" + validateName + "' validation:");
		if (validation == null) {
			System.out.println("[empty]");
		}
		else {
			System.out.println(validation.toString());
		}

		// put

		String putName = "Weekly Friday";
		String putSchedule = "WEEKLY FRIDAY";

		System.out.println();

		int id = -1;
		try {
			id = schedules.put(putName, putSchedule);
		}
		catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
		}

		System.out.println("Schedule '" + putName + "' id: " + String.valueOf(id));

		// delete

		String deletedName = "Doesn't exist";
		boolean deleted = false;

		System.out.println();

		try {
			schedules.delete(deletedName);
			deleted = true;
		}
		catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
		}

		System.out.println("Schedule '" + deletedName + "' deleted? " + String.valueOf(deleted));

		// get doesn't exist

		name = deletedName;
		schedule = "[get failed]";

		System.out.println();

		try {
			schedule = schedules.get(name);
		}
		catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
		}

		System.out.println("Schedule '" + name + "' = " + schedule);

		// Jobs

		// getNames

		likeName = null;
		names = null;

		System.out.println();

		try {
			names = jobs.getNames(likeName);
		}
		catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
		}

		System.out.println("List of job names:");
		if (names == null) {
			System.out.println("[empty]");
		}
		else {
			//System.out.println(names.toString());
			for (String jobName : names) {
				System.out.println(jobName);
			}
		}

		// get

		name = "NotGarbage";
		Job job = null;

		System.out.println();

		try {
			job = jobs.get(name);
		}
		catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
		}

		System.out.println("Job '" + name + "' = " + String.valueOf(job));

		JobRun run = null;

		// run

		String runName = "sleep5";

		System.out.println();

		id = -1;
		try {
			id = jobs.run(runName);
		}
		catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
		}

		System.out.println("Job run '" + runName + "' id: " + String.valueOf(id));

		// getRunning

		run = null;

		System.out.println();

		try {
			run = jobs.getRunning(id);
		}
		catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
		}

		System.out.println("Job now running:");
		if (run == null) {
			System.out.println("[get failed]");
		}
		else {
			System.out.println(run.toString());
		}

		List<JobRun> runs = null;

		// getRunning

		runs = null;

		System.out.println();

		try {
			runs = jobs.getRunning();
		}
		catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
		}

		System.out.println("List of jobs running:");
		if (runs == null) {
			System.out.println("[empty]");
		}
		else {
			for (JobRun jobRun : runs) {
				System.out.println(jobRun.toString());
			}
		}

		// getRuns

		runs = null;

		System.out.println();

		try {
			runs = jobs.getRuns("%", true);
		}
		catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
		}

		System.out.println("List of latest job runs:");
		if (runs == null) {
			System.out.println("[empty]");
		}
		else {
			for (JobRun jobRun : runs) {
				System.out.println(jobRun.toString());
			}
		}

		// Schedules - put

		putName = "quickie";

		LocalDateTime sooner = LocalDateTime.now().plusSeconds(2);
		LocalDateTime later = sooner.plusSeconds(2);
		putSchedule =
				"TODAY EVERY 2 SECONDS FROM '" +
				sooner.format(DateTimeFormatter.ofPattern("HH:mm:ss")) + "' UNTIL '" +
				later.format(DateTimeFormatter.ofPattern("HH:mm:ss")) + "'";

		System.out.println();

		id = -1;
		try {
			id = schedules.put(putName, putSchedule);
		}
		catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
		}

		System.out.println("Schedule '" + putName + "' id: " + String.valueOf(id));

		// Jobs - putScheduleNames, putEnabled(true)

		name = "NotGarbage";

		List<String> putNames = new LinkedList<String>();
		putNames.add(putName);

		System.out.println();

		try {
			jobs.putScheduleNames(name, putNames);
			jobs.putEnabled(name, true);
		}
		catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
		}

		System.out.println("Job '" + name + "' added schedule '" + putName + "'");

		// Schedules - getRunning

		names = null;

		System.out.println();

		try {
			names = schedules.getRunning();
		}
		catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
		}

		System.out.println("List of schedules running:");
		if (names == null) {
			System.out.println("[empty]");
		}
		else {
			for (String scheduleName : names) {
				System.out.println(scheduleName);
			}
		}

		// Wait for schedule to expire.

		try { Thread.sleep(5000); } catch (Exception ex) {}

		// getRunning again

		names = null;

		System.out.println();

		try {
			names = schedules.getRunning();
		}
		catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
		}

		System.out.println("List of schedules running:");
		if (names == null) {
			System.out.println("[empty]");
		}
		else {
			for (String scheduleName : names) {
				System.out.println(scheduleName);
			}
		}

		// Jobs - putEnabled(false)

		try {
			jobs.putEnabled(name, false);
		}
		catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
		}
	}
}
