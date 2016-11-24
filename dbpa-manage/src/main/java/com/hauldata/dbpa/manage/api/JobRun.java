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

package com.hauldata.dbpa.manage.api;

import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Description of a single instance of a job run; the run may be in progress
 */
public class JobRun {

	private int runId;
	private String jobName;
	private JobState state;

	public JobRun() {
		// Jackson deserialization
	}

	/**
	 * Constructor for job not yet submitted or written to database
	 * @param jobName
	 */
	public JobRun(String jobName) {

		this.runId = -1;
		this.jobName = jobName;
		this.state = new JobState(JobStatus.notRun, null, null);
	}

	public JobRun(
			int runId,
			String jobName,
			JobState state) {

		this.runId = runId;
		this.jobName = jobName;
		this.state = state;
	}

	@Override
	public int hashCode() {
		return runId;
	}

	/**
	 * @return true if obj is a JobRun and the fields that uniquely identify a run
	 * have the same values as this object, regardless of the values of any other fields. 
	 */
	@Override
	public boolean equals(Object obj) {
		return (obj != null) && (obj instanceof JobRun) && (((JobRun)obj).runId == this.runId);
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + ":" +
				"{" +
				String.valueOf(runId) + "," +
				String.valueOf(jobName) + "," +
				state.toString() +
				"}";
	}

	// Status updates

	public void runInProgress() {
		state = new JobState(
							JobStatus.runInProgress,
							LocalDateTime.now(),
							null);
	}

	public void parseFailed() {
		setEndStatusNow(JobStatus.parseFailed);
	}

	public void runFailed() {
		setEndStatusNow(JobStatus.runFailed);
	}

	public void runSucceeded() {
		setEndStatusNow(JobStatus.runSucceeded);
	}

	public void runTerminated() {
		setEndStatusNow(JobStatus.runTerminated);
	}

	public void controllerShutdown() {
		setEndStatusNow(JobStatus.controllerShutdown);
	}

	public void setEndStatusNow(JobStatus status) {
		state = new JobState(
							status,
							state.getStartTime(),
							LocalDateTime.now());
	}

	// Setter

	public void setRunId(int runId) {
		this.runId = runId;
	}

	// Getters

	@JsonProperty
	public int getRunId() {
		return runId;
	}

	@JsonProperty
	public String getJobName() {
		return jobName;
	}

	/**
	 * Get job state.
	 *
	 * @return an immutable State object that returns the state of the job run at the time
	 * of this call.  If the state subsequently changes, getState() will return a different
	 * State object with the new state.
	 */
	@JsonProperty
	public JobState getState() {
		return state;
	}
}
