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

	private Integer runId;
	private String jobName;
	private Status status;
	private LocalDateTime startTime;
	private LocalDateTime endTime;

	public enum Status {
		notRun, parseFailed, runInProgress, runFailed, runSucceeded, runTerminated, controllerShutdown;
		
		private static Status[] statuses = values();
		
		public static Status of(int i) { return statuses[i]; } 
		public int value() { return ordinal(); }
	}

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
		this.status = Status.notRun;
		this.startTime = null;
		this.endTime = null;
	}

	public JobRun(
			Integer runId,
			String jobName,
			Status status,
			LocalDateTime startTime,
			LocalDateTime endTime) {

		this.runId = runId;
		this.jobName = jobName;
		this.status = status;
		this.startTime = startTime;
		this.endTime = endTime;
	}

	/*
	 * Constructor that sets the runId field, which uniquely identifies a job.
	 * 
	 * It is guaranteed that the equals() operator returns true when comparing
	 * this JobRun to another JobRun with the same runId.
	 */
	public static JobRun of(Integer runId) {
		return new JobRun(runId, null, null, null, null);
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

	// Status updates

	public void runInProgress() {
		status = Status.runInProgress;
		startTime = LocalDateTime.now();
	}

	public void parseFailed() {
		setEndStatusNow(Status.parseFailed);
	}

	public void runFailed() {
		setEndStatusNow(Status.runFailed);
	}

	public void runSucceeded() {
		setEndStatusNow(Status.runSucceeded);
	}

	public void runTerminated() {
		setEndStatusNow(Status.runTerminated);
	}

	public void controllerShutdown() {
		setEndStatusNow(Status.controllerShutdown);
	}

	public void setEndStatusNow(Status status) {
		this.status = status;
		endTime = LocalDateTime.now();
	}

	// Setter

	public void setRunId(int runId) {
		this.runId = runId;
	}

	// Getters

	@JsonProperty
	public Integer getRunId() {
		return runId;
	}

	@JsonProperty
	public String getJobName() {
		return jobName;
	}

	@JsonProperty
	public Status getStatus() {
		return status;
	}

	@JsonProperty
	public LocalDateTime getStartTime() {
		return startTime;
	}

	@JsonProperty
	public LocalDateTime getEndTime() {
		return endTime;
	}
}
