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
	private State state;

	public enum Status {

		notRun(0), parseFailed(1), runInProgress(2), runFailed(3), runSucceeded(4), runTerminated(5), controllerShutdown(6);

		private int id;

		Status(int id) { this.id = id; }
		
		public static Status valueOf(int id) {
			return values()[id];
		}

		@JsonProperty
		public int getId() {
			return id;
		}
	}

	/**
	 * All state-related fields; immutable
	 */
	public static class State {
		
		private Status status;
		private LocalDateTime startTime;
		private LocalDateTime endTime;

		public State() {
			// Jackson deserialization
		}

		public State(
				Status status,
				LocalDateTime startTime,
				LocalDateTime endTime) {

			this.status = status;
			this.startTime = startTime;
			this.endTime = endTime;
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
		this.state = new State(Status.notRun, null, null);
	}

	public JobRun(
			int runId,
			String jobName,
			State state) {

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

	// Status updates

	public void runInProgress() {
		state = new State(
							Status.runInProgress,
							LocalDateTime.now(),
							null);
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
		state = new State(
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
	public State getState() {
		return state;
	}
}
