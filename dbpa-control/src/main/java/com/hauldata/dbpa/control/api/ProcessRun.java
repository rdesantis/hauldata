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

package com.hauldata.dbpa.control.api;

import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ProcessRun {

	private String configName;
	private int configId;
	private Integer runIndex;
	private Status status;
	private LocalDateTime startTime;
	private LocalDateTime endTime;

	public enum Status {
		notRun, parseFailed, runInProgress, runFailed, runSucceeded, runTerminated, controllerShutdown;
		
		private static Status[] statuses = values();
		
		public static Status of(int i) { return statuses[i]; } 
		public int value() { return ordinal(); }
	}

	public ProcessRun() {
		// Jackson deserialization
	}

	public ProcessRun(
			String configName,
			int configId,
			Integer runIndex,
			Status status,
			LocalDateTime startTime,
			LocalDateTime endTime) {

		this.configName = configName;
		this.configId = configId;
		this.runIndex = runIndex;
		this.status = status;
		this.startTime = startTime;
		this.endTime = endTime;
	}

	public ProcessRun(String configName, int configId) {

		this.configName = configName;
		this.configId = configId;

		runIndex = null;
		status = Status.notRun;
		startTime = null;
		endTime = null;
	}

	/**
	 * Construct a ProcessRun object from the fields that uniquely identify a run.
	 * 
	 * If instantiated from this constructor, the equals() member will return true
	 * when compared to any other ProcessRun with the same values for the fields
	 * entered on this constructor, regardless of the values of any other fields.
	 */
	public ProcessRun(int configId, int runIndex) {

		configName = null;

		this.configId = configId;
		this.runIndex = runIndex;

		status = null;
		startTime = null;
		endTime = null;
	}

	@Override
	public int hashCode() {
		return (this.configId << 16) ^ this.runIndex;
	}

	/**
	 * @return true if obj is a ProcessRun and the fields that uniquely identify a run
	 * have the same values as this object, regardless of the values of any other fields. 
	 */
	@Override
	public boolean equals(Object obj) {
		return (obj != null) && (obj instanceof ProcessRun) && (((ProcessRun)obj).configId == this.configId) && (((ProcessRun)obj).runIndex == this.runIndex);
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

	public void setRunIndex(Integer runIndex) {
		this.runIndex = runIndex;
	}

	// Getters

	@JsonProperty
	public String getConfigName() {
		return configName;
	}

	@JsonProperty
	public int getConfigId() {
		return configId;
	}

	@JsonProperty
	public Integer getRunIndex() {
		return runIndex;
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
