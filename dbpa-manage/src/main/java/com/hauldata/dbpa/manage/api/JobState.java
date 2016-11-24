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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.joda.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.joda.ser.LocalDateTimeSerializer;

/**
 * All state-related fields; immutable
 */
public class JobState {
	
	private JobStatus status;
//	@JsonSerialize(using = LocalDateTimeSerializer.class)
//	@JsonDeserialize(using = LocalDateTimeDeserializer.class)
	@JsonIgnore
	private LocalDateTime startTime;
//	@JsonSerialize(using = LocalDateTimeSerializer.class)
//	@JsonDeserialize(using = LocalDateTimeDeserializer.class)
	@JsonIgnore
	private LocalDateTime endTime;

	public JobState() {
		// Jackson deserialization
	}

	public JobState(
			JobStatus status,
			LocalDateTime startTime,
			LocalDateTime endTime) {

		this.status = status;
		this.startTime = startTime;
		this.endTime = endTime;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + ":" +
				"{" +
				String.valueOf(status) + "," +
				String.valueOf(startTime) + "," +
				String.valueOf(endTime) +
				"}";
	}

	@JsonProperty
	public JobStatus getStatus() {
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