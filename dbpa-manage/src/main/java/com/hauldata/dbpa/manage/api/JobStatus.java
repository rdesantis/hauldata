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

import com.fasterxml.jackson.annotation.JsonProperty;

public enum JobStatus {

	notRun(0), parseFailed(1), runInProgress(2), runFailed(3), runSucceeded(4), runTerminated(5), controllerShutdown(6);

	private int id;

	JobStatus() {
		// Jackson deserialization
	}

	JobStatus(int id) { this.id = id; }

	@Override
	public String toString() {
		return getClass().getSimpleName() + ":" +
				name() + "(" +
				String.valueOf(id) +
				")";
	}

	public static JobStatus valueOf(int id) {
		return values()[id];
	}

	@JsonProperty
	public int getId() {
		return id;
	}
}