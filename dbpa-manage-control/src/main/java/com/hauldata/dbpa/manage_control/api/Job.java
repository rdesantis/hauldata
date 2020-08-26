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

package com.hauldata.dbpa.manage_control.api;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * All the information needed to launch a job and record its status
 */
public class Job {

	private String scriptName;
	private List<ScriptArgument> arguments;
	private List<String> scheduleNames;
	private boolean enabled;
	private String alertTo;
	private String logUsing;

	public Job() {
		// Jackson deserialization
	}

	public Job(
			String scriptName,
			List<ScriptArgument> arguments,
			List<String> scheduleNames,
			boolean enabled,
			String alertTo,
			String logUsing) {

		this.scriptName = scriptName;
		this.arguments = arguments;
		this.scheduleNames = scheduleNames;
		this.enabled = enabled;
		this.alertTo = alertTo;
		this.logUsing = logUsing;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + ":" +
				"{" +
				String.valueOf(scriptName) + "," +
				String.valueOf(arguments) + "," +
				String.valueOf(scheduleNames) + "," +
				String.valueOf(enabled) + "," +
				String.valueOf(alertTo) + "," +
				String.valueOf(logUsing) +
				"}";
	}

	// Getters

	@JsonProperty
	public String getScriptName() {
		return scriptName;
	}

	@JsonProperty
	public List<ScriptArgument> getArguments() {
		return arguments;
	}

	@JsonProperty
	public List<String> getScheduleNames() {
		return scheduleNames;
	}

	@JsonProperty
	public boolean isEnabled() {
		return enabled;
	}

	@JsonProperty
	public String getAlertTo() {
		return alertTo;
	}

	@JsonProperty
	public String getLogUsing() {
		return logUsing;
	}
}
