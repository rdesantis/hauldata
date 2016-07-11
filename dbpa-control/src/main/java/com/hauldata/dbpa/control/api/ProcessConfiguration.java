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

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * All the information needed to launch a process and record its status
 */
public class ProcessConfiguration {

	private Integer id;
	private String processName;
	private String scriptName;
	private String propName;
	private List<ScriptArgument> arguments;

	public ProcessConfiguration() {
		// Jackson deserialization
	}

	public ProcessConfiguration(
			Integer id,
			String name,
			String scriptName,
			String propName,
			List<ScriptArgument> arguments) {

		this.id = id;
		this.processName = name;
		this.scriptName = scriptName;
		this.propName = propName;
		this.arguments = arguments;
	}

	// Setter

	public void setId(Integer id) {
		this.id = id;
	}

	// Getters

	@JsonProperty
	public Integer getId() {
		return id;
	}

	@JsonProperty
	public String getProcessName() {
		return processName;
	}

	@JsonProperty
	public String getScriptName() {
		return scriptName;
	}

	@JsonProperty
	public String getPropName() {
		return propName;
	}

	@JsonProperty
	public List<ScriptArgument> getArguments() {
		return arguments;
	}
}
