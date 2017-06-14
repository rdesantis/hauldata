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

public class ScriptValidation {

	private boolean valid;
	private String validationMessage;
	private List<ScriptParameter> parameters;

	public ScriptValidation() {
		// Jackson deserialization
	}

	public ScriptValidation(
			boolean valid,
			String validationMessage,
			List<ScriptParameter> parameters) {

		this.valid = valid;
		this.validationMessage = validationMessage;
		this.parameters = parameters;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + ":" +
				"{" +
				String.valueOf(valid) + "," +
				String.valueOf(validationMessage) + "," +
				String.valueOf(parameters) +
				"}";
	}

	@JsonProperty
	public boolean isValid() {
		return valid;
	}

	@JsonProperty
	public String getValidationMessage() {
		return validationMessage;
	}

	@JsonProperty
	public List<ScriptParameter> getParameters() {
		return parameters;
	}
}
