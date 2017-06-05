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

import com.fasterxml.jackson.annotation.JsonProperty;

public class ScriptParameter {

	private String name;
	private String typeName;

	public ScriptParameter() {
		// Jackson deserialization
	}

	public ScriptParameter(
			String name,
			String typeName) {

		this.name = name;
		this.typeName = typeName;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + ":" +
				"{" +
				String.valueOf(name) + "," +
				String.valueOf(typeName) +
				"}";
	}

	@JsonProperty
	public String getName() {
		return name;
	}

	@JsonProperty
	public String getTypeName() {
		return typeName;
	}
}