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

import java.util.Properties;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PropertiesTuple {

	private Properties connectionProps;
	private Properties sessionProps;
	private Properties ftpProps;
	private Properties pathProps;
	private Properties logProps;

	public PropertiesTuple() {
		// Jackson deserialization
	}

	public PropertiesTuple(
			Properties connectionProps,
			Properties sessionProps,
			Properties ftpProps,
			Properties pathProps,
			Properties logProps) {

		this.connectionProps = connectionProps;
		this.sessionProps = sessionProps;
		this.ftpProps = ftpProps;
		this.pathProps = pathProps;
		this.logProps = logProps;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + ":" +
				"{" +
				String.valueOf(connectionProps) + "," +
				String.valueOf(sessionProps) + "," +
				String.valueOf(ftpProps) + "," +
				String.valueOf(pathProps) + "," +
				String.valueOf(logProps) +
				"}";
	}

	@JsonProperty
	public Properties getConnectionProps() {
		return connectionProps;
	}

	@JsonProperty
	public Properties getSessionProps() {
		return sessionProps;
	}

	@JsonProperty
	public Properties getFtpProps() {
		return ftpProps;
	}

	@JsonProperty
	public Properties getPathProps() {
		return pathProps;
	}

	@JsonProperty
	public Properties getLogProps() {
		return logProps;
	}
}
