/*
 * Copyright (c) 2017, Ronald DeSantis
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

package com.hauldata.ws.rs.exception;

import javax.ws.rs.core.Response;

import com.fasterxml.jackson.annotation.JsonProperty;

public class WebException {

	private int code;
	private String message;

	public WebException() {
		// Jackson deserialization
	}

	public WebException(
			Response.Status status,
			Exception exception) {
		code = status.getStatusCode();
		message = exception.getMessage();
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + ":" +
				"{" +
				String.valueOf(code) + "," +
				String.valueOf(message) +
				"}";
	}

	public static Response response(
			Response.Status status,
			Exception exception) {

		WebException entity = new WebException(status, exception);
		return Response.status(status).entity(entity).build();
	}

	// Getters

	@JsonProperty
	public int getCode() {
		return code;
	}

	@JsonProperty
	public String getMessage() {
		return message;
	}
}
