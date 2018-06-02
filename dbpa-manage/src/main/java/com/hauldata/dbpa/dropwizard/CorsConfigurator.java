/*
 * Copyright (c) 2018, Ronald DeSantis
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

package com.hauldata.dbpa.dropwizard;

import java.util.EnumSet;

import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;

import org.eclipse.jetty.servlets.CrossOriginFilter;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.dropwizard.setup.Environment;

public class CorsConfigurator {

	private Boolean enabled = false;
	private String allowedOrigins = "*";
	private String allowedHeaders = "X-Requested-With,Content-Type,Accept,Origin";
	private String allowedMethods = "OPTIONS,GET,PUT,POST,DELETE,HEAD";

	@JsonProperty
	public Boolean getEnabled() {
		return enabled;
	}

	@JsonProperty
	public void setEnabled(Boolean enabled) {
		this.enabled = enabled;
	}

	@JsonProperty
	public String getAllowedOrigins() {
		return allowedOrigins;
	}

	@JsonProperty
	public void setAllowedOrigins(String allowedOrigins) {
		this.allowedOrigins = allowedOrigins;
	}

	@JsonProperty
	public String getAllowedHeaders() {
		return allowedHeaders;
	}

	@JsonProperty
	public void setAllowedHeaders(String allowedHeaders) {
		this.allowedHeaders = allowedHeaders;
	}

	@JsonProperty
	public String getAllowedMethods() {
		return allowedMethods;
	}

	@JsonProperty
	public void setAllowedMethods(String allowedMethods) {
		this.allowedMethods = allowedMethods;
	}

	public void enableCors(Environment environment) {

		// See https://stackoverflow.com/questions/25775364/enabling-cors-in-dropwizard-not-working

		// Enable CORS headers
		final FilterRegistration.Dynamic cors =
		    environment.servlets().addFilter("CORS", CrossOriginFilter.class);

		// Configure CORS parameters
		cors.setInitParameter("allowedOrigins", allowedOrigins);
		cors.setInitParameter("allowedHeaders", allowedHeaders);
		cors.setInitParameter("allowedMethods", allowedMethods);

		// Add URL mapping
		cors.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");

		// DO NOT pass a preflight request to down-stream auth filters
		// unauthenticated preflight requests should be permitted by spec
		cors.setInitParameter(CrossOriginFilter.CHAIN_PREFLIGHT_PARAM, Boolean.FALSE.toString());
	}
}
