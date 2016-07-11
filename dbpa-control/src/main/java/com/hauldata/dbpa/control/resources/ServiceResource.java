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

package com.hauldata.dbpa.control.resources;

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;
import com.hauldata.dbpa.control.Commander;
import com.hauldata.dbpa.control.Controller;
import com.hauldata.dbpa.control.api.ProcessConfiguration;
import com.hauldata.dbpa.control.api.ScriptValidation;

@Path("/dbpa-control")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ServiceResource {

	private static Controller controller = Controller.getInstance();

	public ServiceResource() {}

	@POST
	@Timed
	public String interpret(String request) {
		StringBuffer response = new StringBuffer();
		boolean done = false;
		try {
			done = new Commander().interpret(request, response);
			// TODO: Is there any action to take if done is true?
			return response.toString();
		}
		catch (RuntimeException rex) {
			// See http://stackoverflow.com/questions/29109887/how-can-i-return-404-http-status-from-dropwizard
			throw new WebApplicationException(rex.getMessage(), 400);
		}
	}

	@GET
	@Path("/list-scripts")
	@Timed
	public List<String> listScripts() {
		try {
			return controller.listScripts();
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@GET
	@Path("/validate-script")
	@Timed
	public ScriptValidation validateScript(@QueryParam("name") String name) {
		try {
			return controller.validateScript(name);
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@POST
	@Path("/store-config")
	@Timed
	public void storeConfiguration(ProcessConfiguration config) {
		try {
			controller.storeConfiguration(config);
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@GET
	@Path("/list-configs")
	@Timed
	public List<ProcessConfiguration> listConfigurations() {
		try {
			return controller.listConfigurations();
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}
}
