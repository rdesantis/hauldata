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

package com.hauldata.dbpa.manage.resources;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.codahale.metrics.annotation.Timed;
import com.hauldata.dbpa.manage.JobManager;
import com.hauldata.dbpa.manage.JobManagerException.AlreadyUnavailable;

import io.dropwizard.setup.Environment;

@Path("/service")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ServiceResource {

	private Environment environment;

	public ServiceResource(Environment environment) {
		this.environment = environment;
		serviceResource = this;
	}

	/**
	 * For testing, retain the single instance of this class instantiated by
	 * dropwizard Application.run().
	 */
	private static ServiceResource serviceResource;

	public static ServiceResource getInstance() {
		return serviceResource;
	}

	@DELETE
	@Timed
	public void kill() {
		try {
			// Shut down the job manager if running and prevent any new references to the singleton Manager instance.

			JobManager.killInstance();

			// Stop this server.
			// See http://stackoverflow.com/questions/15989008/dropwizard-how-to-stop-service-programmatically

			environment.getApplicationContext().getServer().stop();
		}
		catch (AlreadyUnavailable ex) {
			throw new ClientErrorException(ex.getMessage(), Response.Status.CONFLICT);
		}
		catch (Exception ex) {
			throw new InternalServerErrorException(ex.getLocalizedMessage());
		}
	}
}
