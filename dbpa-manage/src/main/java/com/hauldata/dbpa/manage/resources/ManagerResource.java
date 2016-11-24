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
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.ServiceUnavailableException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.codahale.metrics.annotation.Timed;
import com.hauldata.dbpa.manage.JobManager;
import com.hauldata.dbpa.manage.JobManagerException.AlreadyStarted;
import com.hauldata.dbpa.manage.JobManagerException.NotAvailable;
import com.hauldata.dbpa.manage.JobManagerException.NotStarted;

@Path("/manager")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ManagerResource {

	public ManagerResource() {}

	// TODO: Use exception mapping instead of duplicating code.
	// See https://dennis-xlc.gitbooks.io/restful-java-with-jax-rs-2-0-2rd-edition/content/en/part1/chapter7/exception_handling.html

	@PUT
	@Timed
	public void startup() {
		try {
			JobManager.getInstance().startup();
		}
		catch (NotAvailable ex) {
				throw new ServiceUnavailableException(ex.getMessage());
		}
		catch (AlreadyStarted ex) {
			throw new ClientErrorException(ex.getMessage(), Response.Status.CONFLICT);
		}
		catch (Exception ex) {
			throw new InternalServerErrorException(ex.getLocalizedMessage());
		}
	}

	@GET
	@Timed
	public boolean isStarted() {
		try {
			return JobManager.getInstance().isStarted();
		}
		catch (NotAvailable ex) {
			throw new ServiceUnavailableException(ex.getMessage());
		}
		catch (Exception ex) {
			throw new InternalServerErrorException(ex.getLocalizedMessage());
		}
	}

	@DELETE
	@Timed
	public void shutdown() {
		try {
			JobManager.getInstance().shutdown();
		}
		catch (NotAvailable ex) {
			throw new ServiceUnavailableException(ex.getMessage());
		}
		catch (NotStarted ex) {
			throw new ClientErrorException(ex.getMessage(), Response.Status.CONFLICT);
		}
		catch (Exception ex) {
			throw new InternalServerErrorException(ex.getLocalizedMessage());
		}
	}
}
