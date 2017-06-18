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

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;
import com.hauldata.dbpa.manage.JobManager;

@Path("/service")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ServiceResource {

	public ServiceResource() {
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
		// Shut down the job manager if running and prevent any new references to the singleton Manager instance.

		JobManager.killInstance();

		// Stop this server.

		// Create a separate thread that waits a short time for the server to respond to this service call before
		// shutting down the server.

		final long waitToRespondMillis = 3000;

		Thread shutdownThread = new Thread() {
			@Override
			public void run() {
				try { Thread.sleep(waitToRespondMillis); } catch (InterruptedException ex) {}
				System.exit(0);
			}
		};

		shutdownThread.start();
	}
}
