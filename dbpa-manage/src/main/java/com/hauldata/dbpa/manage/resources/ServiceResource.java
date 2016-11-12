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

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;
import com.hauldata.dbpa.manage.JobManager;

import io.dropwizard.setup.Environment;

@Path("/service")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ServiceResource {

	private Environment environment;

	public ServiceResource(Environment environment) {
		this.environment = environment;
	}

	@DELETE
	@Timed
	public void kill() {
		try {
			JobManager.getInstance().shutdown();

			// See https://community.oracle.com/blogs/enicholas/2006/05/04/understanding-weak-references

			ReferenceQueue<JobManager> queue = new ReferenceQueue<JobManager>();
			PhantomReference<JobManager> managerReference = new PhantomReference<JobManager>(JobManager.getInstance(), queue);

			// Prevent any new references to the Manager instance.
			// Then wait a reasonable time for all in-progress uses to terminate and drop their strong references,
			// which will cause the phantom reference to be pushed to the queue.

			JobManager.killInstance();

			int timeoutSeconds = 30;

			if (queue.remove(timeoutSeconds * 1000L) != managerReference) {
				// The in-progress uses of the Manager instance did not terminate within the timeout interval.
			};

			managerReference.clear();

			// Stop this server.
			// See http://stackoverflow.com/questions/15989008/dropwizard-how-to-stop-service-programmatically

			environment.getApplicationContext().getServer().stop();
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}
}
