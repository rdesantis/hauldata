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

import java.util.List;
import java.util.Optional;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;
import com.hauldata.dbpa.manage.JobManager;
import com.hauldata.dbpa.manage.api.Job;
import com.hauldata.dbpa.manage.api.JobRun;
import com.hauldata.dbpa.manage.api.ScriptArgument;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class JobsResource {

	public JobsResource() {}

	@PUT
	@Path("/jobs/{name}")
	@Timed
	public void putJob(@PathParam("name") String name, Job job) {
		try {
			JobManager.getInstance().putJob(name, job);
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@GET
	@Path("/jobs/{name}")
	@Timed
	public Job getJob(@PathParam("name") String name) {
		try {
			return JobManager.getInstance().getJob(name);
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@DELETE
	@Path("/jobs/{name}")
	@Timed
	public void deleteJob(@PathParam("name") String name) {
		try {
			JobManager.getInstance().deleteJob(name);
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@PUT
	@Path("/jobs/{name}/script")
	@Timed
	public void putJobScriptName(@PathParam("name") String name, String scriptName) {
		try {
			// TODO!!!
			return;
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@PUT
	@Path("/jobs/{name}/propfile")
	@Timed
	public void putJobPropName(@PathParam("name") String name, String propName) {
		try {
			// TODO!!!
			return;
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@PUT
	@Path("/jobs/{name}/arguments")
	@Timed
	public void putJobArguments(@PathParam("name") String name, List<ScriptArgument> arguments) {
		try {
			// TODO!!!
			return;
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@PUT
	@Path("/jobs/{name}/schedules")
	@Timed
	public void putJobScheduleNames(@PathParam("name") String name, List<String> scheduleNames) {
		try {
			// TODO!!!
			return;
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@PUT
	@Path("/jobs/{name}/enabled")
	@Timed
	public void putJobEnabled(@PathParam("name") String name, boolean enabled) {
		try {
			// TODO!!!
			return;
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@GET
	@Path("/job/names")
	@Timed
	public List<String> getJobNames(@QueryParam("like") Optional<String> likeName) {
		try {
			return JobManager.getInstance().getJobNames(likeName.orElse(null));
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@GET
	@Path("/job/runs")
	@Timed
	public List<JobRun> getJobRuns(@QueryParam("like") Optional<String> likeName, @QueryParam("latest") Optional<Boolean> latest) {
		try {
			return JobManager.getInstance().getRuns(likeName.orElse(null), latest.orElse(false));
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@GET
	@Path("/job/running")
	@Timed
	public List<JobRun> getRunningJobs() {
		try {
			return JobManager.getInstance().getRunning();
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@POST
	@Path("/job/running/{name}")
	@Timed
	public Integer runJob(@PathParam("name") String name) {
		try {
			return JobManager.getInstance().run(name).getRunId();
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@DELETE
	@Path("/job/running/{id}")
	@Timed
	public void stopJob(@PathParam("id") int id) {
		try {
			JobManager.getInstance().stop(id);
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}
}
