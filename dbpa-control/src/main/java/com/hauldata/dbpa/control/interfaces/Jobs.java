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

package com.hauldata.dbpa.control.interfaces;

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;
import com.hauldata.dbpa.manage.api.Job;
import com.hauldata.dbpa.manage.api.JobRun;
import com.hauldata.dbpa.manage.api.ScriptArgument;

@Path("/jobs")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface Jobs {

	@PUT
	@Path("{name}")
	@Timed
	public int put(@PathParam("name") String name, Job job);

	@GET
	@Path("{name}")
	@Timed
	public Job get(@PathParam("name") String name);

	@DELETE
	@Path("{name}")
	@Timed
	public void delete(@PathParam("name") String name);

	@PUT
	@Path("{name}/script")
	@Timed
	public void putScriptName(@PathParam("name") String name, String scriptName);

	@PUT
	@Path("{name}/propfile")
	@Timed
	public void putPropName(@PathParam("name") String name, String propName);

	@PUT
	@Path("{name}/arguments")
	@Timed
	public void putArguments(@PathParam("name") String name, List<ScriptArgument> arguments);

	@PUT
	@Path("{name}/schedules")
	@Timed
	public void putScheduleNames(@PathParam("name") String name, List<String> scheduleNames);

	@PUT
	@Path("{name}/enabled")
	@Timed
	public void putEnabled(@PathParam("name") String name, boolean enabled);

	@GET
	@Path("-/names")
	@Timed
	public List<String> getNames(@QueryParam("like") String likeName);

	@GET
	@Path("-/runs")
	@Timed
	public List<JobRun> getRuns(@QueryParam("like") String likeName, @QueryParam("latest") Boolean latest);

	@GET
	@Path("-/running")
	@Timed
	public List<JobRun> getRunning();

	@POST
	@Path("-/running/{name}")
	@Timed
	public int run(@PathParam("name") String name);

	@GET
	@Path("-/running/{id}")
	@Timed
	public JobRun getRunning(@PathParam("id") int id);

	@DELETE
	@Path("-/running/{id}")
	@Timed
	public void stop(@PathParam("id") int id);
}
