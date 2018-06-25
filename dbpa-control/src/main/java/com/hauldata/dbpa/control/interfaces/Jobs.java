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
import java.util.Map;

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

import com.hauldata.dbpa.manage_control.api.Job;
import com.hauldata.dbpa.manage_control.api.JobRun;
import com.hauldata.dbpa.manage_control.api.ScriptArgument;

@Path("/jobs")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface Jobs {

	@PUT
	@Path("{name}")
	public int put(@PathParam("name") String name, Job job);

	@GET
	@Path("{name}")
	public Job get(@PathParam("name") String name);

	@GET
	public Map<String, Job> getJobs(@QueryParam("like") String likeName);

	@DELETE
	@Path("{name}")
	public void delete(@PathParam("name") String name);

	@PUT
	@Path("{name}/script")
	public void putScriptName(@PathParam("name") String name, String scriptName);

	@PUT
	@Path("{name}/arguments")
	public void putArguments(@PathParam("name") String name, List<ScriptArgument> arguments);

	@PUT
	@Path("{name}/schedules")
	public void putScheduleNames(@PathParam("name") String name, List<String> scheduleNames);

	@PUT
	@Path("{name}/enabled")
	public void putEnabled(@PathParam("name") String name, boolean enabled);

	@DELETE
	@Path("{name}/arguments")
	public void deleteArguments(@PathParam("name") String name);

	@DELETE
	@Path("{name}/schedules")
	public void deleteScheduleNames(@PathParam("name") String name);

	@GET
	@Path("-/names")
	public List<String> getNames(@QueryParam("like") String likeName);

	@GET
	@Path("-/runs")
	public List<JobRun> getRuns(@QueryParam("like") String likeName, @QueryParam("latest") Boolean latest, @QueryParam("ascending") Boolean ascending);

	@GET
	@Path("-/running")
	public List<JobRun> getRunning();

	@POST
	@Path("-/running/{name}")
	public int run(@PathParam("name") String name, List<ScriptArgument> arguments);

	@GET
	@Path("-/running/{id}")
	public JobRun getRunning(@PathParam("id") int id);

	@DELETE
	@Path("-/running/{id}")
	public void stop(@PathParam("id") int id);
}
