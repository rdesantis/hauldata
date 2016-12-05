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
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.hauldata.dbpa.manage_control.api.ScheduleValidation;

@Path("/schedules")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface Schedules {

	@PUT
	@Path("{name}")
	public int put(@PathParam("name") String name, String body);

	@GET
	@Path("{name}")
	public String get(@PathParam("name") String name);

	@DELETE
	@Path("{name}")
	public void delete(@PathParam("name") String name);

	@GET
	public Map<String, String> getAll(@QueryParam("like") String likeName);

	@GET
	@Path("-/names")
	public List<String> getNames(@QueryParam("like") String likeName);

	@GET
	@Path("-/validations/{name}")
	public ScheduleValidation validate(@PathParam("name") String name);

	@GET
	@Path("-/running")
	public List<String> getRunning();
}
