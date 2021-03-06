/*
 * Copyright (c) 2016, 2018-2019, Ronald DeSantis
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

import com.hauldata.dbpa.manage_control.api.ScriptValidation;

@Path("/scripts")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface Scripts {

	@PUT
	@Path("{name}")
	public void put(@PathParam("name") String name, String body);

	@GET
	@Path("{name}")
	public String get(@PathParam("name") String name);

	@PUT
	@Path("{name}/rename")
	public void rename(@PathParam("name") String name, String newName);

	@DELETE
	@Path("{name}")
	public void delete(@PathParam("name") String name);

	@GET
	@Path("-/names")
	public List<String> getNames(@QueryParam("like") String likeName);

	@GET
	@Path("{name}/validation")
	public ScriptValidation validate(@PathParam("name") String name);

	@PUT
	@Path("-/validate")
	public ScriptValidation validateBody(String body);

	@GET
	@Path("-/validations")
	public Map<String, ScriptValidation> validateAll(@QueryParam("like") String likeName);

	@POST
	@Path("-/running/{name}")
	public void run(@PathParam("name") String name, String[] arguments);
}
