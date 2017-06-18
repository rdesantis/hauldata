/*
 * Copyright (c) 2017, Ronald DeSantis
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

import java.io.IOException;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;
import com.hauldata.dbpa.manage.JobManager;

@Path("/propfiles")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class PropertiesFilesResource {

	static private FilesResource files = new FilesResource("Properties", ".properties", JobManager.getInstance().getContext().getPropertiesParentPath());

	public static final String notFoundMessageStem = files.getNotFoundMessageStem();

	public PropertiesFilesResource() {}

	@PUT
	@Path("{name}")
	@Timed
	public void put(@PathParam("name") String name, String body) throws IOException {
		files.put(name, body);
	}

	@GET
	@Path("{name}")
	@Timed
	public String get(@PathParam("name") String name) throws IOException {
		return files.get(name);
	}

	@DELETE
	@Path("{name}")
	@Timed
	public void delete(@PathParam("name") String name) throws IOException {
		files.delete(name);
	}

	@GET
	@Path("-/names")
	@Timed
	public List<String> getNames(@QueryParam("like") String likeName) throws IOException {
		return files.getNames(likeName);
	}
}
