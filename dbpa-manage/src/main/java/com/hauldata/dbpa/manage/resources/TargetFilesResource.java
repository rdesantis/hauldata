/*
 * Copyright (c) 2018, Ronald DeSantis
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

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.codahale.metrics.annotation.Timed;
import com.hauldata.dbpa.manage.JobManager;

@Path("/targetfiles")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class TargetFilesResource {

	static private FilesResource files = new FilesResource("Target", "", JobManager.getInstance().getContext().getWriteParentPath());

	public static final String notFoundMessageStem = files.getNotFoundMessageStem();

	public TargetFilesResource() {}

	@GET
	@Path("{name}")
	@Timed
	public String get(@PathParam("name") String name) throws IOException {
		return files.get(name);
	}

	@GET
	@Path("{name}/bytes")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	@Timed
	public Response getBytes(@PathParam("name") String name) throws IOException {
		return files.getBytes(name);
	}

	@DELETE
	@Path("{name}")
	@Timed
	public void delete(@PathParam("name") String name) throws IOException {
		files.delete(name);
	}
}
