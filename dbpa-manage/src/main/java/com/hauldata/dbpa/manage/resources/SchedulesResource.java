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

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;

import com.codahale.metrics.annotation.Timed;

public class SchedulesResource {

	public SchedulesResource() {}

	@PUT
	@Path("/schedules/{name}")
	@Timed
	public void putSchedule(@PathParam("name") String name, String body) {
		try {
			// TODO!!!
			return;
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@GET
	@Path("/schedules/{name}")
	@Timed
	public String getSchedule(@PathParam("name") String name) {
		try {
			// TODO!!!
			return null;
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@DELETE
	@Path("/schedules/{name}")
	@Timed
	public void deleteSchedule(@PathParam("name") String name) {
		try {
			// TODO!!!
			return;
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@GET
	@Path("/schedule/names")
	@Timed
	public List<String> getScheduleNames(@QueryParam("like") Optional<String> likeName) {
		try {
			// TODO!!!
			return null;
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}
}
