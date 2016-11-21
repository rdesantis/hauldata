package com.hauldata.dbpa.manage.resources;

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

import com.codahale.metrics.annotation.Timed;
import com.hauldata.dbpa.manage.api.ScheduleValidation;

@Path("/schedules")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface SchedulesResourceInterface {

	@PUT
	@Path("{name}")
	@Timed
	public int put(@PathParam("name") String name, String body);

	@GET
	@Path("{name}")
	@Timed
	public String get(@PathParam("name") String name);

	@DELETE
	@Path("{name}")
	@Timed
	public void delete(@PathParam("name") String name);

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Timed
	public Map<String, String> getAll(@QueryParam("like") String likeName);

	@GET
	@Path("-/names")
	@Produces(MediaType.APPLICATION_JSON)
	@Timed
	public List<String> getNames(@QueryParam("like") String likeName);

	@GET
	@Path("validations/{name}")
	@Produces(MediaType.APPLICATION_JSON)
	@Timed
	public ScheduleValidation validate(@PathParam("name") String name);
}
