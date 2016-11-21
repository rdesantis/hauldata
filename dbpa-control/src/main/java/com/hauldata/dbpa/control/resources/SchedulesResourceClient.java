package com.hauldata.dbpa.control.resources;

import java.util.List;
import java.util.Map;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;

import com.hauldata.dbpa.manage.api.ScheduleValidation;
import com.hauldata.dbpa.manage.resources.SchedulesResourceInterface;

public class SchedulesResourceClient implements SchedulesResourceInterface {

	private WebTarget baseTarget;

	// See http://www.tutorialspoint.com/restful/restful_methods.htm

	public SchedulesResourceClient(String baseUrl) {
		
		Client client = ClientBuilder.newClient();
		baseTarget = client.target(baseUrl).path("/schedules");
	}

	@Override
	public int put(String name, String body) {

		int id = baseTarget 
				.path("/{name}")
				.resolveTemplate("name", name)
				.request(MediaType.APPLICATION_JSON)
				.put(Entity.entity(body, MediaType.APPLICATION_JSON), int.class);

		return id;
	}

	@Override
	public String get(String name) {

		String schedule = baseTarget
				.path("/{name}")
				.resolveTemplate("name", name)
				.request(MediaType.APPLICATION_JSON)
				.get(String.class);

		return schedule;
	}

	@Override
	public void delete(String name) {

		baseTarget 
				.path("/{name}")
				.resolveTemplate("name", name)
				.request(MediaType.APPLICATION_JSON)
				.delete(void.class);
	}

	@Override
	public Map<String, String> getAll(String likeName) {

		GenericType<Map<String, String>> schedulesClass = new GenericType<Map<String, String>>() {};

		Map<String, String> schedules = baseTarget
				.queryParam("like", "{like}")
				.resolveTemplate("like", likeName)
				.request(MediaType.APPLICATION_JSON)
				.get(schedulesClass);

		return schedules;
	}

	@Override
	public List<String> getNames(String likeName) {

		GenericType<List<String>> namesClass = new GenericType<List<String>>() {};

		List<String> names = baseTarget
				.path("/-/names")
				.queryParam("like", "{like}")
				.resolveTemplate("like", likeName)
				.request(MediaType.APPLICATION_JSON)
				.get(namesClass);

		return names;
	}

	@Override
	public ScheduleValidation validate(String name) {

		ScheduleValidation validation = baseTarget
				.path("/validations/{name}")
				.resolveTemplate("name", name)
				.request(MediaType.APPLICATION_JSON)
				.get(ScheduleValidation.class);

		return validation;
	}
}
