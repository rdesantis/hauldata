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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

import javax.naming.NamingException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.ServiceUnavailableException;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;
import com.hauldata.dbpa.loader.FileLoader;
import com.hauldata.dbpa.manage.JobManager;
import com.hauldata.dbpa.manage.JobManagerException.NotAvailable;
import com.hauldata.dbpa.manage_control.api.ScriptParameter;
import com.hauldata.dbpa.manage_control.api.ScriptValidation;
import com.hauldata.dbpa.process.DbProcess;
import com.hauldata.dbpa.variable.VariableBase;

@Path("/scripts")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ScriptsResource {

	static private FilesResource files = new FilesResource(
			"Script",
			"." + FileLoader.processFileExt,
			Paths.get(((FileLoader)JobManager.getInstance().getContext().loader).getProcessPathString()));

	public static final String scriptNotFoundMessageStem = files.getNotFoundMessageStem();

	public ScriptsResource() {}

	// TODO: Use exception mapping instead of duplicating code.
	// See https://dennis-xlc.gitbooks.io/restful-java-with-jax-rs-2-0-2rd-edition/content/en/part1/chapter7/exception_handling.html

	@PUT
	@Path("{name}")
	@Timed
	public void put(@PathParam("name") String name, String body) {
		try {
			files.put(name, body);
		}
		catch (NotAvailable ex) {
			throw new ServiceUnavailableException(ex.getMessage());
		}
		catch (Exception ex) {
			throw new InternalServerErrorException(ex.getLocalizedMessage());
		}
	}

	@GET
	@Path("{name}")
	@Timed
	public String get(@PathParam("name") String name) {
		try {
			return files.get(name);
		}
		catch (FileNotFoundException ex) {
			throw new NotFoundException(scriptNotFoundMessageStem + name);
		}
		catch (NotAvailable ex) {
			throw new ServiceUnavailableException(ex.getMessage());
		}
		catch (Exception ex) {
			throw new InternalServerErrorException(ex.getLocalizedMessage());
		}
	}

	@DELETE
	@Path("{name}")
	@Timed
	public void delete(@PathParam("name") String name) {
		try {
			files.delete(name);
		}
		catch (NoSuchFileException ex) {
			throw new NotFoundException(scriptNotFoundMessageStem + name);
		}
		catch (NotAvailable ex) {
			throw new ServiceUnavailableException(ex.getMessage());
		}
		catch (Exception ex) {
			throw new InternalServerErrorException(ex.getLocalizedMessage());
		}
	}

	@GET
	@Path("-/names")
	@Timed
	public List<String> getNames(@QueryParam("like") String likeName) {
		try {
			return files.getNames(likeName);
		}
		catch (NotAvailable ex) {
			throw new ServiceUnavailableException(ex.getMessage());
		}
		catch (Exception ex) {
			throw new InternalServerErrorException(ex.getLocalizedMessage());
		}
	}

	@GET
	@Path("-/validations/{name}")
	@Timed
	public ScriptValidation validate(@PathParam("name") String name) {
		try {
			return validateScript(name);
		}
		catch (FileNotFoundException ex) {
			throw new NotFoundException(scriptNotFoundMessageStem + name);
		}
		catch (NotAvailable ex) {
			throw new ServiceUnavailableException(ex.getMessage());
		}
		catch (Exception ex) {
			throw new InternalServerErrorException(ex.getLocalizedMessage());
		}
	}

	/**
	 * Read a script and validate it for syntax.
	 *
	 * @return the validation results.  Fields are:
	 *
	 *	isValid returns true if the script is valid syntactically, other false;
	 *	validationMessage returns an error message if the validation failed, or null if it succeeded;
	 * 	parameters returns the list of parameters that can be passed to the script.
	 *
	 * @throws FileNotFoundException if the file is not found
	 * @throws IOException for any other file system error
	 * @throws Exception if validation otherwise fails fatally not due to invalid syntax
 	 */
	private ScriptValidation validateScript(String name) throws Exception {

		JobManager manager = JobManager.getInstance();

		DbProcess process = null;
		String validationMessage = null;
		List<VariableBase> parameters = null;

		try {
			process = manager.getContext().loader.load(name);
			parameters = process.getParameters();
		}
		catch (RuntimeException | NamingException ex) {
			// See TaskSetParser.parseTasks() for parse exceptions.
			// TODO: Define a SyntaxError exception that collects all syntax errors
			validationMessage = ex.getMessage();
		}
		catch (Exception ex) {
			throw ex;
		}

		return new ScriptValidation(process != null, validationMessage, toScriptParameters(parameters));
	}

	private List<ScriptParameter> toScriptParameters(List<VariableBase> parameters) {

		List<ScriptParameter> scriptParameters = new LinkedList<ScriptParameter>();
		if (parameters != null) {
			for (VariableBase parameter : parameters) {
				scriptParameters.add(new ScriptParameter(parameter.getName(), parameter.getType().getName()));
			}
		}
		return scriptParameters;
	}
}
