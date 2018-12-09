/*
 * Copyright (c) 2016, 2018, Ronald DeSantis
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
import java.io.StringReader;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.naming.NamingException;
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
import com.hauldata.dbpa.loader.FileLoader;
import com.hauldata.dbpa.manage.JobManager;
import com.hauldata.dbpa.manage_control.api.ScriptParameter;
import com.hauldata.dbpa.manage_control.api.ScriptValidation;
import com.hauldata.dbpa.process.Context;
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

	@PUT
	@Path("{name}/rename")
	@Timed
	public void rename(@PathParam("name") String name, String newName) throws IOException {
		files.rename(name, newName);
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
	@GET
	@Path("{name}/validation")
	@Timed
	public ScriptValidation validate(@PathParam("name") String name) throws IOException {

		JobManager manager = JobManager.getInstance();

		DbProcess process = null;
		String validationMessage = null;
		List<VariableBase> parameters = null;

		try {
			process = manager.getContext().loader.load(name);
			parameters = process.getParameters();
		}
		catch (FileNotFoundException ex) {
			throw new FileNotFoundException(scriptNotFoundMessageStem + name);
		}
		catch (RuntimeException | NamingException ex) {
			// See TaskSetParser.parseTasks() for parse exceptions.
			// TODO: Define a SyntaxError exception that collects all syntax errors
			validationMessage = ex.getMessage();
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

	/**
	 * Validate text for script syntax.  No script is saved or retrieved.
	 *
	 * @return the validation results.
	 * @throws IOException
	 */
	@PUT
	@Path("validate")
	@Timed
	public ScriptValidation validateBody(String body) throws IOException {

		DbProcess process = null;
		String validationMessage = null;
		List<VariableBase> parameters = null;

		try {
			process = DbProcess.parse(new StringReader(body));
			parameters = process.getParameters();
		}
		catch (RuntimeException | NamingException ex) {
			validationMessage = ex.getMessage();
		}

		return new ScriptValidation(process != null, validationMessage, toScriptParameters(parameters));
	}

	/**
	 * Retrieve the validations for a set of script(s)
	 * whose names match an optional wildcard pattern
	 *
	 * @param likeName is the name wildcard pattern or null to get all script validations
	 * @return a map of script names to script validations
	 * or an empty map if no script with a matching name is found
	 * @throws IOException
	 */
	@GET
	@Path("-/validations")
	@Timed
	public Map<String, ScriptValidation> validateAll(@QueryParam("like") String likeName) throws IOException {

		Map<String, ScriptValidation> result = new TreeMap<String, ScriptValidation>();

		List<String> names = files.getNames(likeName);
		for (String name : names) {
			ScriptValidation validation = validate(name);
			result.put(name, validation);
		}

		return result;
	}

	/**
	 * Run a script passing arguments to it.
	 *
	 * This method does not return until script execution has completed.
	 * Unlike job execution, this method does not offer any way to track or manage script execution.
	 * This method is not intended to be used to invoke long-running scripts.
	 *
	 * @param name is the name of the script to run
	 * @param arguments is an array of arguments passed to the script;
	 *        the array can be empty but this argument must not be null
	 * @throws FileNotFoundException if the script does not exist
	 * @throws IOException if an error occurs retrieving the script
	 * @throws NamingException if an error occurs parsing the script
	 * @throws a variety of exceptions that can occur while executing a script
	 */
	@POST
	@Path("-/running/{name}")
	@Timed
	public void run(@PathParam("name") String name, String[] arguments) throws Exception {

		JobManager manager = JobManager.getInstance();

		try {
			Context context = manager.getContext();
			DbProcess process = context.loader.load(name);
			process.run(arguments, context);
		}
		catch (FileNotFoundException ex) {
			throw new FileNotFoundException(scriptNotFoundMessageStem + name);
		}
	}
}
