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

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import javax.naming.NamingException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;
import com.hauldata.dbpa.loader.FileLoader;
import com.hauldata.dbpa.manage.JobManager;
import com.hauldata.dbpa.manage.api.ScriptValidation;
import com.hauldata.dbpa.process.DbProcess;
import com.hauldata.dbpa.variable.VariableBase;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ScriptsResource {

	public ScriptsResource() {}

	@PUT
	@Path("/scripts/{name}")
	@Timed
	public void put(@PathParam("name") String name, String body) {
		try {
			putScript(name, body);
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@GET
	@Path("/scripts/{name}")
	@Timed
	public String get(@PathParam("name") String name) {
		try {
			return getScript(name);
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@DELETE
	@Path("/scripts/{name}")
	@Timed
	public void delete(@PathParam("name") String name) {
		try {
			deleteScript(name);
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@GET
	@Path("/script/names")
	@Timed
	public List<String> getNames(@QueryParam("like") Optional<String> likeName) {
		try {
			return listScripts(likeName.orElse(null));
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	@GET
	@Path("/script/validations/{name}")
	@Timed
	public ScriptValidation validate(@PathParam("name") String name) {
		try {
			return validateScript(name);
		}
		catch (Exception ex) {
			throw new WebApplicationException(ex.getLocalizedMessage(), 500);
		}
	}

	private void putScript(String name, String body) {
		//TODO
		throw new RuntimeException("Put script not implemented");
	}

	private String getScript(String name) {
		//TODO
		throw new RuntimeException("Get script not implemented");
	}

	private void deleteScript(String name) throws Exception {
		//TODO
		throw new RuntimeException("Delete script not implemented");
	}

	/**
	 * Retrieve a list of names of all scripts available in the process directory
	 * that match an optional name wildcard pattern
	 *
	 * @param likeName is the wildcard pattern or null to match all names
	 * @return the list of scripts if any exists;
	 * or an empty list if no scripts exist
	 * @throws Exception if an error occurred
	 */
	private List<String> listScripts(String likeName) throws Exception {

		JobManager manager = JobManager.getInstance();
		
		if (likeName == null) {
			likeName = "*";
		}

		final String scriptSuffix = "." + FileLoader.processFileExt;
		final int scriptSuffixLength = scriptSuffix.length();

		List<String> scriptNames = new LinkedList<String>();

		DirectoryStream<java.nio.file.Path> scriptPaths = null;
		try {
			scriptPaths = Files.newDirectoryStream(((FileLoader)(manager.getContext().loader)).getProcessPath(), likeName + scriptSuffix);

			for (java.nio.file.Path scriptPath : scriptPaths) {
				if (Files.isRegularFile(scriptPath)) {

					String scriptFileName = scriptPath.getFileName().toString();
					String scriptName = scriptFileName.substring(0, scriptFileName.length() - scriptSuffixLength);

					scriptNames.add(scriptName);
				}
			}
		}
		catch (Exception ex) {
			throw ex;
		}
		finally {
			try { scriptPaths.close(); } catch (Exception ex) {}
		}

		return scriptNames;
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
	 * @throws Exception if validation fails for a reason other than bad syntax
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
		catch (/*InputMismatchException |*/ NoSuchElementException | /*NameNotFoundException | NameAlreadyBoundException |*/ NamingException ex) {
			// See TaskSetParser.parseTasks() for parse exceptions.
			// TODO: Define a SyntaxError exception that collects all syntax errors
			validationMessage = ex.getMessage();
		}
		catch (Exception ex) {
			throw ex;
		}

		return new ScriptValidation(process != null, validationMessage, parameters);
	}
}
