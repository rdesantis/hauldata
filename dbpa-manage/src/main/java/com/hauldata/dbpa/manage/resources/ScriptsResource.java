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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
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

	/**
	 * Write a script to file.
	 * @param name is the name of the script
	 * @param body is the body of the script
	 */
	private void putScript(String name, String body) {

		String processFilePathName = getProcessFilePathName(name);

		Writer writer = null;
		try {
			writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(processFilePathName)));

			writer.write(body);
		}
		catch (IOException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error occurred writing script: " + message, ex);
		}
		finally { try {
			if (writer != null) writer.close();
		}
		catch (IOException ex) {
			throw new RuntimeException("Error occurred closing script: " + name, ex);
		} }
	}

	private String getProcessFilePathName(String name) {

		JobManager manager = JobManager.getInstance();
		FileLoader fileLoader = (FileLoader)manager.getContext().loader;

		String processFileName = name + "." + FileLoader.processFileExt;
		String processFilePathName = Paths.get(fileLoader.getProcessPathString()).resolve(processFileName).toString();

		return processFilePathName;
	}

	/**
	 * Read a script from file.
	 * @param name is the name of the script
	 * @return the body of the script
	 */
	private String getScript(String name) {

		String processFilePathName = getProcessFilePathName(name);

		final int bufferLength = 2048;
		char[] charBuffer = new char[bufferLength];
		StringBuilder bodyBuilder = new StringBuilder();

		Reader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(processFilePathName)));

			int count;
			while((count = reader.read(charBuffer, 0, bufferLength)) != -1) {
				bodyBuilder.append(charBuffer, 0, count);
			}
		}
		catch (FileNotFoundException ex) {
			throw new RuntimeException("Script not found: " + name);
		}
		catch (IOException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error occurred reading script: " + message, ex);
		}
		finally { try {
			if (reader != null) reader.close();
		}
		catch (IOException ex) {
			throw new RuntimeException("Error occurred closing script: " + name, ex);
		} }

		return bodyBuilder.toString();
	}

	private void deleteScript(String name) throws Exception {
		String processFilePathName = getProcessFilePathName(name);

		try {
			Files.delete(Paths.get(processFilePathName));
		}
		catch (NoSuchFileException ex) {
			throw new RuntimeException("Script not found: " + name);
		}
		catch (IOException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error occurred deleting script: " + message, ex);
		}
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
		FileLoader fileLoader = (FileLoader)manager.getContext().loader;
		
		if (likeName == null) {
			likeName = "*";
		}

		final String scriptSuffix = "." + FileLoader.processFileExt;
		final int scriptSuffixLength = scriptSuffix.length();

		List<String> scriptNames = new LinkedList<String>();

		DirectoryStream<java.nio.file.Path> scriptPaths = null;
		try {
			scriptPaths = Files.newDirectoryStream(Paths.get(fileLoader.getProcessPathString()), likeName + scriptSuffix);

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
