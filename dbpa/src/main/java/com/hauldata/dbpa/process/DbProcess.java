/*
 * Copyright (c) 2016, 2017, Ronald DeSantis
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

package com.hauldata.dbpa.process;

import java.io.IOException;
import java.io.Reader;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.naming.NameAlreadyBoundException;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;

import com.hauldata.dbpa.connection.Connection;
import com.hauldata.dbpa.connection.DatabaseConnection;
import com.hauldata.dbpa.connection.EmailConnection;
import com.hauldata.dbpa.connection.FtpConnection;
import com.hauldata.dbpa.task.Task;
import com.hauldata.dbpa.variable.*;
import com.hauldata.util.tokenizer.BacktrackingTokenizerMark;

/**
 * Executable database process
 */
public class DbProcess extends TaskSet {

	// For non-task-specific runtime logging

	public static final String processTaskId = "process";

	public static final String startMessage = "Starting process";
	public static final String completeMessage = "Process completed";
	public static final String terminateMessage = "Process terminated";
	public static final String stopMessage = "Process stopped";
	public static final String failMessageStem = "Process failed: ";
	public static final String elapsedMessageStem = "Elapsed time: ";

	// Note that Task objects created outside the scope of this compilation unit but owned by this DbProcess
	// hold direct references to elements of the variables map.  Therefore the map is retained
	// even though the compiler warns it is "unused" because this class does not directly reference it
	// again after it is set.

	private List<VariableBase> parameters;
	@SuppressWarnings("unused")
	private Map<String, VariableBase> variables;
	private Map<String, Connection> connections;

	/**
	 * Instantiate a DbProcess object by parsing a script from a Reader
	 *
	 * @param r is the Reader supplying the script to define the process
	 * @throws IOException
	 * @throws InputMismatchException
	 * @throws NoSuchElementException
	 * @throws NameAlreadyBoundException
	 * @throws NameNotFoundException
	 */
	public static DbProcess parse(Reader r) throws IOException, NamingException {

		DbProcessParser parser = new DbProcessParser(r);

		return parser.parse();
	}

	/**
	 * Constructor with package visibility for use by DbProcessParser
	 */
	DbProcess(
				List<VariableBase> parameters,
				Map<String, VariableBase> variables,
				Map<String, Connection> connections,
				Map<String, Task> tasks) {

		super(tasks);

		this.parameters = parameters;
		this.variables = variables;
		this.connections = connections;
	}

	/**
	 * @return the parameters for the process
	 */
	public List<VariableBase> getParameters() {
		return parameters;
	}

	/**
	 * Run the process
	 *
	 * @param args are the argument values to pass into the process.
	 * @param context is the context in which to run the process.
	 * <p>
	 * The first and last log messages written to the log are guaranteed to have
	 * Task ID set to public static data member <code>processTaskId</code> and
	 * content as follows:
	 * <p>
	 * First message has level <code>info</code> and content <code>startMessage</code>.
	 * <p>
	 * On normal completion next to last message has level <code>info</code> and content <code>completeMessage</code>.
	 * <p>
	 * On interrupt next to last message has level <code>error</code> and content <code>terminateMessage</code>.
	 * <p>
	 * On failure next to last message has level <code>error</code> and content
	 * begins with <code>failMessageStem</code>.
	 * <p>
	 * Last message has level <code>message</code> and content begins with <code>elapsedMessageStem</code>.
	 * <p>
	 * The log of the context is NOT closed when the process has completed.
	 * Caller must close the log.
	 * @throws Exception
	 */
	public void run(String[] args, Context context) throws Exception {

		LocalDateTime startTime = LocalDateTime.now();

		try {
			context.logger.info(processTaskId, startMessage);

			setParameters(args);
			runTasks(context);

			context.logger.info(processTaskId, completeMessage);
		}
		catch (InterruptedException ex) {
			context.logger.error(processTaskId, terminateMessage);

			throw new RuntimeException(terminateMessage);
		}
		catch (Task.StoppedException ex) {
			context.logger.info(processTaskId, stopMessage);
		}
		catch (Exception ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();

			context.logger.error(processTaskId, failMessageStem + message);

			throw new RuntimeException(message);
		}
		finally {
			context.close(connections);

			long millis = ChronoUnit.MILLIS.between(startTime,  LocalDateTime.now());

			context.logger.message(processTaskId, elapsedMessageStem + formatElapsed(millis));
		}
	}

	/**
	 * Validate that the arguments are of appropriate types for the process
	 *
	 * @throws Exception if argument types do not match the process parameter types
	 */
	public void validate(String[] args) {
		setParameters(args);
	}

	private String formatElapsed(long millis) {

		long seconds = millis / 1000;
		long minutes = seconds / 60;
		long hours = minutes / 60;
		long days = hours / 24;

		return String.format("%d days %02d:%02d:%02d.%03d", days, hours % 24, minutes % 60, seconds % 60, millis % 1000);
	}

	private void setParameters(String[] args) {
		VariablesFromStrings.set(parameters, args);
	}
}

/**
 * Database process parser
 */
class DbProcessParser extends TaskSetParser {

	public DbProcessParser(Reader r) throws IOException {
		super(r,
				new HashMap<String, VariableBase>(),
				new HashMap<String, Connection>());
	}

	/**
	 * Parse a script from the Reader provided on the constructor
	 * returning the results to the variables provided on the constructor
	 *
	 * @throws IOException if physical read of the script fails
	 * @throws RuntimeException if any syntax errors are encountered in the script
	 */
	public DbProcess parse() throws IOException {

		List<VariableBase> parameters = new ArrayList<VariableBase>();

		Map<String, Task> tasks;
		try {
			parseParameters(parameters);
			parseVariables();
			parseConnections();

			tasks = parseTasks(null, enclosingStructureName());

			if (tokenizer.hasNext()) {
				throw new RuntimeException("Unexpected token where " + KW.TASK.name() + " is expected");
			}
		}
		catch (IOException ex) { throw ex; }
		catch (Exception ex) {
			String newMessage = "At line " + Integer.toString(tokenizer.lineno()) + ": " + ex.getMessage();
			throw new RuntimeException(newMessage, ex);
		}
		finally {
			close();
		}

		return new DbProcess(parameters, variables, connections, tasks);
	}

	private void parseParameters(List<VariableBase> parameters)
			throws IOException, InputMismatchException, NoSuchElementException, NameAlreadyBoundException {

		String section = KW.PARAMETERS.name();
		if (tokenizer.skipWordIgnoreCase(section)) {

			do {
				parameters.add(parseVariable(true));
			} while (tokenizer.skipDelimiter(","));

			endSection(section);
		}
	}

	private void parseVariables()
			throws IOException, InputMismatchException, NoSuchElementException, NameAlreadyBoundException {

		String section = KW.VARIABLES.name();
		if (tokenizer.skipWordIgnoreCase(section)) {

			do {
				if (parseVariable(false) == null) {
					parseConnection(false);
				}
			} while (tokenizer.skipDelimiter(","));

			endSection(section);
		}
	}

	private VariableBase parseVariable(boolean isRequired)
			throws InputMismatchException, NoSuchElementException, IOException, NameAlreadyBoundException {

		BacktrackingTokenizerMark mark = tokenizer.mark();

		String name = tokenizer.nextWordUpperCase();
		if (variables.containsKey(name)) {
			throw new NameAlreadyBoundException("Duplicate variable name: " + name);
		}
		else if (reservedVariableNames.contains(name)) {
			throw new NameAlreadyBoundException("Cannot use reserved word as a variable name: " + name);
		}

		VariableBase variable = null;
		VariableType type = parseType();

		if (type != null) {
			variable = new Variable<Object>(name, type);
			variables.put(name, variable);
		}
		else if (isRequired) {
			throw new InputMismatchException("Invalid variable type name: " + type);
		}
		else {
			tokenizer.reset(mark);
		}

		return variable;
	}

	/**
	 * Parse a data type
	 *
	 * @return the data type object
	 * @throws InputMismatchException
	 * @throws IOException
	 */
	private VariableType parseType() throws InputMismatchException, IOException {

		String type = tokenizer.nextWordUpperCase();

		if (type.equals(KW.TINYINT.name()) || type.equals(KW.INT.name()) || type.equals(KW.INTEGER.name())) {
			return VariableType.INTEGER;
		}
		else if (type.equals(KW.BIT.name())) {
			return VariableType.BIT;
		}
		else if (type.equals(KW.VARCHAR.name()) || type.equals(KW.CHAR.name()) || type.equals(KW.CHARACTER.name())) {
			// Ignore length qualifier if present
			if (tokenizer.skipDelimiter("(")) {
				tokenizer.nextToken();
				tokenizer.skipDelimiter(")");
			}
			return VariableType.VARCHAR;
		}
		else if (type.equals(KW.DATETIME.name()) || type.equals(KW.DATE.name())) {
			return VariableType.DATETIME;
		}
		else {
			return null;
		}
	}

	private void parseConnections()
			throws IOException, InputMismatchException, NoSuchElementException, NameAlreadyBoundException {

		String section = KW.CONNECTIONS.name();
		if (tokenizer.skipWordIgnoreCase(section)) {

			do { parseConnection(true);
			} while (tokenizer.skipDelimiter(","));

			endSection(section);
		}
	}

	private void parseConnection(boolean isConnection)
			throws InputMismatchException, NoSuchElementException, IOException, NameAlreadyBoundException {

		String name = tokenizer.nextWordUpperCase();
		if (connections.containsKey(name)) {
			throw new NameAlreadyBoundException("Duplicate " + entityTypeName(isConnection) + " name: " + name);
		}
		else if (reservedConnectionNames.contains(name)) {
			throw new NameAlreadyBoundException("Cannot use reserved word as a connection name: " + name);
		}
		else if (variables.containsKey(name)) {
			// This is not strictly necessary but it will eliminate certain confusing task constructs.
			throw new NameAlreadyBoundException("Connection name cannot be the same as a variable name: " + name);
		}

		String type = tokenizer.nextWordUpperCase();
		Connection connection = null;

		if (type.equals(KW.DATABASE.name())) {
			connection = new DatabaseConnection();
		}
		else if (type.equals(KW.FTP.name())) {
			connection = new FtpConnection();
		}
		else if (type.equals(KW.EMAIL.name())) {
			connection = new EmailConnection();
		}
		else {
			throw new InputMismatchException("Invalid " + entityTypeName(isConnection) + " type name: " + type);
		}

		connections.put(name, connection);
	}

	private String entityTypeName(boolean isConnection) {
		return isConnection ? "connection" : "variable";
	}
}
