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

package com.hauldata.dbpa.process;

import java.io.IOException;
import java.io.Reader;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.InputMismatchException;
import java.util.Iterator;
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
import com.hauldata.dbpa.expression.*;
import com.hauldata.dbpa.task.Task;
import com.hauldata.dbpa.variable.*;

/**
 * Executable database process
 */
public class DbProcess extends TaskSet {

	// For non-task-specific runtime logging

	public static final String processTaskId = "process";

	public static final String startMessage = "Starting process";
	public static final String completeMessage = "Process completed";
	public static final String terminateMessage = "Process terminated";
	public static final String failMessageStem = "Process failed: ";
	public static final String elapsedMessageStem = "Elapsed time: ";

	private List<VariableBase> parameters;

	// Note that Task objects created outside the scope of this compilation unit but owned by this DbProcess
	// hold direct references to elements of these Map objects.  Therefore the maps must be retained
	// even though the compiler warns they "unused" because this class does not directly reference them
	// again after they are set.
	@SuppressWarnings("unused")
	private Map<String, VariableBase> variables;
	@SuppressWarnings("unused")
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
	 * <P>
	 * On interrupt next to last message has level <code>error</code> and content <code>terminateMessage</code>.
	 * <P>
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
		catch (Exception ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();

			context.logger.error(processTaskId, failMessageStem + message);

			throw new RuntimeException(message);
		}
		finally {
			context.files.assureAllClosed();
			
			long millis = ChronoUnit.MILLIS.between(startTime,  LocalDateTime.now());
			
			context.logger.message(processTaskId, elapsedMessageStem + formatElapsed(millis));
		}
	}

	private String formatElapsed(long millis) {

		long seconds = millis / 1000;
		long minutes = seconds / 60;
		long hours = minutes / 60;
		long days = hours / 24;

		return String.format("%d days %02d:%02d:%02d.%03d", days, hours % 24, minutes % 60, seconds % 60, millis % 1000); 	
	}

	private void setParameters(String[] args) {

		int n = Math.min(parameters.size(), args.length);
		Iterator<VariableBase> parameterIterator = parameters.iterator();
		for (int i = 0; i < n; ++i) {
			setParameter(parameterIterator.next(), args[i]);
		}
	}

	private void setParameter(VariableBase parameter, String arg) {

		if (arg == null) {
			parameter.setValueObject(null);
		}
		else if (parameter.getType() == VariableType.INTEGER) {
			parameter.setValueObject(Integer.valueOf(arg));
		}
		else if (parameter.getType() == VariableType.VARCHAR) {
			parameter.setValueObject(arg);
		}
		else if (parameter.getType() == VariableType.DATETIME) {
			DatetimeFromString converter = new DatetimeFromString(new StringConstant(arg));
			parameter.setValueObject(converter.evaluate());
		}
	}
}

/**
 * Database process parser
 */
class DbProcessParser extends TaskSetParser {

	// Sections

	private List<VariableBase> parameters;

	public DbProcessParser(Reader r) {

		super(r);

		parameters = new ArrayList<VariableBase>();
	}

	/**
	 * Parse a script from the Reader provided on the constructor
	 * returning the results to the variables provided on the constructor
	 * 
	 * @throws IOException if physical read of the script fails
	 * @throws RuntimeException if any syntax errors are encountered in the script
	 */
	public DbProcess parse() throws IOException {

		try {
			parseParameters();
			parseVariables();
			parseConnections();
			parseTasks();
			
			if (tokenizer.hasNext()) {
				throw new RuntimeException("Unexpected token where " + RW.TASK.name() + " is expected");
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

	private void parseParameters()
			throws IOException, InputMismatchException, NoSuchElementException, NameAlreadyBoundException {

		String section = RW.PARAMETERS.name();
		if (tokenizer.skipWordIgnoreCase(section)) {

			do { parameters.add(parseVariable());
			} while (tokenizer.skipDelimiter(","));
			
			nextEnd(section);
		}
	}

	private void parseVariables()
			throws IOException, InputMismatchException, NoSuchElementException, NameAlreadyBoundException {

		String section = RW.VARIABLES.name();
		if (tokenizer.skipWordIgnoreCase(section)) {

			do { parseVariable();
			} while (tokenizer.skipDelimiter(","));

			nextEnd(section);
		}
	}

	private VariableBase parseVariable()
			throws InputMismatchException, NoSuchElementException, IOException, NameAlreadyBoundException {

		String name = tokenizer.nextWordUpperCase();
		if (variables.containsKey(name)) {
			throw new NameAlreadyBoundException("Duplicate variable name: " + name);
		}
		else if (reservedWords.contains(name)) {
			throw new NameAlreadyBoundException("Cannot use reserved word as a variable name: " + name);
		}

		VariableType type = parseType();

		VariableBase variable = new Variable<Object>(name, type);
		variables.put(name, variable);

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

		if (type.equals(RW.TINYINT.name()) || type.equals(RW.INT.name()) || type.equals(RW.INTEGER.name())) {
			return VariableType.INTEGER;
		}
		else if (type.equals(RW.VARCHAR.name()) || type.equals(RW.CHAR.name()) || type.equals(RW.CHARACTER.name())) {
			// Ignore length qualifier if present
			if (tokenizer.skipDelimiter("(")) {
				tokenizer.nextToken();
				tokenizer.skipDelimiter(")");
			}
			return VariableType.VARCHAR;
		}
		else if (type.equals(RW.DATETIME.name()) || type.equals(RW.DATE.name())) {
			return VariableType.DATETIME;
		}
		else {
			throw new InputMismatchException("Invalid variable type name: " + type);
		}
	}

	private void parseConnections()
			throws IOException, InputMismatchException, NoSuchElementException, NameAlreadyBoundException {

		String section = RW.CONNECTIONS.name();
		if (tokenizer.skipWordIgnoreCase(section)) {

			do { parseConnection();
			} while (tokenizer.skipDelimiter(","));

			nextEnd(section);
		}
	}

	private void parseConnection()
			throws InputMismatchException, NoSuchElementException, IOException, NameAlreadyBoundException {

		String name = tokenizer.nextWordUpperCase();
		if (connections.containsKey(name)) {
			throw new NameAlreadyBoundException("Duplicate connection name: " + name);
		}
		else if (reservedWords.contains(name)) {
			throw new NameAlreadyBoundException("Cannot use reserved word as a connection name: " + name);
		}

		String type = tokenizer.nextWordUpperCase();
		Connection connection = null;

		if (type.equals(RW.DATABASE.name())) {
			connection = new DatabaseConnection();
		}
		else if (type.equals(RW.FTP.name())) {
			connection = new FtpConnection();
		}
		else if (type.equals(RW.EMAIL.name())) {
			connection = new EmailConnection();
		}
		else {
			throw new InputMismatchException("Invalid connection type name: " + type);
		}

		connections.put(name, connection);
	}
}
