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

package com.hauldata.dbpa.control;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.hauldata.dbpa.control.api.ProcessConfiguration;
import com.hauldata.dbpa.control.api.ProcessRun;
import com.hauldata.dbpa.control.api.ScriptArgument;
import com.hauldata.dbpa.control.api.ScriptParameter;
import com.hauldata.dbpa.control.api.ScriptValidation;
import com.hauldata.util.tokenizer.Quoted;
import com.hauldata.util.tokenizer.Tokenizer;

public class Commander {

	private static final String CREATE = "CREATE";
	private static final String LIST = "LIST";
	private static final String VALIDATE = "VALIDATE";
	private static final String GET = "GET";
	private static final String DELETE = "DELETE";
	private static final String STARTUP = "STARTUP";
	private static final String RUN = "RUN";
	private static final String STOP = "STOP";
	private static final String SHUTDOWN = "SHUTDOWN";
	private static final String EXIT = "EXIT";

	private static final String SCRIPT = "SCRIPT";
	private static final String PROPERTIES = "PROPERTIES";
	private static final String PROP = "PROP";
	private static final String SCHEMA = "SCHEMA";
	private static final String CONFIGURATION = "CONFIGURATION";
	private static final String CONFIG = "CONFIG";
	private static final String ARGUMENTS = "ARGUMENTS";
	private static final String ARGS = "ARGS";
	private static final String RUNNING = "RUNNING";

	private static final String CREATE_SCHEMA = CREATE + ' ' + SCHEMA;
	private static final String CREATE_CONFIG = CREATE + ' ' + CONFIG;
	private static final String LIST_SCRIPTS = LIST + ' ' + SCRIPT;
	private static final String LIST_PROPS = LIST + ' ' + PROP;
	private static final String LIST_CONFIGS = LIST + ' ' + CONFIG;
	private static final String LIST_RUNS = LIST + ' ' + RUN;
	private static final String LIST_RUNNING = LIST + ' ' + RUNNING;
	private static final String VALIDATE_SCRIPT = VALIDATE + ' ' + SCRIPT;
	private static final String GET_CONFIG = GET + ' ' + CONFIG;
	private static final String DELETE_SCRIPT = DELETE + ' ' + SCRIPT;
	private static final String DELETE_PROP = DELETE + ' ' + PROP;
	private static final String DELETE_CONFIG = DELETE + ' ' + CONFIG;

	private static Controller controller = Controller.getInstance();

	private Map<String, Command> commands;

	private Tokenizer tokenizer;
	private PrintStream out;

	public Commander() {
		commands = new HashMap<String, Command>();
		
		commands.put(CREATE, new CreateCommand());
		commands.put(LIST, new ListCommand());
		commands.put(VALIDATE, new ValidateCommand());
		commands.put(GET, new GetCommand());
		commands.put(DELETE, new DeleteCommand());
		commands.put(STARTUP, new StartupCommand());
		commands.put(RUN, new RunCommand());
		commands.put(STOP, new StopCommand());
		commands.put(SHUTDOWN, new ShutdownCommand());
		commands.put(EXIT, new ExitCommand());

		commands.put(CREATE_SCHEMA, new CreateSchemaCommand());
		commands.put(CREATE_CONFIG, new CreateConfigurationCommand());
		commands.put(LIST_SCRIPTS, new ListScriptsCommand());
		commands.put(LIST_PROPS, new ListPropertiesCommand());
		commands.put(LIST_CONFIGS, new ListConfigurationsCommand());
		commands.put(LIST_RUNS, new ListRunsCommand());
		commands.put(LIST_RUNNING, new ListRunningCommand());
		commands.put(VALIDATE_SCRIPT, new ValidateScriptCommand());
		commands.put(GET_CONFIG, new GetConfigurationCommand());
		commands.put(DELETE_SCRIPT, new DeleteScriptCommand());
		commands.put(DELETE_PROP, new DeletePropertiesCommand());
		commands.put(DELETE_CONFIG, new DeleteConfigurationCommand());
	}

	/**
	 * Interpret a command string
	 * @param request is the command with its arguments
	 * @param response is a buffer (normally empty on entry) to which the command response is appended
	 * @return true if the command string is a request to terminate command processing, false otherwise
	 * @throws RuntimeException is case of a syntax error
	 *
	 * TODO: This implementation is not thread safe.
	 * Can't use member variables tokenizer and out.
	 */
	public boolean interpret(String request, StringBuffer response) {

		tokenizer = new Tokenizer(new StringReader(request));
		tokenizer.eolIsSignificant(true);
		tokenizer.wordChars('_', '_');

		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		out = new PrintStream(outStream);

		boolean done = false;
		try {
			String commandName = tokenizer.nextWordUpperCase();
			Command command = commands.get(commandName);

			if (command == null) {
				throw new RuntimeException("Unrecognized command: " + commandName);
			}

			done = command.interpret();

			response.append(outStream.toString());
		}
		catch (Exception ex) {
			throw new RuntimeException(ex.getMessage());
		}
		finally {
			try { out.close(); } catch (Exception e) {}
			try { tokenizer.close(); } catch (Exception e) {}
		}

		return done;
	}

	// Generic commands that delegate to specific commands

	private interface Command  { boolean interpret() throws Exception; }

	private class CreateCommand implements Command {

		@Override
		public boolean interpret() throws Exception {
			
			String commandName = null;
			if (tokenizer.skipWordIgnoreCase(SCHEMA)) {
				commandName = CREATE_SCHEMA;
			}
			else if (tokenizer.skipWordIgnoreCase(CONFIG) || tokenizer.skipWordIgnoreCase(CONFIGURATION)) {
				commandName = CREATE_CONFIG;
			}
			else {
				throw new RuntimeException("Unrecognized CREATE command");
			}
			
			return commands.get(commandName).interpret();
		}
	}

	private class ListCommand implements Command {

		@Override
		public boolean interpret() throws Exception {

			String commandName = null;
			if (skipWordPluralIgnoreCase(SCRIPT)) {
				commandName = LIST_SCRIPTS;
			}
			else if (skipWordPluralIgnoreCase(PROP) || tokenizer.skipWordIgnoreCase(PROPERTIES)) {
				commandName = LIST_PROPS;
			}
			else if (skipWordPluralIgnoreCase(CONFIG) || skipWordPluralIgnoreCase(CONFIGURATION)) {
				commandName = LIST_CONFIGS;
			}
			else if (skipWordPluralIgnoreCase(RUN)) {
				commandName = LIST_RUNS;
			}
			else if (tokenizer.skipWordIgnoreCase(RUNNING)) {
				commandName = LIST_RUNNING;
			}
			else {
				throw new RuntimeException("Unrecognized LIST command");
			}
			
			return commands.get(commandName).interpret();
		}
	}

	private class ValidateCommand implements Command {

		@Override
		public boolean interpret() throws Exception {

			String commandName = null;
			if (tokenizer.skipWordIgnoreCase(SCRIPT)) {
				commandName = VALIDATE_SCRIPT;
			}
			else {
				throw new RuntimeException("Unrecognized VALIDATE command");
			}
			
			return commands.get(commandName).interpret();
		}
	}

	private class GetCommand implements Command {

		@Override
		public boolean interpret() throws Exception {

			String commandName = null;
			if (tokenizer.skipWordIgnoreCase(CONFIG) || tokenizer.skipWordIgnoreCase(CONFIGURATION)) {
				commandName = GET_CONFIG;
			}
			else {
				throw new RuntimeException("Unrecognized GET command");
			}
			
			return commands.get(commandName).interpret();
		}
	}

	private class DeleteCommand implements Command {

		@Override
		public boolean interpret() throws Exception {

			String commandName = null;
			if (tokenizer.skipWordIgnoreCase(SCRIPT)) {
				commandName = DELETE_SCRIPT;
			}
			else if (tokenizer.skipWordIgnoreCase(PROP) || tokenizer.skipWordIgnoreCase(PROPERTIES)) {
				commandName = DELETE_PROP;
			}
			else if (tokenizer.skipWordIgnoreCase(CONFIG) || tokenizer.skipWordIgnoreCase(CONFIGURATION)) {
				commandName = DELETE_CONFIG;
			}
			else {
				throw new RuntimeException("Unrecognized DELETE command");
			}
			
			return commands.get(commandName).interpret();
		}
	}

	// Command patterns

	private abstract class NullaryMessageCommand implements Command {
		
		@Override
		public boolean interpret() throws Exception {

			nextEndOfLine();

			String message = action();
			
			if (message != null) {
				out.println(message);
			}

			return false;
		}

		protected abstract String action() throws Exception;
	}

	private abstract class UnaryMessageCommand implements Command {
		
		@Override
		public boolean interpret() throws Exception {

			String name = tokenizer.nextWord();
			nextEndOfLine();

			String message = action(name);
			
			if (message != null) {
				out.println(message);
			}

			return false;
		}

		protected abstract String action(String name) throws Exception;
	}

	private abstract class NullaryListCommand implements Command {

		@Override
		public boolean interpret() throws Exception {

			nextEndOfLine();

			List<String> list = action();
			
			for (String item : list) {
				out.println(item);
			}

			return false;
		}

		protected abstract List<String> action() throws Exception;
	}

	// Specific commands

	private class ListScriptsCommand extends NullaryListCommand {

		@Override
		protected List<String> action() throws Exception {
			return controller.listScripts();
		}
	}

	private class ValidateScriptCommand implements Command {

		@Override
		public boolean interpret() throws Exception {

			String name = tokenizer.nextWord();
			nextEndOfLine();

			ScriptValidation validation = controller.validateScript(name);
			
			if (!validation.isValid()) {
				out.println(validation.getValidationMessage());
			}
			else {
				for (ScriptParameter parameter : validation.getParameters()) {
					
					out.print(parameter.getName());
					out.print("\t");
					out.print(parameter.getTypeName());
					out.println();
				}
			}

			return false;
		}
	}

	private class DeleteScriptCommand extends UnaryMessageCommand {

		@Override
		protected String action(String name) throws Exception {
			controller.deleteScript(name);
			return null;
		}
	}

	private class ListPropertiesCommand extends NullaryListCommand {

		@Override
		protected List<String> action() throws Exception {
			return controller.listPropertiesFiles();
		}
	}

	private class DeletePropertiesCommand extends UnaryMessageCommand {

		@Override
		protected String action(String name) throws Exception {
			controller.deletePropertiesFile(name);
			return null;
		}
	}

	private class CreateSchemaCommand extends NullaryMessageCommand {

		@Override
		protected String action() throws Exception {
			controller.createSchema();
			return null;
		}
	}

	private class CreateConfigurationCommand implements Command {

		@Override
		public boolean interpret() throws Exception {

			String configName = tokenizer.nextWordUpperCase();

			tokenizer.skipWordIgnoreCase(SCRIPT);
			String scriptName = tokenizer.nextWordUpperCase();

			String propName = null;
			if (tokenizer.skipWordIgnoreCase(PROP) || tokenizer.skipWordIgnoreCase(PROPERTIES)) {
				propName = tokenizer.nextWordUpperCase();
			}

			List<ScriptArgument> arguments = new LinkedList<ScriptArgument>();
			if (tokenizer.skipWordIgnoreCase(ARGS) || tokenizer.skipWordIgnoreCase(ARGUMENTS)) {
				do {
					String name = tokenizer.nextWordUpperCase();
					Object value = tokenizer.nextToken();
					if (value instanceof Quoted) {
						value = ((Quoted)value).getBody();
					}

					ScriptArgument argument = new ScriptArgument(name, value.toString());
					arguments.add(argument);

				} while (tokenizer.skipDelimiter(","));
			}

			nextEndOfLine();

			controller.storeConfiguration(new ProcessConfiguration(null, configName, scriptName, propName, arguments));

			return false;
		}
	}

	private class ListConfigurationsCommand implements Command {

		@Override
		public boolean interpret() throws Exception {

			nextEndOfLine();

			List<ProcessConfiguration> configs = controller.listConfigurations();

			if (configs != null) {
				for (ProcessConfiguration config : configs) {
					printConfiguration(config);
				}
			}

			return false;
		}
	}

	private class GetConfigurationCommand implements Command {

		@Override
		public boolean interpret() throws Exception {

			String configName = tokenizer.nextWordUpperCase();

			nextEndOfLine();

			ProcessConfiguration config = controller.loadConfiguration(configName);

			printConfiguration(config);

			return false;
		}
	}

	private void printConfiguration(ProcessConfiguration config) {

		out.print(config.getProcessName());

		if (0 < config.getId()) {
			out.print(" SCRIPT ");
			out.print(config.getScriptName());
	
			if (config.getPropName() != null) {
				out.print(" PROPERTIES ");
				out.print(config.getPropName());
			}
	
			if ((config.getArguments() != null) && !config.getArguments().isEmpty()) {
				out.print(" ARGUMENTS ");
				
				boolean first = true;
				for (ScriptArgument argument : config.getArguments()) {
					if (!first) {
						out.print(", ");
					}
	
					out.print(argument.getName());
					out.print("\t");
					out.print(argument.getValue());
	
					first = false;
				}
			}
		}

		out.println();
	}

	private class DeleteConfigurationCommand extends UnaryMessageCommand {

		@Override
		protected String action(String name) throws Exception {
			controller.deleteConfiguration(name);
			return null;
		}
	}

	private class ListRunsCommand implements Command {

		@Override
		public boolean interpret() throws Exception {

			String configName = null;
			if (!tokenizer.skipDelimiter("*")) {
				configName = tokenizer.nextWordUpperCase();
			}

			boolean latest = !tokenizer.skipDelimiter("*");

			nextEndOfLine();

			List<ProcessRun> runs = controller.listRuns(configName, latest);
			
			printRuns(runs);

			return false;
		}
	}

	private class ListRunningCommand implements Command {

		@Override
		public boolean interpret() throws Exception {

			List<ProcessRun> runs = controller.listRunning();
			
			printRuns(runs);

			return false;
		}
	}

	private void printRuns(List<ProcessRun> runs) {

		for (ProcessRun run : runs) {

			out.print(run.getConfigName());

			if (0 < run.getConfigId()) {
				out.print("\t");
				out.print(run.getRunIndex());
				out.print("\t");
				out.print(run.getStatus().name());
				out.print("\t");
				printDateTime(run.getStartTime());
				out.print("\t");
				printDateTime(run.getEndTime());
			}

			out.println();
		}
	}

	private void printDateTime(LocalDateTime time) {
		final String nullTimeText = "          ---          ";
		out.print((time == null) ? nullTimeText : time.toString());
	}

	private class StartupCommand extends NullaryMessageCommand {

		@Override
		protected String action() throws Exception {
			controller.startup();
			return null; 
		}
	}

	private class RunCommand extends UnaryMessageCommand {

		@Override
		protected String action(String name) throws Exception {
			ProcessRun run = controller.run(name);
			return String.valueOf(run.getRunIndex());
		}
	}

	private class StopCommand implements Command {

		@Override
		public boolean interpret() throws Exception {

			String name = tokenizer.nextWord();
			
			boolean latest = !tokenizer.skipDelimiter("*");

			nextEndOfLine();

			// TODO: Add a method Controller.loadRunning() which doesn't go to the database
			// but instead asks the ProcessExecutor for the running processes from its map.
			List<ProcessRun> runs = controller.listRuns(name, latest);

			for (ProcessRun run : runs) {
				if (run.getStatus() == ProcessRun.Status.runInProgress) {
					controller.stop(run);
				}
			}

			return false;
		}
	}

	private class ShutdownCommand extends NullaryMessageCommand {

		@Override
		protected String action() throws Exception {
			controller.shutdown();
			return null;
		}
	}

	private class ExitCommand implements Command {

		@Override
		public boolean interpret() throws Exception {

			nextEndOfLine();

			// TODO: Must allow for exit while controller is started,
			// which disconnects the console but leaves controller running.
			
			if (controller.isStarted()) {
				controller.shutdown();
			}

			return true;
		}
	}

	// Tokenizer helpers

	private boolean skipWordPluralIgnoreCase(String word) throws IOException {
		return 
				tokenizer.skipWordIgnoreCase(word) ||
				tokenizer.skipWordIgnoreCase(word + 'S');
	}

	private void nextEndOfLine() throws IOException {

		boolean atEndOfLine = !tokenizer.hasNextOnLine();

		skipPastEndOfLine();

		if (!atEndOfLine) {
			throw new RuntimeException("Unexpected tokens at end of command");
		}
	}

	private void skipPastEndOfLine() throws IOException {
		while (tokenizer.nextTokenOnLine() != null) {}
	}
}
