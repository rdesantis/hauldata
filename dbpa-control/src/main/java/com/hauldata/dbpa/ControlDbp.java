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

package com.hauldata.dbpa;

import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.AbstractMap.SimpleEntry;

import com.hauldata.dbpa.control.Controller;
import com.hauldata.dbpa.control.ProcessConfiguration;
import com.hauldata.dbpa.control.ProcessRun;
import com.hauldata.dbpa.control.ScriptValidation;
import com.hauldata.dbpa.variable.VariableBase;
import com.hauldata.util.tokenizer.Quoted;
import com.hauldata.util.tokenizer.Tokenizer;

public class ControlDbp {

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

	private static final String CREATE_SCHEMA = CREATE + ' ' + SCHEMA;
	private static final String CREATE_CONFIG = CREATE + ' ' + CONFIG;
	private static final String LIST_SCRIPTS = LIST + ' ' + SCRIPT;
	private static final String LIST_PROPS = LIST + ' ' + PROP;
	private static final String LIST_CONFIGS = LIST + ' ' + CONFIG;
	private static final String LIST_RUNS = LIST + ' ' + RUN;
	private static final String VALIDATE_SCRIPT = VALIDATE + ' ' + SCRIPT;
	private static final String GET_CONFIG = GET + ' ' + CONFIG;
	private static final String DELETE_SCRIPT = DELETE + ' ' + SCRIPT;
	private static final String DELETE_PROP = DELETE + ' ' + PROP;
	private static final String DELETE_CONFIG = DELETE + ' ' + CONFIG;

	private static Map<String, Command> commands;
	private static Tokenizer tokenizer;

	private static Controller controller;

	public static void main(String[] args) {

		initialize();
		interpretCommands();
		cleanup();
	}

	private static void initialize() {

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
		commands.put(VALIDATE_SCRIPT, new ValidateScriptCommand());
		commands.put(GET_CONFIG, new GetConfigurationCommand());
		commands.put(DELETE_SCRIPT, new DeleteScriptCommand());
		commands.put(DELETE_PROP, new DeletePropertiesCommand());
		commands.put(DELETE_CONFIG, new DeleteConfigurationCommand());

//		tokenizer = new Tokenizer(System.console().reader());
		tokenizer = new Tokenizer(new InputStreamReader(System.in));
		tokenizer.eolIsSignificant(true);
		tokenizer.wordChars('_', '_');

		controller = Controller.getInstance();
	}

	private static void interpretCommands() {

		boolean done = false;
		do {
			System.out.print(">");
			System.out.flush();
			try {
				String commandName = tokenizer.nextWordUpperCase();
				Command command = commands.get(commandName);

				if (command == null) {
					throw new RuntimeException("Unrecognized command: " + commandName);
				}
				
				done = command.interpret();
			}
			catch (Exception ex) {
				System.out.println(ex.getMessage());
				
				try {
					skipPastEndOfLine();
				}
				catch (Exception exx) {
					done = true;
				}
			}
		} while (!done);
	}

	private static void cleanup () {

		controller.close();

		try { tokenizer.close(); } catch (Exception ex) {}
	}

	// Generic commands that delegate to specific commands

	private interface Command  { boolean interpret() throws IOException; }

	private static class CreateCommand implements Command {

		@Override
		public boolean interpret() throws IOException {
			
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

	private static class ListCommand implements Command {

		@Override
		public boolean interpret() throws IOException {

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
			else {
				throw new RuntimeException("Unrecognized LIST command");
			}
			
			return commands.get(commandName).interpret();
		}
	}

	private static class ValidateCommand implements Command {

		@Override
		public boolean interpret() throws IOException {

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

	private static class GetCommand implements Command {

		@Override
		public boolean interpret() throws IOException {

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

	private static class DeleteCommand implements Command {

		@Override
		public boolean interpret() throws IOException {

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

	private static abstract class NullaryMessageCommand implements Command {
		
		@Override
		public boolean interpret() throws InputMismatchException, NoSuchElementException, IOException {

			nextEndOfLine();

			String message = action();
			
			if (message != null) {
				System.out.println(message);
			}

			return false;
		}

		protected abstract String action();
	}

	private static abstract class UnaryMessageCommand implements Command {
		
		@Override
		public boolean interpret() throws InputMismatchException, NoSuchElementException, IOException {

			String name = tokenizer.nextWord();
			nextEndOfLine();

			String message = action(name);
			
			if (message != null) {
				System.out.println(message);
			}

			return false;
		}

		protected abstract String action(String name);
	}

	private static abstract class NullaryListCommand implements Command {

		@Override
		public boolean interpret() throws IOException {

			nextEndOfLine();

			List<String> list = action();
			
			for (String item : list) {
				System.out.println(item);
			}

			return false;
		}

		protected abstract List<String> action();
	}

	// Specific commands

	private static class ListScriptsCommand extends NullaryListCommand {

		@Override
		protected List<String> action() {
			return controller.listScripts();
		}
	}

	private static class ValidateScriptCommand implements Command {

		@Override
		public boolean interpret() throws InputMismatchException, NoSuchElementException, IOException {

			String name = tokenizer.nextWord();
			nextEndOfLine();

			ScriptValidation validation = controller.validateScript(name);
			
			if (!validation.isValid) {
				System.out.println(validation.validationMessage);
			}
			else {
				for (VariableBase parameter : validation.parameters) {
					
					System.out.print(parameter.getName());
					System.out.print("\t");
					System.out.print(parameter.getType().getName());
					System.out.println();
				}
			}

			return false;
		}
	}

	private static class DeleteScriptCommand extends UnaryMessageCommand {

		@Override
		protected String action(String name) {
			return controller.deleteScript(name);
		}
	}

	private static class ListPropertiesCommand extends NullaryListCommand {

		@Override
		protected List<String> action() {
			return controller.listPropertiesFiles();
		}
	}

	private static class DeletePropertiesCommand extends UnaryMessageCommand {

		@Override
		protected String action(String name) {
			return controller.deletePropertiesFile(name);
		}
	}

	private static class CreateSchemaCommand extends NullaryMessageCommand {

		@Override
		protected String action() {
			return controller.createSchema();
		}
	}

	private static class CreateConfigurationCommand implements Command {

		@Override
		public boolean interpret() throws InputMismatchException, NoSuchElementException, IOException {

			String configName = tokenizer.nextWordUpperCase();

			tokenizer.skipWordIgnoreCase(SCRIPT);
			String scriptName = tokenizer.nextWordUpperCase();

			String propName = null;
			if (tokenizer.skipWordIgnoreCase(PROP) || tokenizer.skipWordIgnoreCase(PROPERTIES)) {
				propName = tokenizer.nextWordUpperCase();
			}

			List<SimpleEntry<String, String>> arguments = new LinkedList<SimpleEntry<String, String>>();
			if (tokenizer.skipWordIgnoreCase(ARGS) || tokenizer.skipWordIgnoreCase(ARGUMENTS)) {
				do {
					String name = tokenizer.nextWordUpperCase();
					Object value = tokenizer.nextToken();
					if (value instanceof Quoted) {
						value = ((Quoted)value).getBody();
					}

					SimpleEntry<String, String> argument = new SimpleEntry<String, String>(name, value.toString());
					arguments.add(argument);

				} while (tokenizer.skipDelimiter(","));
			}

			nextEndOfLine();

			String message = controller.storeConfiguration(new ProcessConfiguration(null, configName, scriptName, propName, arguments));

			if (message != null) {
				System.out.print(message);
			}

			return false;
		}
	}

	private static class ListConfigurationsCommand implements Command {

		@Override
		public boolean interpret() throws IOException {

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

	private static class GetConfigurationCommand implements Command {

		@Override
		public boolean interpret() throws InputMismatchException, NoSuchElementException, IOException {

			String configName = tokenizer.nextWordUpperCase();

			nextEndOfLine();

			ProcessConfiguration config = controller.loadConfiguration(configName);

			printConfiguration(config);

			return false;
		}
	}

	private static void printConfiguration(ProcessConfiguration config) {

		System.out.print(config.processName);

		if (0 < config.id) {
			System.out.print(" SCRIPT ");
			System.out.print(config.scriptName);
	
			if (config.propName != null) {
				System.out.print(" PROPERTIES ");
				System.out.print(config.propName);
			}
	
			if ((config.arguments != null) && !config.arguments.isEmpty()) {
				System.out.print(" ARGUMENTS ");
				
				boolean first = true;
				for (SimpleEntry<String, String> argument : config.arguments) {
					if (!first) {
						System.out.print(", ");
					}
	
					System.out.print(argument.getKey());
					System.out.print("\t");
					System.out.print(argument.getValue());
	
					first = false;
				}
			}
		}

		System.out.println();
	}

	private static class DeleteConfigurationCommand extends UnaryMessageCommand {

		@Override
		protected String action(String name) {
			return controller.deleteConfiguration(name);
		}
	}

	private static class ListRunsCommand implements Command {

		@Override
		public boolean interpret() throws InputMismatchException, NoSuchElementException, IOException {

			String configName = null;
			if (!tokenizer.skipDelimiter("*")) {
				configName = tokenizer.nextWordUpperCase();
			}

			boolean latest = !tokenizer.skipDelimiter("*");

			nextEndOfLine();

			List<ProcessRun> runs = controller.listRuns(configName, latest);

			for (ProcessRun run : runs) {

				System.out.print(run.configName);

				if (0 < run.configId) {
					System.out.print("\t");
					System.out.print(run.runIndex);
					System.out.print("\t");
					System.out.print(run.status.name());
					System.out.print("\t");
					printDateTime(run.startTime);
					System.out.print("\t");
					printDateTime(run.endTime);
				}

				System.out.println();
			}

			return false;
		}
	}

	private static void printDateTime(LocalDateTime time) {
		final String nullTimeText = "          ---          ";
		System.out.print((time == null) ? nullTimeText : time.toString());
	}

	private static class StartupCommand extends NullaryMessageCommand {

		@Override
		protected String action() {
			return controller.startup();
		}
	}

	private static class RunCommand extends UnaryMessageCommand {

		@Override
		protected String action(String name) {
			ProcessRun run = controller.run(name);
			return (0 < run.configId) ? String.valueOf(run.runIndex) : run.configName;
		}
	}

	private static class StopCommand implements Command {

		@Override
		public boolean interpret() throws InputMismatchException, NoSuchElementException, IOException {

			String name = tokenizer.nextWord();
			
			boolean latest = !tokenizer.skipDelimiter("*");

			nextEndOfLine();

			// TODO: Add a method Controller.loadRunning() which doesn't go to the database
			// but instead asks the ProcessExecutor for the running processes from its map.
			List<ProcessRun> runs = controller.listRuns(name, latest);

			for (ProcessRun run : runs) {
				if (run.status == ProcessRun.Status.runInProgress) {

					String message = controller.stop(run);

					if (message != null) {
						System.out.println(message);
					}
				}
			}

			return false;
		}
	}

	private static class ShutdownCommand extends NullaryMessageCommand {

		@Override
		protected String action() {
			return controller.shutdown();
		}
	}

	private static class ExitCommand implements Command {

		@Override
		public boolean interpret() throws IOException {

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

	private static boolean skipWordPluralIgnoreCase(String word) throws IOException {
		return 
				tokenizer.skipWordIgnoreCase(word) ||
				tokenizer.skipWordIgnoreCase(word + 'S');
	}

	private static void nextEndOfLine() throws IOException {

		boolean atEndOfLine = !tokenizer.hasNextOnLine();

		skipPastEndOfLine();

		if (!atEndOfLine) {
			throw new RuntimeException("Unexpected tokens at end of command");
		}
	}

	private static void skipPastEndOfLine() throws IOException {
		while (tokenizer.nextTokenOnLine() != null) {}
	}
}
