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
import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.ws.rs.NotFoundException;

import com.hauldata.dbpa.control.interfaces.Jobs;
import com.hauldata.dbpa.control.interfaces.Manager;
import com.hauldata.dbpa.control.interfaces.Schedules;
import com.hauldata.dbpa.control.interfaces.Schema;
import com.hauldata.dbpa.control.interfaces.Scripts;
import com.hauldata.util.tokenizer.BacktrackingTokenizer;
import com.hauldata.util.tokenizer.EndOfLine;
import com.hauldata.util.tokenizer.Token;
import com.hauldata.ws.rs.client.WebClient;

public class ControlDbp {

	enum KW {

		// Commands

		PUT,
		GET,
		DELETE,
		LIST,
		VALIDATE,
		SHOW,
		UPDATE,
		RUN,
		STOP,
		START,
		CONFIRM,

		EXIT,

		// Objects

		SCRIPT,
		PROPERTIES,
		SCHEDULE,
		JOB,
		//RUN,		// Also a command name
		RUNNING,
		MANAGER,
		SERVICE,
		SCHEMA,

		// Attributes

		ARGUMENTS,
		ENABLED,

		// Other

		LIKE,
		FROM,
		TO,
		ON,
		OFF,
		NO
	}

	static Map<String, Command> commands;
	static Map<String, Command> putCommands;
	static Map<String, Command> showCommands;
	static Map<String, ResourceList> resourceLists;

	static Manager manager;
	static Schema schema;
	static Scripts scripts;
	static Schedules schedules;
	static Jobs jobs;

	public static void main(String[] args) throws ReflectiveOperationException {

		initialize();
		interpretCommands();
		cleanup();
	}

	static void initialize() {

		// Commands

		commands = new HashMap<String, Command>();
		commands.put(KW.PUT.name(), new PutCommand());
		commands.put(KW.GET.name(), new GetCommand());
		commands.put(KW.DELETE.name(), new DeleteCommand());
		commands.put(KW.LIST.name(), new ListCommand());
		commands.put(KW.VALIDATE.name(), new ValidateCommand());
		commands.put(KW.SHOW.name(), new ShowCommand());
		commands.put(KW.UPDATE.name(), new UpdateCommand());
		commands.put(KW.RUN.name(), new RunCommand());
		commands.put(KW.STOP.name(), new StopCommand());
		commands.put(KW.START.name(), new StartCommand());
		commands.put(KW.CONFIRM.name(), new ConfirmCommand());
		commands.put(KW.EXIT.name(), new ExitCommand());

		putCommands = new HashMap<String, Command>();
		putCommands.put(KW.SCRIPT.name(), new PutScriptCommand());
		putCommands.put(KW.PROPERTIES.name(), new PutPropertiesCommand());
		putCommands.put(KW.SCHEDULE.name(), new PutScheduleCommand());
		putCommands.put(KW.JOB.name(), new PutJobCommand());

		showCommands = new HashMap<String, Command>();
		showCommands.put(KW.SCHEDULE.name(), new ShowScheduleCommand());
		showCommands.put(KW.JOB.name(), new ShowJobCommand());
		showCommands.put(KW.RUN.name(), new ShowRunCommand());

		// Resource lists

		resourceLists = new HashMap<String, ResourceList>();
		resourceLists.put(KW.SCRIPT.name() + "S", new ScriptsList());
		resourceLists.put(KW.SCHEDULE.name() + "S", new SchedulesList());
		resourceLists.put(KW.JOB.name() + "S", new JobsList());

		// Resources

		String baseUrl = "http://localhost:8080";

		WebClient client = new WebClient(baseUrl);

		try {
			manager = (Manager)client.getResource(Manager.class);
			schema = (Schema)client.getResource(Schema.class);
			scripts = (Scripts)client.getResource(Scripts.class);
			schedules = (Schedules)client.getResource(Schedules.class);
			jobs = (Jobs)client.getResource(Jobs.class);

			boolean isStarted = manager.isStarted();
			System.out.printf("Manager is %sstarted.\n", isStarted ? "" : "not ");
		}
		catch (ReflectiveOperationException ex) {
			System.err.println("Error instantiating resources.");
			System.exit(2);
		}

	}

	static void interpretCommands() {

		BacktrackingTokenizer tokenizer = new BacktrackingTokenizer(new InputStreamReader(System.in));
		tokenizer.eolIsSignificant(true);
		tokenizer.wordChars('_', '_');

		try {
			boolean done = false;
			do {
				try {
					System.out.print("Command: ");

					String commandName = tokenizer.nextWordUpperCase();
					Command command = commands.get(commandName);

					if (command == null) {
						throw new NoSuchElementException("Unrecognized command: " + commandName);
					}

					done = command.interpret(tokenizer);
				}
				catch (NoSuchElementException | NotFoundException e) {
					System.err.println(e.getMessage());

					while (!EndOfLine.value.equals(tokenizer.lastToken())) {
						tokenizer.nextToken();
					}
				}
			} while (!done);
		}
		catch (IOException e) {
			System.err.println(e.getMessage());
			System.exit(3);
		}
		finally {
			try { tokenizer.close(); } catch (IOException e) {}
		}
	}

	static void cleanup() {
		// Nothing to do.
	}

	static interface Command  { boolean interpret(BacktrackingTokenizer tokenizer) throws IOException; }

	static class PutCommand implements Command {

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {

			String typeName = tokenizer.nextWordUpperCase();

			Command putCommand = putCommands.get(typeName);
			if (putCommand == null) {
				throw new InputMismatchException("Can't put this: " + typeName);
			}

			return putCommand.interpret(tokenizer);
		}
	}

	static class PutScriptCommand implements Command {

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {
			// TODO: Must implement!
			throw new InputMismatchException("Command is not implemented");
		}
	}

	static class PutPropertiesCommand implements Command {

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {
			// TODO: Must implement!
			throw new InputMismatchException("Command is not implemented");
		}
	}

	static class PutScheduleCommand implements Command {

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {

			String name = nextEntityName(tokenizer);
			String schedule = tokenizer.nextQuoted().getBody();
			endOfLine(tokenizer);

			schedules.put(name, schedule);

			return false;
		}
	}

	static class PutJobCommand implements Command {

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {
			// TODO: Must implement!
			throw new InputMismatchException("Command is not implemented");
		}
	}

	static class GetCommand implements Command {

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {
			// TODO: Must implement!
			throw new InputMismatchException("Command is not implemented");
		}
	}

	static class DeleteCommand implements Command {

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {
			// TODO: Must implement!
			throw new InputMismatchException("Command is not implemented");
		}
	}

	static class ListCommand implements Command {

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {

			// Parse the command.

			String typeName = tokenizer.nextWordUpperCase();
			if (!typeName.endsWith("S")) {
				typeName += "S";
			}

			ResourceList resourceList = resourceLists.get(typeName);
			if (resourceList == null) {
				throw new InputMismatchException("Can't list this: " + typeName);
			}

			String likeName = null;
			if (tokenizer.skipWordIgnoreCase(KW.LIKE.name())) {
				likeName = tokenizer.nextQuoted().getBody();
			}

			endOfLine(tokenizer);

			// Execute the command.

			List<String> names = resourceList.get(likeName);

			for (String name : names) {
				System.out.printf("%s\n", name);
			}

			return false;
		}
	}

	static interface ResourceList { List<String> get(String likeName); }

	static class ScriptsList implements ResourceList {

		@Override
		public List<String> get(String likeName) {
			return scripts.getNames(likeName);
		}
	}

	static class SchedulesList implements ResourceList {

		@Override
		public List<String> get(String likeName) {
			return schedules.getNames(likeName);
		}
	}

	static class JobsList implements ResourceList {

		@Override
		public List<String> get(String likeName) {
			return jobs.getNames(likeName);
		}
	}

	static class ValidateCommand implements Command {

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {
			// TODO: Must implement!
			throw new InputMismatchException("Command is not implemented");
		}
	}

	static class ShowCommand implements Command {

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {

			String typeName = tokenizer.nextWordUpperCase();

			Command showCommand = showCommands.get(typeName);
			if (showCommand == null) {
				throw new InputMismatchException("Can't show this: " + typeName);
			}

			return showCommand.interpret(tokenizer);
		}
	}

	static class ShowScheduleCommand implements Command {

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {

			String name = nextEntityName(tokenizer);
			endOfLine(tokenizer);

			String schedule = schedules.get(name);

			System.out.printf("%s\n", schedule);

			return false;
		}
	}

	static class ShowJobCommand implements Command {

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {
			// TODO: Must implement!
			throw new InputMismatchException("Command is not implemented");
		}
	}

	static class ShowRunCommand implements Command {

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {
			// TODO: Must implement!
			throw new InputMismatchException("Command is not implemented");
		}
	}

	static class UpdateCommand implements Command {

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {
			// TODO: Must implement!
			throw new InputMismatchException("Command is not implemented");
		}
	}

	static class RunCommand implements Command {

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {
			// TODO: Must implement!
			throw new InputMismatchException("Command is not implemented");
		}
	}

	static class StopCommand implements Command {

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {
			// TODO: Must implement!
			throw new InputMismatchException("Command is not implemented");
		}
	}

	static class StartCommand implements Command {

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {
			// TODO: Must implement!
			throw new InputMismatchException("Command is not implemented");
		}
	}

	static class ConfirmCommand implements Command {

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {
			// TODO: Must implement!
			throw new InputMismatchException("Command is not implemented");
		}
	}

	static class ExitCommand implements Command {

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {

			endOfLine(tokenizer);
			return true;
		}
	}

	private static String nextEntityName(BacktrackingTokenizer tokenizer) throws InputMismatchException, IOException {
		if (!tokenizer.hasNextOnLine()) {
			throw new InputMismatchException("Expected entity name, found end of line");
		}
		if (tokenizer.hasNextWord()) {
			return tokenizer.nextWordUpperCase();
		}
		if (tokenizer.hasNextQuoted()) {
			return tokenizer.nextQuoted().getBody();
		}
		throw new InputMismatchException("Expected entity name, found: " + tokenizer.nextToken().getImage());
	}

	private static void endOfLine(BacktrackingTokenizer tokenizer) throws InputMismatchException, IOException {
		Token token = tokenizer.nextTokenOnLine();
		if (token != null) {
			throw new InputMismatchException("Unexpected token on line: " + token.getImage());
		}
	}
}
