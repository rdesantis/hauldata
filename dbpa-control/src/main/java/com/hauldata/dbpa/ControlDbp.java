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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.InputMismatchException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import javax.ws.rs.NotFoundException;

import com.hauldata.dbpa.control.interfaces.Jobs;
import com.hauldata.dbpa.control.interfaces.Manager;
import com.hauldata.dbpa.control.interfaces.Schedules;
import com.hauldata.dbpa.control.interfaces.Schema;
import com.hauldata.dbpa.control.interfaces.Scripts;
import com.hauldata.dbpa.control.interfaces.Service;
import com.hauldata.dbpa.manage.api.Job;
import com.hauldata.dbpa.manage.api.JobRun;
import com.hauldata.dbpa.manage.api.ScriptArgument;
import com.hauldata.util.tokenizer.BacktrackingTokenizer;
import com.hauldata.util.tokenizer.EndOfLine;
import com.hauldata.util.tokenizer.Token;
import com.hauldata.ws.rs.client.WebClient;

public class ControlDbp {

	enum KW {

		// Verbs

		PUT,
		GET,
		DELETE,
		LIST,
		VALIDATE,
		SHOW,
		UPDATE,
		STOP,
		START,
		CREATE,
		CONFIRM,

		EXIT,

		// Objects

		SCRIPT,
		PROPERTIES,
		SCHEDULE,
		JOB,
		RUN,
		MANAGER,
		SERVICE,
		SCHEMA,

		// Attributes

		ARGUMENT,
		ENABLED,

		// Other

		LATEST,
		RUNNING,

		LIKE,
		FROM,
		TO,
		ON,
		OFF,
		NO
	}

	static interface Command {
		boolean interpret(BacktrackingTokenizer tokenizer) throws IOException;
	}

	static interface TransitiveCommand extends Command {
		boolean supportsPlural();
	}

	static Map<String, TransitiveCommand> putCommands;
	static Map<String, TransitiveCommand> updateCommands;
	static Map<String, TransitiveCommand> validateCommands;
	static Map<String, TransitiveCommand> getCommands;
	static Map<String, TransitiveCommand> listCommands;
	static Map<String, TransitiveCommand> showCommands;
	static Map<String, TransitiveCommand> deleteCommands;
	static Map<String, TransitiveCommand> startCommands;
	static Map<String, TransitiveCommand> stopCommands;
	static Map<String, TransitiveCommand> createCommands;
	static Map<String, TransitiveCommand> confirmCommands;
	static Map<String, Map<String, TransitiveCommand>> transitiveCommandMaps;

	static Map<String, Command> intransitiveCommands;

	static Map<String, String> singulars;

	static Manager manager;
	static Schema schema;
	static Scripts scripts;
	static Schedules schedules;
	static Jobs jobs;
	static Service service;

	public static void main(String[] args) throws ReflectiveOperationException {

		initialize();
		interpretCommands();
		cleanup();
	}

	static void initialize() {

		// Commands

		putCommands = new HashMap<String, TransitiveCommand>();
		putCommands.put(KW.SCRIPT.name(), new PutScriptCommand());
		putCommands.put(KW.PROPERTIES.name(), new PutPropertiesCommand());
		putCommands.put(KW.SCHEDULE.name(), new PutScheduleCommand());
		putCommands.put(KW.JOB.name(), new PutJobCommand());

		updateCommands = new HashMap<String, TransitiveCommand>();
		updateCommands.put(KW.JOB.name(), new UpdateJobCommand());

		validateCommands = new HashMap<String, TransitiveCommand>();
		validateCommands.put(KW.SCRIPT.name(), new ValidateScriptCommand());
		validateCommands.put(KW.SCHEDULE.name(), new ValidateScheduleCommand());

		getCommands = new HashMap<String, TransitiveCommand>();
		getCommands.put(KW.SCRIPT.name(), new GetScriptCommand());
		getCommands.put(KW.PROPERTIES.name(), new GetPropertiesCommand());

		listCommands = new HashMap<String, TransitiveCommand>();
		listCommands.put(KW.SCRIPT.name(), new ListScriptsCommand());
		listCommands.put(KW.PROPERTIES.name(), new ListPropertiesCommand());
		listCommands.put(KW.SCHEDULE.name(), new ListSchedulesCommand());
		listCommands.put(KW.JOB.name(), new ListJobsCommand());
		listCommands.put(KW.RUNNING.name(), new ListRunningCommand());
		listCommands.put(KW.LATEST.name(), new ListLatestCommand());
		listCommands.put(KW.RUN.name(), new ListRunsCommand());

		showCommands = new HashMap<String, TransitiveCommand>();
		showCommands.put(KW.SCHEDULE.name(), new ShowSchedulesCommand());
		showCommands.put(KW.JOB.name(), new ShowJobCommand());
		showCommands.put(KW.RUNNING.name(), new ShowRunningCommand());

		deleteCommands = new HashMap<String, TransitiveCommand>();
		deleteCommands.put(KW.SCRIPT.name(), new DeleteScriptCommand());
		deleteCommands.put(KW.PROPERTIES.name(), new DeletePropertiesCommand());
		deleteCommands.put(KW.SCHEDULE.name(), new DeleteScheduleCommand());
		deleteCommands.put(KW.JOB.name(), new DeleteJobCommand());

		startCommands = new HashMap<String, TransitiveCommand>();
		startCommands.put(KW.JOB.name(), new StartJobCommand());
		startCommands.put(KW.MANAGER.name(), new StartManagerCommand());

		stopCommands = new HashMap<String, TransitiveCommand>();
		stopCommands.put(KW.JOB.name(), new StopJobCommand());
		stopCommands.put(KW.MANAGER.name(), new StopManagerCommand());
		stopCommands.put(KW.SERVICE.name(), new StopServiceCommand());

		createCommands = new HashMap<String, TransitiveCommand>();
		createCommands.put(KW.SCHEMA.name(), new CreateSchemaCommand());

		confirmCommands = new HashMap<String, TransitiveCommand>();
		confirmCommands.put(KW.MANAGER.name(), new ConfirmManagerCommand());
		confirmCommands.put(KW.SCHEMA.name(), new ConfirmSchemaCommand());

		transitiveCommandMaps = new HashMap<String, Map<String, TransitiveCommand>>();
		transitiveCommandMaps.put(KW.PUT.name(), putCommands);
		transitiveCommandMaps.put(KW.UPDATE.name(), updateCommands);
		transitiveCommandMaps.put(KW.VALIDATE.name(), validateCommands);
		transitiveCommandMaps.put(KW.GET.name(), getCommands);
		transitiveCommandMaps.put(KW.LIST.name(), listCommands);
		transitiveCommandMaps.put(KW.SHOW.name(), showCommands);
		transitiveCommandMaps.put(KW.DELETE.name(), deleteCommands);
		transitiveCommandMaps.put(KW.START.name(), startCommands);
		transitiveCommandMaps.put(KW.STOP.name(), stopCommands);
		transitiveCommandMaps.put(KW.CREATE.name(), createCommands);
		transitiveCommandMaps.put(KW.CONFIRM.name(), confirmCommands);

		intransitiveCommands = new HashMap<String, Command>();
		intransitiveCommands.put(KW.EXIT.name(), new ExitCommand());

		singulars = new HashMap<String, String>();
		addSingular(KW.SCRIPT.name());
		addSingular(KW.SCHEDULE.name());
		addSingular(KW.JOB.name());
		addSingular(KW.ARGUMENT.name());
		addSingular(KW.RUN.name());

		// Resources

		String baseUrl = "http://localhost:8080";

		WebClient client = new WebClient(baseUrl);

		try {
			manager = (Manager)client.getResource(Manager.class);
			schema = (Schema)client.getResource(Schema.class);
			scripts = (Scripts)client.getResource(Scripts.class);
			schedules = (Schedules)client.getResource(Schedules.class);
			jobs = (Jobs)client.getResource(Jobs.class);
			service = (Service)client.getResource(Service.class);

			boolean isStarted = manager.isStarted();
			System.out.printf("Manager is %sstarted.\n", isStarted ? "" : "not ");
		}
		catch (Exception ex) {
			System.err.println("Error instantiating resources:" + ex.getLocalizedMessage());
			System.exit(2);
		}
	}

	static void addSingular(String noun) {
		singulars.put(noun + "S", noun);
	}

	static String getSingular(String noun) {
		String singular = singulars.get(noun);
		return (singular != null) ? singular : noun;
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

					String verb = tokenizer.nextWordUpperCase();
					Command command = intransitiveCommands.get(verb);

					if (command == null && tokenizer.hasNextWord()) {

						Map<String, TransitiveCommand> transitiveCommands = transitiveCommandMaps.get(verb);
						if (transitiveCommands != null) {

							String noun = tokenizer.nextWordUpperCase();
							String singularNoun = getSingular(noun);
							TransitiveCommand transitiveCommand = transitiveCommands.get(singularNoun);

							if (transitiveCommand != null) {
								boolean isPlural = !noun.equals(singularNoun);
								if (isPlural && !transitiveCommand.supportsPlural()) {
									throw new InputMismatchException(verb + " only allows " + singularNoun +", not " + noun);
								}
							}
							else {
								throw new NoSuchElementException("Unrecognized object for " + verb + " command: " + noun);
							}

							command = transitiveCommand;
						}
					}

					if (command == null) {
						throw new NoSuchElementException("Unrecognized command: " + verb);
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

	static class PutScriptCommand implements TransitiveCommand {

		@Override
		public boolean supportsPlural() { return false; }

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {
			// TODO: Must implement!
			throw new InputMismatchException("Command is not implemented");
		}
	}

	static class PutPropertiesCommand implements TransitiveCommand {

		@Override
		public boolean supportsPlural() { return false; }

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {
			// TODO: Must implement!
			throw new InputMismatchException("Command is not implemented");
		}
	}

	static class PutScheduleCommand implements TransitiveCommand {

		@Override
		public boolean supportsPlural() { return false; }

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {

			String name = nextEntityName(tokenizer);
			String schedule = tokenizer.nextQuoted().getBody();
			endOfLine(tokenizer);

			schedules.put(name, schedule);

			return false;
		}
	}

	static class PutJobCommand extends WriteJobCommand {

		@Override
		protected boolean execute(String name, Set<Component> components, Job job) {

			if (job.getScriptName() == null) {
				throw new InputMismatchException("Script name is required");
			}

			jobs.put(name, job);

			return false;
		}
	}

	static class UpdateJobCommand extends WriteJobCommand {

		@Override
		protected boolean execute(String name, Set<Component> components, Job job) {

			if (components.contains(Component.script)) {
				jobs.putScriptName(name, job.getScriptName());
			}
			if (components.contains(Component.properties)) {
				if (job.getPropName() != null) {
					jobs.putPropName(name, job.getPropName());
				}
				else {
					jobs.deletePropName(name);
				}
			}
			if (components.contains(Component.arguments)) {
				if (job.getArguments() != null) {
					jobs.putArguments(name, job.getArguments());
				}
				else {
					jobs.deleteArguments(name);
				}
			}
			if (components.contains(Component.schedules)) {
				if (job.getScheduleNames() != null) {
					jobs.putScheduleNames(name, job.getScheduleNames());
				}
				else {
					jobs.deleteScheduleNames(name);
				}
			}
			if (components.contains(Component.enabled)) {
				jobs.putEnabled(name, job.isEnabled());
			}

			return false;
		}
	}

	static abstract class WriteJobCommand implements TransitiveCommand {

		protected enum Component { script, properties, arguments, schedules, enabled };

		@Override
		public boolean supportsPlural() { return false; }

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {

			String name = nextEntityName(tokenizer);

			Set<Component> components = new HashSet<Component>();

			String scriptName = null;
			if (tokenizer.skipWordIgnoreCase(KW.SCRIPT.name())) {
				components.add(Component.script);
				scriptName = nextEntityName(tokenizer);
			}

			boolean none;
			none = tokenizer.skipWordIgnoreCase(KW.NO.name());

			String propName = null;
			if (tokenizer.skipWordIgnoreCase(KW.PROPERTIES.name())) {
				components.add(Component.properties);
				if (!none) {
					propName = nextEntityName(tokenizer);
				}
				none = tokenizer.skipWordIgnoreCase(KW.NO.name());
			}

			List<ScriptArgument> arguments = null;
			if (skipWordOrPluralIgnoreCase(tokenizer, KW.ARGUMENT.name())) {
				components.add(Component.arguments);
				if (!none) {
					arguments = nextArguments(tokenizer);
				}
				none = tokenizer.skipWordIgnoreCase(KW.NO.name());
			}

			List<String> scheduleNames = null;
			if (skipWordOrPluralIgnoreCase(tokenizer, KW.SCHEDULE.name())) {
				components.add(Component.schedules);
				if (!none) {
					scheduleNames = nextEntityNames(tokenizer);
				}
				none = false;
			}

			if (none) {
				throw new InputMismatchException("Unexpected token: " + KW.NO.name());
			}

			boolean enabled = true;
			if (tokenizer.skipWordIgnoreCase(KW.ENABLED.name())) {
				components.add(Component.enabled);
				if (tokenizer.skipWordIgnoreCase(KW.ON.name())) {
					enabled = true;
				}
				else if (tokenizer.skipWordIgnoreCase(KW.OFF.name())) {
					enabled = false;
				}
				else {
					throw new InputMismatchException("Invalid ENABLED value: " + tokenizer.nextToken().getImage());
				}
			}

			endOfLine(tokenizer);

			Job job = new Job(scriptName, propName, arguments, scheduleNames, enabled);

			return execute(name, components, job);
		}

		protected abstract boolean execute(String name, Set<Component> components, Job job);
	}

	private static List<ScriptArgument> nextArguments(BacktrackingTokenizer tokenizer) throws InputMismatchException, IOException {

		List<ScriptArgument> arguments = new LinkedList<ScriptArgument>();

		do {
			String name = tokenizer.nextWordUpperCase();

			String value = null;
			if (tokenizer.hasNextWord()) {
				value = tokenizer.nextWord();
			}
			else if (tokenizer.hasNextQuoted()) {
				value = tokenizer.nextQuoted().getBody();
			}
			else if (tokenizer.hasNextInt()) {
				value = tokenizer.nextToken().getImage();
			}
			else {
				throw new InputMismatchException("Valid argument value not found for " + name);
			}

			arguments.add(new ScriptArgument(name, value));
		} while (tokenizer.skipDelimiter(","));

		return arguments;
	}

	private static List<String> nextEntityNames(BacktrackingTokenizer tokenizer) throws InputMismatchException, IOException {

		List<String> names = new LinkedList<String>();

		do {
			names.add(nextEntityName(tokenizer));
		} while (tokenizer.skipDelimiter(","));

		return names;
	}

	static class ValidateScriptCommand extends StandardDisplayCommand {

		@Override
		protected String get(String name) {
			return scripts.validate(name).toString();
		}
	}

	static class ValidateScheduleCommand extends StandardDisplayCommand {

		@Override
		protected String get(String name) {
			return schedules.validate(name).toString();
		}
	}

	static class GetScriptCommand extends StandardDisplayCommand {

		@Override
		protected String get(String name) {
			// TODO: This is a temporary implementation.  Actual implementation should write script to file.
			return scripts.get(name);
		}
	}

	static class GetPropertiesCommand implements TransitiveCommand {

		@Override
		public boolean supportsPlural() { return false; }

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {
			// TODO: Must implement!
			throw new InputMismatchException("Command is not implemented");
		}
	}

	static class ListScriptsCommand extends StandardListCommand {

		@Override
		protected Collection<?> getObjects(String likeName) {
			return scripts.getNames(likeName);
		}
	}

	static class ListSchedulesCommand extends StandardListCommand {

		@Override
		public Collection<?> getObjects(String likeName) {
			return schedules.getNames(likeName);
		}
	}

	static class ListJobsCommand extends StandardListCommand {

		@Override
		public Collection<?> getObjects(String likeName) {
			return jobs.getNames(likeName);
		}
	}

	static class ListPropertiesCommand implements TransitiveCommand {

		@Override
		public boolean supportsPlural() { return true; }

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {
			// TODO: Must implement!
			throw new InputMismatchException("Command is not implemented");
		}
	}

	static class ListRunningCommand implements TransitiveCommand {

		@Override
		public boolean supportsPlural() { return true; }

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {

			endOfLine(tokenizer);

			// Execute.

			List<JobRun> runs = jobs.getRunning();

			for (JobRun run : runs) {
				System.out.printf("%s\n", run.toString());
			}

			return false;
		}
	}

	static class ListLatestCommand extends ListAnyRunsCommand {

		public ListLatestCommand() {
			super(true);
		}

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {

			String noun = tokenizer.nextWordUpperCase();
			if (!getSingular(noun).equals(KW.RUN.name())) {
				throw new NoSuchElementException("Unrecognized object for " + KW.LIST.name() + " " + KW.LATEST.name() + " command: " + noun);
			}

			return super.interpret(tokenizer);
		}
	}

	static class ListRunsCommand extends ListAnyRunsCommand {

		public ListRunsCommand() {
			super(false);
		}
	}

	static class ListAnyRunsCommand extends StandardListCommand {

		protected boolean latest;

		protected ListAnyRunsCommand(boolean latest) {
			this.latest = latest;
		}

		@Override
		public Collection<?> getObjects(String likeName) {
			return jobs.getRuns(likeName, latest);
		}
	}

	static class ShowSchedulesCommand extends StandardListCommand {

		@Override
		public Collection<?> getObjects(String likeName) {
			Map<String, String> scheduleMap = schedules.getAll(likeName);
			return scheduleMap.entrySet();
		}
	}

	static class ShowJobCommand extends StandardDisplayCommand {

		@Override
		protected String get(String name) {
			return jobs.get(name).toString();
		}
	}

	static class ShowRunningCommand implements TransitiveCommand {

		@Override
		public boolean supportsPlural() { return false; }

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {

			int id = tokenizer.nextInt();

			endOfLine(tokenizer);

			JobRun run = jobs.getRunning(id);

			System.out.printf("%s\n", run.toString());

			return false;
		}
	}

	static class DeleteScriptCommand extends StandardDeleteCommand {

		@Override
		protected KW delete(String name) {
			scripts.delete(name);
			return KW.SCRIPT;
		}
	}

	static class DeletePropertiesCommand extends StandardDeleteCommand {

		@Override
		protected KW delete(String name) {
			// TODO: Must implement!
			throw new InputMismatchException("Command is not implemented");
		}
	}

	static class DeleteScheduleCommand extends StandardDeleteCommand {

		@Override
		protected KW delete(String name) {
			schedules.delete(name);
			return KW.SCHEDULE;
		}
	}

	static class DeleteJobCommand extends StandardDeleteCommand {

		@Override
		protected KW delete(String name) {
			jobs.delete(name);
			return KW.JOB;
		}
	}

	static abstract class StandardDeleteCommand extends StandardNamedObjectCommand {

		@Override
		protected String execute(String name) {

			KW kw = delete(name);

			String message = String.format("Deleted %s: %s\n", kw.name().toLowerCase(), name);

			return message;
		}

		protected abstract KW delete(String name);
	}

	static class StartJobCommand extends StandardNamedObjectCommand {

		@Override
		protected String execute(String name) {

			int id = jobs.run(name);

			String message = String.format("Running job \"%s\" with ID %d\n", name, id);

			return message;
		}
	}

	static class StartManagerCommand extends StandardSingletonCommand {

		@Override
		protected String execute() {
			manager.startup();
			return "Manager started\n";
		}
	}

	static class StopJobCommand implements TransitiveCommand {

		@Override
		public boolean supportsPlural() { return false; }

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {

			int id = tokenizer.nextInt();

			endOfLine(tokenizer);

			jobs.stop(id);

			System.out.printf("Job stopped\n");

			return false;
		}
	}

	static class StopManagerCommand extends StandardSingletonCommand {

		@Override
		protected String execute() {
			manager.shutdown();
			return "Manager stopped\n";
		}
	}

	static class StopServiceCommand extends StandardSingletonCommand {

		@Override
		protected String execute() {
			service.kill();
			return "Service stopped\n";
		}
	}

	static class CreateSchemaCommand extends StandardSingletonCommand {

		@Override
		protected String execute() {
			schema.create();
			return "Schema created\n";
		}
	}

	static class ConfirmManagerCommand implements TransitiveCommand {

		@Override
		public boolean supportsPlural() { return false; }

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {

			endOfLine(tokenizer);

			boolean isStarted = manager.isStarted();
			System.out.printf("Manager is %sstarted.\n", isStarted ? "" : "not ");

			return false;
		}
	}

	static class ConfirmSchemaCommand implements TransitiveCommand {

		@Override
		public boolean supportsPlural() { return false; }

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {

			endOfLine(tokenizer);

			boolean exists = schema.confirm();
			System.out.printf("Schema does %sexist.\n", exists ? "" : "not ");

			return false;
		}
	}

	static class ExitCommand implements Command {

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {

			endOfLine(tokenizer);
			return true;
		}
	}

	static abstract class StandardDisplayCommand implements TransitiveCommand {

		@Override
		public boolean supportsPlural() { return false; }

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {

			String name = nextEntityName(tokenizer);

			endOfLine(tokenizer);

			System.out.printf("%s\n", get(name));

			return false;
		}

		protected abstract String get(String name);
	}

	static abstract class StandardListCommand implements TransitiveCommand {

		@Override
		public boolean supportsPlural() { return true; }

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {

			// Parse.

			String likeName = null;
			if (tokenizer.skipWordIgnoreCase(KW.LIKE.name())) {
				likeName = tokenizer.nextQuoted().getBody();
			}

			endOfLine(tokenizer);

			// Execute.

			Collection<?> objects = getObjects(likeName);

			for (Object object : objects) {
				System.out.printf("%s\n", object.toString());
			}

			return false;
		}

		protected abstract Collection<?> getObjects(String likeName);
	}

	static abstract class StandardNamedObjectCommand implements TransitiveCommand {

		@Override
		public boolean supportsPlural() { return false; }

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {

			// Parse.

			String name = nextEntityName(tokenizer);

			endOfLine(tokenizer);

			// Execute.

			String message = execute(name);

			if (message != null) {
				System.out.printf(message);
			}

			return false;
		}

		protected abstract String execute(String name);
	}

	static abstract class StandardSingletonCommand implements TransitiveCommand {

		@Override
		public boolean supportsPlural() { return false; }

		@Override
		public boolean interpret(BacktrackingTokenizer tokenizer) throws IOException {

			endOfLine(tokenizer);

			String message = execute();

			if (message != null) {
				System.out.printf(message);
			}

			return false;
		}

		protected abstract String execute();
	}

	private static boolean skipWordOrPluralIgnoreCase(BacktrackingTokenizer tokenizer, String word) throws InputMismatchException, IOException {
		return
				tokenizer.skipWordIgnoreCase(word) ||
				tokenizer.skipWordIgnoreCase(word + "S");
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
