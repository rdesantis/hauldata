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
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.AbstractMap.SimpleEntry;

import javax.naming.NameAlreadyBoundException;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;

import com.hauldata.dbpa.connection.Connection;
import com.hauldata.dbpa.connection.DatabaseConnection;
import com.hauldata.dbpa.connection.EmailConnection;
import com.hauldata.dbpa.connection.FtpConnection;
import com.hauldata.dbpa.expression.*;
import com.hauldata.dbpa.file.*;
import com.hauldata.dbpa.task.*;
import com.hauldata.dbpa.task.Task.Prologue;
import com.hauldata.dbpa.variable.*;
import com.hauldata.util.schedule.ScheduleSet;
import com.hauldata.util.tokenizer.*;


/**
 * Base class parser for a set of tasks at either outer process level or inner nested level
 */
abstract class TaskSetParser {

	TaskSetParser thisTaskParser;

	/**
	 * Keywords
	 */
	public enum KW {

		// Process sections

		PARAMETERS,
		VARIABLES,
		CONNECTIONS,

		// Data types

		TINYINT,
		INT,
		INTEGER,
		VARCHAR,
		CHAR,
		CHARACTER,
		DATETIME,
		DATE,

		// Connection types

		DATABASE,
		FTP,
		//EMAIL,	// Also a task type

		// Task delimiters

		TASK,
		END,

		// Task predecessors and conditions

		AFTER,
		PREVIOUS,
		SUCCEEDS,
		FAILS,
		COMPLETES,
		IF,

		// Boolean terms

		AND,
		OR,
		NOT,

		// Task types

		SET,
		UPDATE,
		RUN,
		CREATE,
		APPEND,
		WRITE,
		OPEN,
		LOAD,
		READ,
		ZIP,
		UNZIP,
		PUT,
		GET,
		EMAIL,
		DELETE,
		RENAME,
		COPY,
		MAKE,
		LOG,
		GO,
		FAIL,
		PROCESS,
		DO,
		FOR,
		ON,
		WAITFOR,
		CONNECT,
		// Future:
		CLOSE,
		REMOVE,
		SEND,
		RECEIVE,
		SYSTEM,
		EXECUTE,
		CALL,


		// Task data sources and targets

		STATEMENT,
		SQL,
		TABLE,
		SCRIPT,
		VALUES,
		FILES,
		// Future:
		PROCEDURE,
		FILE,
		VARIABLE,

		// Task parameters

		CONNECTION,
		NAME,
		FROM,
		INTO,
		CSV,
		TSV,
		TXT,
		XLSX,
		SHEET,
		WITH,
		NO,
		IGNORE,
		HEADERS,
		COLUMNS,
		PREFIX,
		BINARY,
		ASCII,
		THROUGH,
		TO,
		CC,
		SUBJECT,
		BODY,
		ATTACH,
		DIRECTORY,
		SYNC,
		SYNCHRONOUSLY,
		ASYNC,
		ASYNCHRONOUSLY,
		WHILE,
		SCHEDULE,
		DELAY,
		TIME,
		DEFAULT,
		SUBSTITUTING,
		// Future:
		ATTACHMENT,
		IN,
		OUT,
		OUTPUT,

		// Functions

		ISNULL,
		IIF,
		DATEPART,
		YEAR,
		MONTH,
		DAY,
		WEEKDAY,
		HOUR,
		MINUTE,
		SECOND,
		FORMAT,
		GETDATE,
		DATEADD,
		DATEFROMPARTS,
		DATETIMEFROMPARTS,

		IS,
		NULL,

		// Future:
		CASE,
		WHEN,
		THEN,
		ELSE,
		BEGIN
	}

	/**
	 * Sorted sets of keyword names.
	 */
	public static TreeSet<String> taskTypeNames;
	public static TreeSet<String> reservedTaskNames;
	public static TreeSet<String> reservedVariableNames;
	public static TreeSet<String> reservedConnectionNames;

	static {
		KW firstTaskTypeKeyword = KW.SET;
		KW lastTaskTypeKeyword = KW.CALL;

		taskTypeNames = Stream.of(KW.values()).filter(
				kw -> (kw.compareTo(firstTaskTypeKeyword) >= 0) && (kw.compareTo(lastTaskTypeKeyword) <= 0)
				).map(Enum::name).collect(Collectors.toCollection(TreeSet::new));

		reservedTaskNames = Stream.of(new KW[]
				{
						KW.NAME,
						KW.END,
						KW.TASK,

						KW.AFTER,
						KW.PREVIOUS,
						KW.SUCCEEDS,
						KW.FAILS,
						KW.COMPLETES,
						KW.IF
				}).map(Enum::name).collect(Collectors.toCollection(TreeSet::new));

		reservedVariableNames = Stream.of(new KW[]
				{
						KW.NULL,
						KW.CASE,

						KW.END,
						KW.TASK
				}).map(Enum::name).collect(Collectors.toCollection(TreeSet::new));

		reservedConnectionNames = Stream.of(new KW[]
				{
						KW.CONNECTION,
						KW.DEFAULT,

						KW.STATEMENT,
						KW.SQL,
						KW.TABLE,
						KW.SCRIPT,
						KW.VALUES,
						KW.PROCEDURE,
						KW.FILE,
						KW.FILES,
						KW.VARIABLE,

						KW.END,
						KW.TASK
				}).map(Enum::name).collect(Collectors.toCollection(TreeSet::new));
	}

	// Parse-time data

	protected BacktrackingTokenizer tokenizer;

	protected TermParser<Integer> integerTermParser;
	protected TermParser<String> stringTermParser;
	protected TermParser<LocalDateTime> datetimeTermParser;

	protected Map<String, TaskParser> taskParsers;

	// Parse-time through run-time data

	protected Map<String, VariableBase> variables;
	protected Map<String, Connection> connections;
	protected Map<String, Task> tasks;

	/**
	 * Constructor for outermost task set parser, i.e., process parser
	 * @param r
	 * @param variables
	 */
	public TaskSetParser(Reader r) {

		thisTaskParser = this;

		setupVariables();

		setupConnections();

		setupParsing(r);
	}

	/**
	 * Constructor for nested task set parser
	 * @param parent is the parser for the enclosing task set
	 * @param tasks
	 */
	public TaskSetParser(TaskSetParser parent) {

		thisTaskParser = this;

		reuseVariables(parent);

		reuseConnections(parent);

		reuseParsing(parent);
	}

	/**
	 * Parse a set of tasks
	 *
	 * @throws IOException
	 * @throws InputMismatchException
	 * @throws NoSuchElementException
	 * @throws NameNotFoundException
	 * @throws NameAlreadyBoundException
	 * @throws NamingException
	 */
	public void parseTasks() throws IOException, InputMismatchException, NoSuchElementException, NameNotFoundException, NameAlreadyBoundException, NamingException {

		tasks = new HashMap<String, Task>();

		int taskIndex = 1;
		Task previousTask = null;
		while (tokenizer.hasNextWordIgnoreCase(KW.TASK.name())) {

			Task task = parseTask(previousTask);
			if (task.getName() == null) {
				task.setAnonymousIndex(taskIndex);
			}
			tasks.put(task.getName(), task);

			++taskIndex;
			previousTask = task;
		}

		determineSuccessors();
	}

	private void determineSuccessors() {
		for (Task task : tasks.values()) {
			for (Task potentialSuccessor : tasks.values()) {
				if (potentialSuccessor.getPredecessors().containsKey(task)) {
					task.addSuccessor(potentialSuccessor);
				}
			}
		}
	}

	/**
	 * Close for outermost task set parser only!, i.e., process parser
	 */
	public void close() throws IOException {
		cleanupParsing();
	}

	private void setupVariables() {
		this.variables = new HashMap<String, VariableBase>();
	}

	private void reuseVariables(TaskSetParser parent) {
		this.variables = parent.variables;
	}

	private void setupConnections() {
		this.connections = new HashMap<String, Connection>();
	}

	private void reuseConnections(TaskSetParser parent) {
		this.connections = parent.connections;
	}

	private void setupParsing(Reader r) {

		tokenizer = new BacktrackingTokenizer(r);

		tokenizer.useDelimiter("<=");
		tokenizer.useDelimiter(">=");
		tokenizer.useDelimiter("<>");
		tokenizer.useEndLineCommentDelimiter("--");

		tokenizer.wordChars('_', '_');

		tokenizer.eolIsSignificant(false);

		integerTermParser = new TermParser<Integer>() { public Expression<Integer> parseExpression() throws IOException { return parseIntegerExpression(); } };
		stringTermParser = new TermParser<String>() { public Expression<String> parseExpression() throws IOException { return parseStringExpression(); } };
		datetimeTermParser = new TermParser<LocalDateTime>() { public Expression<LocalDateTime> parseExpression() throws IOException { return parseDatetimeExpression(); } };

		taskParsers = new HashMap<String, TaskParser>();

		taskParsers.put(KW.SET.name(), new SetVariablesTaskParser());
		taskParsers.put(KW.UPDATE.name(), new UpdateTaskParser());
		taskParsers.put(KW.RUN.name(), new RunTaskParser());
		taskParsers.put(KW.CREATE.name(), new CreateTaskParser());
		taskParsers.put(KW.APPEND.name(), new AppendTaskParser());
		taskParsers.put(KW.WRITE.name(), new WriteTaskParser());
		taskParsers.put(KW.OPEN.name(), new OpenTaskParser());
		taskParsers.put(KW.LOAD.name(), new LoadTaskParser());
		taskParsers.put(KW.READ.name(), new ReadTaskParser());
		taskParsers.put(KW.ZIP.name(), new ZipTaskParser());
		taskParsers.put(KW.UNZIP.name(), new UnzipTaskParser());
		taskParsers.put(KW.PUT.name(), new PutTaskParser());
		taskParsers.put(KW.GET.name(), new GetTaskParser());
		taskParsers.put(KW.EMAIL.name(), new EmailTaskParser());
		taskParsers.put(KW.DELETE.name(), new DeleteTaskParser());
		taskParsers.put(KW.RENAME.name(), new RenameTaskParser());
		taskParsers.put(KW.COPY.name(), new CopyTaskParser());
		taskParsers.put(KW.MAKE.name(), new MakeDirectoryTaskParser());
		taskParsers.put(KW.LOG.name(), new LogTaskParser());
		taskParsers.put(KW.GO.name(), new GoTaskParser());
		taskParsers.put(KW.FAIL.name(), new FailTaskParser());
		taskParsers.put(KW.PROCESS.name(), new ProcessTaskParser());
		taskParsers.put(KW.DO.name(), new DoTaskParser());
		taskParsers.put(KW.FOR.name(), new ForTaskParser());
		taskParsers.put(KW.ON.name(), new OnTaskParser());
		taskParsers.put(KW.WAITFOR.name(), new WaitforTaskParser());
		taskParsers.put(KW.CONNECT.name(), new ConnectTaskParser());

		KW firstNotImplemented = KW.CLOSE;
		KW lastNotImplemented = KW.CALL;

		for (KW notImplemented : Stream.of(KW.values()).filter(
				kw -> (kw.compareTo(firstNotImplemented) >= 0) && (kw.compareTo(lastNotImplemented) <= 0)).collect(Collectors.toList())) {
			taskParsers.put(notImplemented.name(), new NotImplementedTaskParser(notImplemented));
		}

		CsvFile.registerHandler(KW.CSV.name());
		TsvFile.registerHandler(KW.TSV.name());
		TxtFile.registerHandler(KW.TXT.name());
		XlsxBook.registerHandler(KW.XLSX.name());
	}

	private void cleanupParsing() throws IOException {

		integerTermParser = null;
		stringTermParser = null;
		datetimeTermParser = null;

		tokenizer.close();
		tokenizer = null;
	}

	private void reuseParsing(TaskSetParser parent) {
		this.tokenizer = parent.tokenizer;
		this.integerTermParser = parent.integerTermParser;
		this.stringTermParser = parent.stringTermParser;
		this.datetimeTermParser = parent.datetimeTermParser;
		this.taskParsers = parent.taskParsers;
	}

	interface TaskParser {
		Task parse(Task.Prologue prologue) throws IOException, NamingException;
	}

	private Task parseTask(Task precedingTask)
			throws IOException, InputMismatchException, NoSuchElementException, NamingException {

		String section = KW.TASK.name();
		if (!tokenizer.skipWordIgnoreCase(section)) {
			throw new InputMismatchException(section + " not found where expected");
		}

		// Parse the task name.
		// With an explicit NAME keyword, task name is allowed to be the same as a task type name.
		// With no NAME keyword, task name may be omitted as indicated by the next token
		// being a task type name or a reserved name.

		String name;
		if (tokenizer.skipWordIgnoreCase(KW.NAME.name())) {
			name = tokenizer.nextWordUpperCase();

			if (reservedTaskNames.contains(name)) {
				throw new NameAlreadyBoundException("Cannot use reserved name as a " + KW.TASK.name() + " name: " + name);
			}
		}
		else {
			BacktrackingTokenizerMark nameMark = tokenizer.mark();

			name = tokenizer.nextWordUpperCase();
			if (taskTypeNames.contains(name) || reservedTaskNames.contains(name)) {
				// If token is a task type name or reserved name, task is anonymous.
				name = null;
				tokenizer.reset(nameMark);
			}
		}

		if (name != null) {
			if (tasks.containsKey(name)) {
				throw new NameAlreadyBoundException("Duplicate " + KW.TASK.name() + " name: " + name);
			}
		}

		// Parse the common clauses that can introduce any task.

		Map<Task, Task.Result> predecessors = new HashMap<Task, Task.Result>();
		Expression.Combination combination = null;

		if (tokenizer.skipWordIgnoreCase(KW.AFTER.name())) {
			combination = parsePredecessors(predecessors, precedingTask);
		}

		Expression<Boolean> condition = null;

		if (tokenizer.skipWordIgnoreCase(KW.IF.name())) {
			condition = parseBooleanExpression();
		}

		String taskTypeName = tokenizer.nextWordUpperCase();
		TaskParser parser = taskParsers.get(taskTypeName);

		if (parser == null) {
			throw new InputMismatchException("Invalid " + KW.TASK.name() +" type: " + taskTypeName);
		}

		Task task = parser.parse(new Task.Prologue(name, predecessors, combination, condition));

		nextEnd(section);

		return task;
	}

	private Expression.Combination parsePredecessors(Map<Task, Task.Result> predecessors, Task precedingTask)
			throws InputMismatchException, NoSuchElementException, IOException, NameNotFoundException {

		SimpleEntry<Task, Task.Result> firstPredecessor = parseFirstPredecessor(precedingTask);
		predecessors.put(firstPredecessor.getKey(), firstPredecessor.getValue());

		Expression.Combination combination = null;

		if (tokenizer.skipWordIgnoreCase(KW.AND.name())) {
			combination = Expression.Combination.and;
		}
		else if (tokenizer.skipWordIgnoreCase(KW.OR.name())) {
			combination = Expression.Combination.or;
		}

		if (combination != null) {
			do {
				SimpleEntry<Task, Task.Result> nextPredecessor = parsePredecessor();
				predecessors.put(nextPredecessor.getKey(), nextPredecessor.getValue());
			} while (
					((combination == Expression.Combination.and) && tokenizer.skipWordIgnoreCase(KW.AND.name())) ||
					((combination == Expression.Combination.or) && tokenizer.skipWordIgnoreCase(KW.OR.name())));

			if (tokenizer.skipWordIgnoreCase(KW.AND.name()) || tokenizer.skipWordIgnoreCase(KW.OR.name())) {
				throw new InputMismatchException("Combinations mixing both " + KW.AND.name() + " and " + KW.OR.name() +" are not allowed with " + KW.AFTER.name());
			}
		}

		return combination;
	}

	private SimpleEntry<Task, Task.Result> parseFirstPredecessor(Task precedingTask)
			throws InputMismatchException, NoSuchElementException, IOException, NameNotFoundException {

		if (tokenizer.skipWordIgnoreCase(KW.PREVIOUS.name())) {
			return parsePredecessorResult(precedingTask);
		}

		BacktrackingTokenizerMark nameMark = tokenizer.mark();
		String name = tokenizer.nextWordUpperCase();
		tokenizer.reset(nameMark);

		if (!tasks.containsKey(name)) {
			// If token is not a task name, this is a naked AFTER clause.  Predecessor is the preceding task.
			return parsePredecessorResult(precedingTask);
		}
		else {
			return parsePredecessor();
		}
	}

	private SimpleEntry<Task, Task.Result> parsePredecessor()
			throws InputMismatchException, NoSuchElementException, IOException, NameNotFoundException {

		String name = tokenizer.nextWordUpperCase();
		if (!tasks.containsKey(name)) {
			throw new NameNotFoundException("Predecessor task not defined: " + name);
		}
		Task task = tasks.get(name);

		return parsePredecessorResult(task);
	}

	private SimpleEntry<Task, Task.Result> parsePredecessorResult(Task task)
			throws InputMismatchException, NoSuchElementException, IOException, NameNotFoundException {

		Task.Result result = Task.Result.success;

		if (tokenizer.skipWordIgnoreCase(KW.SUCCEEDS.name())) {
			result = Task.Result.success;
		}
		else if (tokenizer.skipWordIgnoreCase(KW.FAILS.name())) {
			result = Task.Result.failure;
		}
		else if (tokenizer.skipWordIgnoreCase(KW.COMPLETES.name())) {
			result = Task.Result.completed;
		}

		return new SimpleEntry<Task, Task.Result>(task, result);
	}

	protected void nextEnd(String section) throws IOException {

		if (!tokenizer.skipWordIgnoreCase(KW.END.name()) || !tokenizer.skipWordIgnoreCase(section)) {
			throw new InputMismatchException(KW.END.name() + " " + section + " not found where expected");
		}
	}

	class SetVariablesTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			List<SetVariablesTask.Assignment> assignments = new LinkedList<SetVariablesTask.Assignment>();

			do { assignments.add(parseAssignment());
			} while (tokenizer.skipDelimiter(","));

			// Note no check that there actually were any assignments.  Not an error, just pointless.

			return new SetVariablesTask(prologue, assignments);
		}

		private SetVariablesTask.Assignment parseAssignment()
				throws InputMismatchException, NoSuchElementException, IOException {

			VariableBase variable = parseVariableReference();

			tokenizer.nextDelimiter("=");

			ExpressionBase expression = parseExpression(variable.getType());

			return new SetVariablesTask.Assignment(variable, expression);
		}

	}

	class RunTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			DatabaseConnection connection = parseDatabaseConnection(KW.THROUGH.name());

			if (tokenizer.skipWordIgnoreCase(KW.SCRIPT.name())) {
				return parseRunScript(prologue, connection);
			}
			else if (hasSQL(KW.RUN.name())) {
				return parseRunParameterizedStatement(prologue, connection);
			}
			else {
				return parseRunStatement(prologue, connection);
			}
		}

		private Task parseRunScript(Task.Prologue prologue, DatabaseConnection connection) throws IOException {

			Expression<String> source = parseStringExpression();

			return new RunScriptTask(prologue, connection, source);
		}

		private Task parseRunStatement(Task.Prologue prologue, DatabaseConnection connection) throws IOException {

			Expression<String> statement = parseStringExpression();

			return new RunStatementTask(prologue, connection, statement);
		}

		private Task parseRunParameterizedStatement(Task.Prologue prologue, DatabaseConnection connection) throws IOException {

			List<ExpressionBase> expressions = new ArrayList<ExpressionBase>();
			StringBuilder statement = new StringBuilder();
			parseParameterizedStatement(expressions, statement);

			return new RunParameterizedStatementTask(prologue, connection, expressions, statement.toString());
		}
	}

	class UpdateTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue)
						throws InputMismatchException, NoSuchElementException, IOException {

			List<VariableBase> variables = new ArrayList<VariableBase>();
			do {
				variables.add(parseVariableReference());
			} while (tokenizer.skipDelimiter(","));

			DatabaseConnection connection = parseDatabaseConnection(KW.FROM.name());

			if (hasSQL(KW.UPDATE.name())) {
				return parseUpdateFromParameterizedStatement(prologue, variables, connection);
			}
			else {
				return parseUpdateFromStatement(prologue, variables, connection);
			}
		}

		private Task parseUpdateFromStatement(
				Task.Prologue prologue,
				List<VariableBase> variables,
				DatabaseConnection connection) throws IOException {

			Expression<String> statement = parseStringExpression();
			return new UpdateFromStatementTask(prologue, variables, connection, statement);
		}

		private Task parseUpdateFromParameterizedStatement(
				Task.Prologue prologue,
				List<VariableBase> variables,
				DatabaseConnection connection) throws IOException {

			List<ExpressionBase> expressions = new ArrayList<ExpressionBase>();
			StringBuilder statement = new StringBuilder();

			parseParameterizedStatement(expressions, statement);
			return new UpdateFromParameterizedStatementTask(prologue, variables, connection, expressions, statement.toString());
		}
	}

	class CreateTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			PageIdentifierExpression page = parsePageIdentifier(true);

			WriteHeaderExpressions headers = parseWriteHeaders(KW.END.name());

			return new CreateTask(prologue, page, headers);
		}
	}

	class AppendTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			PageIdentifierExpression page = parsePageIdentifier(true);

			DatabaseConnection connection = parseDatabaseConnection(KW.FROM.name());

			if (hasSQL(KW.APPEND.name())) {
				return parseAppendFromParameterizedStatement(prologue, page, connection);
			}
			else {
				return parseAppendFromStatement(prologue, page, connection);
			}
		}

		private Task parseAppendFromStatement(
				Task.Prologue prologue,
				PageIdentifierExpression page,
				DatabaseConnection connection) throws IOException {

			Expression<String> statement = parseStringExpression();

			return new AppendFromStatementTask(prologue, page, connection, statement);
		}

		private Task parseAppendFromParameterizedStatement(
				Task.Prologue prologue,
				PageIdentifierExpression page,
				DatabaseConnection connection) throws IOException {

			List<ExpressionBase> expressions = new ArrayList<ExpressionBase>();
			StringBuilder statement = new StringBuilder();
			parseParameterizedStatement(expressions, statement);

			return new AppendFromParameterizedStatementTask(prologue, page, connection, expressions, statement.toString());
		}
	}

	class WriteTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			PageIdentifierExpression page = parsePageIdentifier(true);

			WriteHeaderExpressions headers = parseWriteHeaders(KW.FROM.name());

			DatabaseConnection connection = parseDatabaseConnection(KW.FROM.name());

			if (tokenizer.skipWordIgnoreCase(KW.TABLE.name())) {
				return parseWriteFromTable(prologue, page, headers, connection);
			}
			else if (hasSQL(KW.WRITE.name())) {
				return parseWriteFromParameterizedStatement(prologue, page, headers, connection);
			}
			else {
				return parseWriteFromStatement(prologue, page, headers, connection);
			}
		}

		private Task parseWriteFromStatement(
				Task.Prologue prologue,
				PageIdentifierExpression page,
				WriteHeaderExpressions headers,
				DatabaseConnection connection) throws IOException {

			Expression<String> statement = parseStringExpression();

			return new WriteFromStatementTask(prologue, page, headers, connection, statement);
		}

		private Task parseWriteFromParameterizedStatement(
				Task.Prologue prologue,
				PageIdentifierExpression page,
				WriteHeaderExpressions headers,
				DatabaseConnection connection) throws IOException {

			List<ExpressionBase> expressions = new ArrayList<ExpressionBase>();
			StringBuilder statement = new StringBuilder();
			parseParameterizedStatement(expressions, statement);

			return new WriteFromParameterizedStatementTask(prologue, page, headers, connection, expressions, statement.toString());
		}

		private Task parseWriteFromTable(
				Task.Prologue prologue,
				PageIdentifierExpression page,
				WriteHeaderExpressions headers,
				DatabaseConnection connection) throws IOException {

			Expression<String> table = parseStringExpression();

			return new WriteFromTableTask(prologue, page, headers, connection, table);
		}
	}

	class OpenTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			PageIdentifierExpression page = parsePageIdentifier(true);

			ReadHeaderExpressions headers = parseReadHeaders(KW.END.name());

			return new OpenTask(prologue, page, headers);
		}
	}

	class LoadTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			PageIdentifierExpression page = parsePageIdentifier(false);

			ColumnExpressions columns = parseColumns();

			DatabaseConnection connection = parseDatabaseConnection(KW.INTO.name());

			if (hasSQL(KW.LOAD.name())) {
				return parseLoadIntoTokenizedStatement(prologue, page, columns, connection);
			}
			else {
				return parseLoadIntoStatement(prologue, page, columns, connection);
			}
		}

		private Task parseLoadIntoStatement(
				Task.Prologue prologue,
				PageIdentifierExpression page,
				ColumnExpressions columns,
				DatabaseConnection connection) throws IOException {

			Expression<String> statement = parseStringExpression();

			return new LoadIntoStatementTask(prologue, page, columns, connection, statement);
		}

		private Task parseLoadIntoTokenizedStatement(
				Task.Prologue prologue,
				PageIdentifierExpression page,
				ColumnExpressions columns,
				DatabaseConnection connection) throws IOException {

			StringBuilder statement = new StringBuilder();
			parseTokenizedStatement(statement);

			return new LoadIntoTokenizedStatementTask(prologue, page, columns, connection, statement.toString());
		}
	}

	class ReadTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			PageIdentifierExpression page = parsePageIdentifier(false);

			ReadHeaderExpressions headers = parseReadHeaders(KW.INTO.name());

			ColumnExpressions columns = parseColumns();

			columns.validate(headers);

			DatabaseConnection connection = parseDatabaseConnection(KW.INTO.name());

			if (tokenizer.skipWordIgnoreCase(KW.TABLE.name())) {
				return parseReadIntoTable(prologue, page, headers, columns, connection);
			}
			else if (hasSQL(KW.READ.name())) {
				return parseReadIntoTokenizedStatement(prologue, page, headers, columns, connection);
			}
			else {
				return parseReadIntoStatement(prologue, page, headers, columns, connection);
			}
		}

		private Task parseReadIntoStatement(
				Task.Prologue prologue,
				PageIdentifierExpression page,
				ReadHeaderExpressions headers,
				ColumnExpressions columns,
				DatabaseConnection connection) throws IOException {

			Expression<String> statement = parseStringExpression();

			return new ReadIntoStatementTask(prologue, page, headers, columns, connection, statement);
		}

		private Task parseReadIntoTokenizedStatement(
				Task.Prologue prologue,
				PageIdentifierExpression page,
				ReadHeaderExpressions headers,
				ColumnExpressions columns,
				DatabaseConnection connection) throws IOException {

			StringBuilder statement = new StringBuilder();
			parseTokenizedStatement(statement);

			return new ReadIntoTokenizedStatementTask(prologue, page, headers, columns, connection, statement.toString());
		}

		private Task parseReadIntoTable(
				Task.Prologue prologue,
				PageIdentifierExpression page,
				ReadHeaderExpressions headers,
				ColumnExpressions columns,
				DatabaseConnection connection) throws IOException {

			if (!headers.exist()) {
				throw new RuntimeException(KW.READ.name() + " " + KW.INTO.name() + " " + KW.TABLE.name() + " requires column headers");
			}

			Expression<String> table = parseStringExpression();

			Expression<String> prefix = null;
			if (tokenizer.skipWordIgnoreCase(KW.PREFIX.name())) {
				tokenizer.skipWordIgnoreCase(KW.WITH.name());
				prefix = parseStringExpression();
			}

			return new ReadIntoTableTask(prologue, page, headers, columns, connection, table, prefix);
		}
	}

	class ZipTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			tokenizer.skipWordIgnoreCase(KW.FROM.name());
			ArrayList<Expression<String>> sources = new ArrayList<Expression<String>>();
			do {
				sources.add(parseStringExpression());
			} while (tokenizer.skipDelimiter(","));

			tokenizer.skipWordIgnoreCase(KW.TO.name());
			Expression<String> target = parseStringExpression();

			return new ZipTask(prologue, sources, target);
		}
	}

	class UnzipTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			tokenizer.skipWordIgnoreCase(KW.FROM.name());
			Expression<String> source = parseStringExpression();

			Expression<String> target = null;
			if (tokenizer.skipWordIgnoreCase(KW.TO.name())) {
				target = parseStringExpression();
			}
			else {
				target = new StringConstant(".");
			}

			return new UnzipTask(prologue, source, target);
		}
	}

	class PutTaskParser extends FtpTaskParser {

		public PutTaskParser() { super(true); }

		@Override
		public Task makeTask(
				Prologue prologue,
				boolean isBinary,
				FtpConnection connection,
				List<Expression<String>> fromNames,
				Expression<String> toName) {
			return new PutTask(prologue, isBinary, connection, fromNames, toName);
		}
	}

	class GetTaskParser extends FtpTaskParser {

		public GetTaskParser() { super(false); }

		@Override
		public Task makeTask(
				Prologue prologue,
				boolean isBinary,
				FtpConnection connection,
				List<Expression<String>> fromNames,
				Expression<String> toName) {
			return new GetTask(prologue, isBinary, connection, fromNames, toName);
		}
	}

	abstract class FtpTaskParser implements TaskParser {

		private boolean putNotGet;

		protected FtpTaskParser(boolean putNotGet) { this.putNotGet = putNotGet; }

		public Task parse(Task.Prologue prologue) throws IOException {

			boolean isBinary = tokenizer.skipWordIgnoreCase(KW.BINARY.name());
			if (!isBinary) {
				tokenizer.skipWordIgnoreCase(KW.ASCII.name());
			}

			FtpConnection connection = null;

			tokenizer.skipWordIgnoreCase(KW.FROM.name());
			if (!putNotGet) {
				connection = parseFtpConnection();
			}

			List<Expression<String>> fromNames = new LinkedList<Expression<String>>();
			do {
				fromNames.add(parseStringExpression());
			} while (tokenizer.skipDelimiter(","));

			tokenizer.skipWordIgnoreCase(KW.TO.name());
			if (putNotGet) {
				connection = parseFtpConnection();
			}

			Expression<String> toName = null;
			if (!hasNextEnd(KW.TASK.name())) {
				toName = parseStringExpression();
			}

			return makeTask(prologue, isBinary, connection, fromNames, toName);
		}

		public abstract Task makeTask(
				Prologue prologue,
				boolean isBinary,
				FtpConnection connection,
				List<Expression<String>> fromNames,
				Expression<String> toName);
	}

	class EmailTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			EmailConnection connection = null;
			if (tokenizer.skipWordIgnoreCase(KW.THROUGH.name())) {
				connection = parseEmailConnection();
				if (connection == null) {
					throw new RuntimeException("Missing connection on " + KW.THROUGH.name() + " in " + KW.EMAIL.name());
				}
			}
			if (!tokenizer.skipWordIgnoreCase(KW.FROM.name())) {
				throw new RuntimeException("Missing " + KW.FROM.name() + " in " + KW.EMAIL.name());
			}
			Expression<String> from = parseStringExpression();

			if (!tokenizer.skipWordIgnoreCase(KW.TO.name())) {
				throw new RuntimeException("Missing " + KW.TO.name() + " in " + KW.EMAIL.name());
			}
			List<Expression<String>> to = new LinkedList<Expression<String>>();
			do {
				to.add(parseStringExpression());
			} while (tokenizer.skipDelimiter(","));

			List<Expression<String>> cc = new LinkedList<Expression<String>>();
			if (tokenizer.skipWordIgnoreCase(KW.CC.name())) {
				do {
					cc.add(parseStringExpression());
				} while (tokenizer.skipDelimiter(","));
			}

			Expression<String> subject = null;
			if (tokenizer.skipWordIgnoreCase(KW.SUBJECT.name())) {
				subject = parseStringExpression();
			}

			Expression<String> body = null;
			if (tokenizer.skipWordIgnoreCase(KW.BODY.name())) {
				body = parseStringExpression();
			}

			List<Expression<String>> attachments = new LinkedList<Expression<String>>();
			if (tokenizer.skipWordIgnoreCase(KW.ATTACH.name()) || tokenizer.skipWordIgnoreCase(KW.ATTACHMENT.name())) {
				do {
					attachments.add(parseStringExpression());
				} while (tokenizer.skipDelimiter(","));
			}

			return new EmailTask(prologue, connection, from, to, cc, subject, body, attachments);
		}
	}

	class DeleteTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			List<Expression<String>> files = new LinkedList<Expression<String>>();
			do {
				files.add(parseStringExpression());
			} while (tokenizer.skipDelimiter(","));
			return new DeleteTask(prologue, files);
		}
	}

	class RenameTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			tokenizer.skipWordIgnoreCase(KW.FROM.name());
			Expression<String> from = parseStringExpression();
			tokenizer.skipWordIgnoreCase(KW.TO.name());
			Expression<String> to = parseStringExpression();
			return new RenameTask(prologue, from, to);
		}
	}

	class CopyTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			tokenizer.skipWordIgnoreCase(KW.FROM.name());
			List<Expression<String>> from = new LinkedList<Expression<String>>();
			do {
				from.add(parseStringExpression());
			} while (tokenizer.skipDelimiter(","));

			tokenizer.skipWordIgnoreCase(KW.TO.name());
			Expression<String> to = parseStringExpression();

			return new CopyTask(prologue, from, to);
		}
	}

	class MakeDirectoryTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			tokenizer.skipWordIgnoreCase(KW.DIRECTORY.name());
			Expression<String> path = parseStringExpression();
			return new MakeDirectoryTask(prologue, path);
		}
	}

	class LogTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			Expression<String> expression = parseStringExpression();
			return new LogTask(prologue, expression);
		}
	}

	class GoTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			Expression<String> expression = null;
			if (!hasNextEnd(KW.TASK.name())) {
				expression = parseStringExpression();
			}
			return new GoTask(prologue, expression);
		}
	}

	class FailTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			Expression<String> expression = null;
			if (!hasNextEnd(KW.TASK.name())) {
				expression = parseStringExpression();
			}
			return new FailTask(prologue, expression);
		}
	}

	class ProcessTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			boolean isSynchronous;
			if (tokenizer.skipWordIgnoreCase(KW.ASYNC.name()) || tokenizer.skipWordIgnoreCase(KW.ASYNCHRONOUSLY.name())) {
				isSynchronous = false;
			}
			else if (tokenizer.skipWordIgnoreCase(KW.SYNC.name()) || tokenizer.skipWordIgnoreCase(KW.SYNCHRONOUSLY.name()) || true) {
				isSynchronous = true;
			}

			Expression<String> name = parseStringExpression();

			List<ExpressionBase> arguments = new LinkedList<ExpressionBase>();
			if (tokenizer.skipWordIgnoreCase(KW.WITH.name()) || !hasNextEnd(KW.TASK.name())) {
				do {
					arguments.add(parseAnyExpression());
				} while (tokenizer.skipDelimiter(","));
			}

			if (isSynchronous) {
				return new SyncProcessTask(prologue, name, arguments);
			}
			else {
				return new AsyncProcessTask(prologue, name, arguments);
			}
		}
	}

	class DoTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException, NamingException {

			Expression<Boolean> whileCondition = null;
			if (tokenizer.skipWordIgnoreCase(KW.WHILE.name())) {
				whileCondition = parseBooleanExpression();
			}

			NestedTaskSet taskSet = NestedTaskSet.parse(thisTaskParser);

			return new DoTask(prologue, whileCondition, taskSet);
		}
	}

	class ForTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue)
				throws InputMismatchException, NoSuchElementException, IOException, NamingException {

			ArrayList<VariableBase> variables = new ArrayList<VariableBase>();
			do {
				variables.add(parseVariableReference());
			} while (tokenizer.skipDelimiter(","));

			tokenizer.skipWordIgnoreCase(KW.FROM.name());

			if (hasFileType(false)) {
				return parseForReadFile(prologue, variables);
			}
			else if (tokenizer.skipWordIgnoreCase(KW.VALUES.name())) {
				return parseForValues(prologue, variables);
			}
			else if (tokenizer.skipWordIgnoreCase(KW.FILES.name())) {
				return parseForFiles(prologue, variables);
			}
			else {
				DatabaseConnection connection = parseDatabaseConnection(null);

				if (hasSQL(KW.FOR.name())) {
					return parseForParameterizedStatement(prologue, variables, connection);
				}
				else {
					return parseForStatement(prologue, variables, connection);
				}
			}
		}

		private Task parseForStatement(
				Task.Prologue prologue,
				List<VariableBase> variables,
				DatabaseConnection connection) throws IOException, NamingException {

			Expression<String> statement = parseStringExpression();

			NestedTaskSet taskSet = NestedTaskSet.parse(thisTaskParser);

			return new ForStatementTask(prologue, variables, connection, statement, taskSet);
		}

		private Task parseForParameterizedStatement(
				Task.Prologue prologue,
				List<VariableBase> variables,
				DatabaseConnection connection) throws IOException, NamingException {

			List<ExpressionBase> expressions = new ArrayList<ExpressionBase>();
			StringBuilder statement = new StringBuilder();
			parseParameterizedStatement(expressions, statement);

			NestedTaskSet taskSet = NestedTaskSet.parse(thisTaskParser);

			return new ForParameterizedStatementTask(prologue, variables, connection, expressions, statement.toString(), taskSet);
		}

		private Task parseForReadFile(
				Task.Prologue prologue,
				ArrayList<VariableBase> variables) throws IOException, NamingException {

			PageIdentifierExpression page = parsePageIdentifier(false);

			ReadHeaderExpressions headers = parseReadHeaders(KW.TASK.name());

			ColumnExpressions columns = parseColumns();

			columns.validate(headers);

			NestedTaskSet taskSet = NestedTaskSet.parse(thisTaskParser);

			return new ForReadTask(prologue, variables, page, headers, columns, taskSet);
		}

		private Task parseForValues(
				Task.Prologue prologue,
				ArrayList<VariableBase> variables) throws IOException, NamingException {

			List<ExpressionBase[]> values = new LinkedList<ExpressionBase[]>();
			do {
				values.add(parseExpressionList(variables));
			} while (tokenizer.skipDelimiter(","));

			NestedTaskSet taskSet = NestedTaskSet.parse(thisTaskParser);

			return new ForValuesTask(prologue, variables, values, taskSet);
		}

		private ExpressionBase[] parseExpressionList(ArrayList<VariableBase> variables)
				throws InputMismatchException, NoSuchElementException, IOException {

			ExpressionBase[] expressionList = new ExpressionBase[variables.size()];
			tokenizer.nextDelimiter("(");

			int index = 0;
			for (VariableBase variable : variables) {
				expressionList[index] = parseExpression(variable.getType());
				if (++index < variables.size()) {
					tokenizer.nextDelimiter(",");
				}
			}

			tokenizer.nextDelimiter(")");
			return expressionList;
		}

		@SuppressWarnings("unchecked")
		private Task parseForFiles(
				Task.Prologue prologue,
				ArrayList<VariableBase> variables) throws IOException, NamingException {

			if ((variables.size() != 1) || (variables.get(0).getType() != VariableType.VARCHAR)) {
				throw new RuntimeException(KW.FOR.name() + " " + KW.FILES.name() + " must have a single VARCHAR variable in the list");
			}

			Expression<String> filename = parseStringExpression();

			NestedTaskSet taskSet = NestedTaskSet.parse(thisTaskParser);

			return new ForFilesTask(prologue, (Variable<String>)variables.get(0), filename, taskSet);
		}
	}

	class OnTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue)
				throws InputMismatchException, NoSuchElementException, IOException, NamingException {

			if (tokenizer.skipWordIgnoreCase(KW.SCHEDULE.name())) {
				return parseOnSchedule(prologue);
			}
			else {
				return parseOn(prologue);
			}
		}

		private Task parseOnSchedule(Task.Prologue prologue) throws IOException, NamingException {

			Expression<String> schedule = parseStringExpression();

			NestedTaskSet taskSet = NestedTaskSet.parse(thisTaskParser);

			return new OnScheduleTask(prologue, schedule, taskSet, connections);
		}

		private Task parseOn(Task.Prologue prologue) throws IOException, NamingException {

			ScheduleSet schedules = ScheduleSet.parse(tokenizer);

			NestedTaskSet taskSet = NestedTaskSet.parse(thisTaskParser);

			return new OnTask(prologue, schedules, taskSet, connections);
		}
	}

	class WaitforTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue)
				throws InputMismatchException, NoSuchElementException, IOException, NamingException {

			if (tokenizer.skipWordIgnoreCase(KW.DELAY.name())) {
				return parseWaitforDelay(prologue);
			}
			if (tokenizer.skipWordIgnoreCase(KW.TIME.name())) {
				return parseWaitforTime(prologue);
			}
			else {
				throw new InputMismatchException("Invalid argument in " + KW.WAITFOR.name() + " " + KW.TASK.name());
			}
		}

		private Task parseWaitforDelay(Task.Prologue prologue) throws IOException, NamingException {

			Expression<String> delay = parseStringExpression();

			return new WaitforDelayTask(prologue, delay, connections);
		}

		private Task parseWaitforTime(Task.Prologue prologue) throws IOException, NamingException {

			Expression<String> time = parseStringExpression();

			return new WaitforTimeTask(prologue, time, connections);
		}
	}

	class ConnectTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue)
				throws InputMismatchException, NoSuchElementException, IOException, NamingException {

			Connection connection = parseConnection();
			boolean inherit = false;
			Expression<String> properties = null;

			tokenizer.skipWordIgnoreCase(KW.TO.name());
			if (tokenizer.skipWordIgnoreCase(KW.DEFAULT.name())) {
				inherit = true;
				if (tokenizer.skipWordIgnoreCase(KW.WITH.name())) {
					properties = parseStringExpression();
				}
			}
			else {
				properties = parseStringExpression();
			}

			return new ConnectTask(prologue, connection, inherit, properties);
		}
	}

	class NotImplementedTaskParser implements TaskParser {

		private String taskTypeName;

		public NotImplementedTaskParser(KW taskTypeKeyword) {
			taskTypeName = taskTypeKeyword.name();
		}

		public Task parse(Task.Prologue prologue)
				throws NoSuchElementException {

			throw new NoSuchElementException("Use NAME keyword before a name reserved for a future task type: " + taskTypeName);
		}
	}

	private boolean hasSQL(String taskTypeName) throws IOException {

		if (tokenizer.skipWordIgnoreCase(KW.STATEMENT.name())) {
			return false;
		}
		else if (tokenizer.skipWordIgnoreCase(KW.SQL.name())) {
			return true;
		}
		else {
			throw new InputMismatchException("Invalid data source in " + taskTypeName + " " + KW.TASK.name());
		}
	}

	private VariableBase parseVariableReference() throws InputMismatchException, NoSuchElementException, IOException {

		String name = tokenizer.nextWordUpperCase();
		VariableBase variable = variables.get(name);
		if (variable == null) {
			throw new NoSuchElementException("Variable name not declared: " + name);
		}
		return variable;
	}

	private PageIdentifierExpression parsePageIdentifier(boolean writeNotRead) throws IOException {

		String typeName = tokenizer.nextWordUpperCase();
		FileHandler handler = FileHandler.get(typeName, writeNotRead);
		Expression<String> filePath = parseStringExpression();
		if (!handler.getHasSheets()) {
			return new FileIdentifierExpression(handler, filePath);
		}
		else {
			Expression<String> sheetName = parseStringExpression();
			tokenizer.skipWordIgnoreCase(KW.SHEET.name());
			return new SheetIdentifierExpression(handler, filePath, sheetName);
		}
	}

	private WriteHeaderExpressions parseWriteHeaders(String wordAfterHeaders) throws IOException {

		tokenizer.skipWordIgnoreCase(KW.WITH.name());
		boolean noHeaders = tokenizer.skipWordIgnoreCase(KW.NO.name());
		tokenizer.skipWordIgnoreCase(KW.HEADERS.name());

		ArrayList<Expression<String>> captions = new ArrayList<Expression<String>>();
		if (!noHeaders && !tokenizer.hasNextWordIgnoreCase(wordAfterHeaders)) {
			do {
				captions.add(parseStringExpression());
			} while (tokenizer.skipDelimiter(","));
		}

		boolean headersFromMetadata = !noHeaders && (captions.size() == 0);

		return new WriteHeaderExpressions(!noHeaders, headersFromMetadata, captions);
	}

	private ReadHeaderExpressions parseReadHeaders(String wordAfterHeaders) throws IOException {

		boolean noHeaders = false;
		boolean ignoreHeaders = tokenizer.skipWordIgnoreCase(KW.IGNORE.name());
		if (!ignoreHeaders) {
			tokenizer.skipWordIgnoreCase(KW.WITH.name());
			noHeaders = tokenizer.skipWordIgnoreCase(KW.NO.name());
		}
		tokenizer.skipWordIgnoreCase(KW.HEADERS.name());

		boolean validateHeaders = !noHeaders && !ignoreHeaders &&
				!tokenizer.hasNextWordIgnoreCase(KW.COLUMNS.name()) && !tokenizer.hasNextWordIgnoreCase(wordAfterHeaders);

		ArrayList<Expression<String>> captions = new ArrayList<Expression<String>>();
		if (validateHeaders) {
			do {
				captions.add(parseStringExpression());
			} while (tokenizer.skipDelimiter(","));

			validateHeaders = (0 < captions.size());
		}

		boolean headersToMetadata = !noHeaders && !ignoreHeaders && !validateHeaders;

		return new ReadHeaderExpressions(!noHeaders, validateHeaders, headersToMetadata, captions);
	}

	private ColumnExpressions parseColumns() throws IOException {

		ArrayList<ExpressionBase> columns = new ArrayList<ExpressionBase>();
		if (tokenizer.skipWordIgnoreCase(KW.COLUMNS.name())) {
			do {
				columns.add(parseAnyExpression());
			} while (tokenizer.skipDelimiter(","));
		}
		return new ColumnExpressions(columns);
	}

	private void parseParameterizedStatement(
			List<ExpressionBase> expressions,
			StringBuilder statement) throws IOException {

		if (tokenizer.skipWordIgnoreCase(KW.SUBSTITUTING.name())) {
			do {
				expressions.add(parseAnyExpression());
			} while (tokenizer.skipDelimiter(","));
			tokenizer.skipWordIgnoreCase(KW.INTO.name());
		}

		parseTokenizedStatement(statement);
	}

	private void parseTokenizedStatement(StringBuilder statement) throws IOException {

		BacktrackingTokenizerMark mark = tokenizer.mark();

		while (!hasNextBeginOrEnd(KW.TASK.name())) {
			Token nextToken = tokenizer.nextToken();
			statement.append(nextToken.render());

			if (!tokenizer.hasNext()) {
				tokenizer.reset(mark);
				throw new NoSuchElementException("SQL not terminated properly");
			}
		}
	}

	private Connection parseConnection() throws InputMismatchException, NoSuchElementException, IOException {

		String name = tokenizer.nextWordUpperCase();
		Connection connection = connections.get(name);
		if (connection == null) {
			throw new NoSuchElementException("Connection name not declared: " + name);
		}
		return connection;
	}

	private DatabaseConnection parseDatabaseConnection(String introWord) throws InputMismatchException, NoSuchElementException, IOException {

		if (introWord != null) {
			tokenizer.skipWordIgnoreCase(introWord);
		}
		return (DatabaseConnection)parseOptionalConnection(KW.DATABASE.name(), DatabaseConnection.class.getName());
	}

	private FtpConnection parseFtpConnection() throws InputMismatchException, NoSuchElementException, IOException {
		return (FtpConnection)parseOptionalConnection(KW.FTP.name(), FtpConnection.class.getName());
	}

	private EmailConnection parseEmailConnection() throws InputMismatchException, NoSuchElementException, IOException {
		return (EmailConnection)parseOptionalConnection(KW.EMAIL.name(), EmailConnection.class.getName());
	}

	private Connection parseOptionalConnection(String typeName, String className) throws InputMismatchException, NoSuchElementException, IOException {

		Connection connection = null;
		if (tokenizer.hasNextWord()) {
			if (tokenizer.skipWordIgnoreCase(KW.DEFAULT.name())) {
				tokenizer.skipWordIgnoreCase(KW.CONNECTION.name());
			}
			else {
				if (tokenizer.skipWordIgnoreCase(KW.CONNECTION.name())) {
					// Connection name must be present.
					connection = parseConnection();
				}
				else if (tokenizer.hasNextWord()) {
					// Connection name may or may not be present.
					BacktrackingTokenizerMark mark = tokenizer.mark();

					String name = tokenizer.nextWordUpperCase();
					connection = connections.get(name);

					if (connection == null) {
						tokenizer.reset(mark);
					}
				}

				if (connection != null) {
					if (connection.getClass().getName() != className) {
						throw new InputMismatchException("Connection is not " + typeName + " type");
					}
				}
			}
		}
		return connection;
	}

	/**
	 * @return true if the next token is the name of a file type, otherwise false.
	 * The tokenizer does not advance past any input.
	 * @param writeNotRead is true if the file will be written, false if it will be read
	 * @throws UnsupportedOperationException if the file handler does not support the specified write or read operation.
	 * @throws IOException
	 */
	private boolean hasFileType(boolean writeNotRead) throws IOException {

		BacktrackingTokenizerMark mark = tokenizer.mark();
		try {
			String name = tokenizer.nextWordUpperCase();
			FileHandler.get(name, writeNotRead);
		}
		catch (NoSuchElementException ex) {	// includes subclass InputMismatchException
			return false;
		}
		finally {
			tokenizer.reset(mark);
		}

		return true;
	}

	private boolean hasNextEnd(String section) throws IOException {

		if (!tokenizer.hasNextWordIgnoreCase(KW.END.name())) {
			return false;
		}

		BacktrackingTokenizerMark mark = tokenizer.mark();
		tokenizer.nextToken();

		boolean result = tokenizer.hasNextWordIgnoreCase(section);

		tokenizer.reset(mark);
		return result;
	}

	private boolean hasNextBeginOrEnd(String section) throws IOException {

		if (tokenizer.hasNextWordIgnoreCase(KW.END.name())) {
			return hasNextEnd(section);
		}
		else if (tokenizer.hasNextWordIgnoreCase(section)) {
			if (section == KW.TASK.name()) {
				BacktrackingTokenizerMark mark = tokenizer.mark();
				tokenizer.nextToken();

				boolean result = tokenizer.hasNextWord();

				tokenizer.reset(mark);
				return result;
			}
			else {
				return true;
			}
		}
		else {
			return false;
		}
	}

	private ExpressionBase parseExpression(VariableType type)
			throws InputMismatchException, NoSuchElementException, IOException {

		ExpressionBase expression = null;
		if (type == VariableType.INTEGER) {
			expression = parseIntegerExpression();
		}
		else if (type == VariableType.VARCHAR) {
			expression = parseStringExpression();
		}
		else if (type == VariableType.DATETIME) {
			expression = parseDatetimeFromCompatibleExpression();
		}
		else {
			throw new InputMismatchException("Internal error - variable type not recognized");
		}

		return expression;
	}

	private ExpressionBase parseAnyExpression() {

		BacktrackingTokenizerMark mark = tokenizer.mark();

		try { return parseIntegerExpression(); }
		catch (Exception ex) { tokenizer.reset(mark); }

		try { return parseStringExpression(); }
		catch (Exception ex) { tokenizer.reset(mark); }

		try { return parseDatetimeExpression(); }
		catch (Exception ex) { tokenizer.reset(mark); }

		throw new InputMismatchException("Invalid expression");
	}

	@SuppressWarnings("unchecked")
	private Expression<LocalDateTime> parseDatetimeFromCompatibleExpression() {

		ExpressionBase expression = parseDatetimeCompatibleExpression();

		if (expression.getType() == VariableType.DATETIME) {
			return (Expression<LocalDateTime>)expression;
		}
		else if (expression.getType() == VariableType.VARCHAR) {
			return new DatetimeFromString((Expression<String>)expression);
		}
		else {
			throw new InputMismatchException("Internal error - unsupported " + KW.DATETIME.name() + " compatible expression");
		}
	}

	private ExpressionBase parseDatetimeCompatibleExpression() {

		BacktrackingTokenizerMark mark = tokenizer.mark();

		try { return parseStringExpression(); }
		catch (Exception ex) { tokenizer.reset(mark); }

		try { return parseDatetimeExpression(); }
		catch (Exception ex) { tokenizer.reset(mark); }

		throw new InputMismatchException("Invalid expression");
	}

	private ExpressionBase parseFormatableExpression() {

		BacktrackingTokenizerMark mark = tokenizer.mark();

		try { return parseIntegerExpression(); }
		catch (Exception ex) { tokenizer.reset(mark); }

		try { return parseDatetimeExpression(); }
		catch (Exception ex) { tokenizer.reset(mark); }

		throw new InputMismatchException("Invalid expression for use with " + KW.FORMAT.name());
	}

	private Expression<Boolean> parseBooleanExpression() throws IOException {

		// See https://en.wikipedia.org/wiki/Recursive_descent_parser

		Expression<Boolean> left = parseBooleanAnd();
		while (tokenizer.skipWordIgnoreCase(KW.OR.name())) {
			Expression<Boolean> right = parseBooleanAnd();
			left = new BooleanBinary(left, right, Expression.Combination.or);
		}
		return left;
	}

	private Expression<Boolean> parseBooleanAnd() throws IOException {

		Expression<Boolean> left = parseBooleanInvert();
		while (tokenizer.skipWordIgnoreCase(KW.AND.name())) {
			Expression<Boolean> right = parseBooleanInvert();
			left = new BooleanBinary(left, right, Expression.Combination.and);
		}
		return left;
	}

	private Expression<Boolean> parseBooleanInvert() throws IOException {

		boolean invert = false;
		while (tokenizer.skipWordIgnoreCase(KW.NOT.name())) {
			invert = !invert;
		}

		Expression<Boolean> term = parseBooleanTerm();

		return invert ? new BooleanNot(term) : term;
	}

	@SuppressWarnings("unchecked")
	private Expression<Boolean> parseBooleanTerm() throws IOException {

		if (tokenizer.skipDelimiter("(")) {
			Expression<Boolean> parenthesized = parseBooleanExpression();
			tokenizer.nextDelimiter(")");
			return parenthesized;
		}

		ExpressionBase left = parseAnyExpression();

		Expression.Comparison comparison =
				tokenizer.skipDelimiter("<") ? Expression.Comparison.lt :
				tokenizer.skipDelimiter("<=") ? Expression.Comparison.le :
				tokenizer.skipDelimiter("=") ? Expression.Comparison.eq :
				tokenizer.skipDelimiter("<>") ? Expression.Comparison.ne :
				tokenizer.skipDelimiter(">=") ? Expression.Comparison.ge :
				tokenizer.skipDelimiter(">") ? Expression.Comparison.gt :
				null;

		if (comparison == null) {
			if (tokenizer.skipWordIgnoreCase(KW.IS.name())) {

				boolean invert = tokenizer.skipWordIgnoreCase(KW.NOT.name());
				if (!tokenizer.skipWordIgnoreCase(KW.NULL.name())) {
					throw new InputMismatchException("NULL not found after IS " + (invert ? KW.NOT.name() + " " : "") + "as expected");
				}

				Expression<Boolean> term = new NullTest(left);
				return invert ? new BooleanNot(term) : term;
			}
			else {
				throw new InputMismatchException("Comparison operator not found where expected");
			}
		}
		else if (left.getType() == VariableType.INTEGER) {
			Expression<Integer> rightInteger = parseIntegerExpression();
			return new BooleanFromIntegers((Expression<Integer>)left, rightInteger, comparison);
		}
		else if (left.getType() == VariableType.DATETIME) {
			Expression<LocalDateTime> rightDatetime = parseDatetimeFromCompatibleExpression();
			return new BooleanFromDatetimes((Expression<LocalDateTime>)left, rightDatetime, comparison);
		}
		else if (left.getType() == VariableType.VARCHAR) {
			ExpressionBase right = parseDatetimeCompatibleExpression();

			if (right.getType() == VariableType.VARCHAR) {
				return new BooleanFromStrings((Expression<String>)left, (Expression<String>)right, comparison);
			}
			else if (right.getType() == VariableType.DATETIME) {
				Expression<LocalDateTime> leftDatetime = new DatetimeFromString((Expression<String>)left);
				return new BooleanFromDatetimes(leftDatetime, (Expression<LocalDateTime>)right, comparison);
			}
			else {
				throw new InputMismatchException("Internal error - unsupported " + KW.DATETIME.name() + " compatible comparison");
			}
		}
		else {
			throw new InputMismatchException("Internal error - expression type not recognized in comparison");
		}
	}

	Expression<Integer> parseIntegerExpression() throws IOException {

		Expression<Integer> left = parseIntegerAddend();
		IntegerBinary.Operator operator = null;
		while ((operator =
				tokenizer.skipDelimiter("+") ? IntegerBinary.Operator.add :
				tokenizer.skipDelimiter("-") ? IntegerBinary.Operator.subtract :
				null) != null ) {

			Expression<Integer> right = parseIntegerAddend();
			left = new IntegerBinary(left, right, operator);
		}
		return left;
	}

	private Expression<Integer> parseIntegerAddend() throws IOException {

		Expression<Integer> left = parseIntegerFactor();
		IntegerBinary.Operator operator = null;
		while ((operator =
				tokenizer.skipDelimiter("*") ? IntegerBinary.Operator.multiply :
				tokenizer.skipDelimiter("/") ? IntegerBinary.Operator.divide :
				tokenizer.skipDelimiter("%") ? IntegerBinary.Operator.modulo :
				null) != null ) {

			Expression<Integer> right = parseIntegerFactor();
			left = new IntegerBinary(left, right, operator);
		}
		return left;
	}

	private Expression<Integer> parseIntegerFactor() throws IOException {

		boolean negate = false;
		IntegerBinary.Operator operator = null;
		while ((operator =
				tokenizer.skipDelimiter("+") ? IntegerBinary.Operator.add :
				tokenizer.skipDelimiter("-") ? IntegerBinary.Operator.subtract :
				null) != null ) {
			if (operator == IntegerBinary.Operator.subtract) {
				negate = !negate;
			}
		}

		Expression<Integer> term = parseIntegerTerm();

		return negate ? new IntegerBinary(new IntegerConstant(0), term, IntegerBinary.Operator.subtract) : term;
	}

	private Expression<Integer> parseIntegerTerm()
			throws InputMismatchException, NoSuchElementException, IOException {

		if (tokenizer.hasNextInt()) {
			return new IntegerConstant(tokenizer.nextInt());
		}
		else if (tokenizer.skipWordIgnoreCase(KW.NULL.name())) {
			return new IntegerConstant(null);
		}
		else if (hasNextFunction()) {
			String name = tokenizer.nextWordUpperCase();

			if (name.equals(KW.ISNULL.name())) {
				return integerTermParser.parseIsNull(VariableType.INTEGER);
			}
			else if (name.equals(KW.IIF.name())) {
				return integerTermParser.parseIf();
			}
			else if (name.equals(KW.DATEPART.name())) {

				tokenizer.skipDelimiter("(");
				ChronoField field =
						tokenizer.skipWordIgnoreCase(KW.YEAR.name()) ? ChronoField.YEAR :
						tokenizer.skipWordIgnoreCase(KW.MONTH.name()) ? ChronoField.MONTH_OF_YEAR :
						tokenizer.skipWordIgnoreCase(KW.DAY.name()) ? ChronoField.DAY_OF_MONTH :
						tokenizer.skipWordIgnoreCase(KW.WEEKDAY.name()) ? ChronoField.DAY_OF_WEEK :
						tokenizer.skipWordIgnoreCase(KW.HOUR.name()) ? ChronoField.HOUR_OF_DAY :
						tokenizer.skipWordIgnoreCase(KW.MINUTE.name()) ? ChronoField.MINUTE_OF_HOUR :
						tokenizer.skipWordIgnoreCase(KW.SECOND.name()) ? ChronoField.SECOND_OF_MINUTE :
						null;
				if (field == null) {
					throw new InputMismatchException("Unrecognized date part name for " + KW.DATEPART.name());
				}

				tokenizer.skipDelimiter(",");
				Expression<LocalDateTime> datetime = parseDatetimeExpression();
				tokenizer.skipDelimiter(")");

				return new IntegerFromDatetime(datetime, field);
			}
			else {
				throw new InputMismatchException("Unrecognized " + KW.INTEGER.name() + " function: " + name);
			}
		}
		else if (hasNextVariable()) {
			return integerTermParser.parseReference(VariableType.INTEGER);
		}
		else if (tokenizer.skipDelimiter("(")) {
			return integerTermParser.parseParenthesized();
		}
		else {
			throw new InputMismatchException("Invalid " + KW.INTEGER.name() + " expression term: " + tokenizer.nextToken().render());
		}
	}

	Expression<String> parseStringExpression() throws IOException {

		Expression<String> left = parseStringTerm();
		while (tokenizer.skipDelimiter("+")) {
			Expression<String> right = parseStringTerm();
			left = new StringConcat(left, right);
		}
		return left;
	}

	@SuppressWarnings("unchecked")
	private Expression<String> parseStringTerm()
			throws InputMismatchException, NoSuchElementException, IOException {

		if (tokenizer.hasNextQuoted()) {
			return new StringConstant(tokenizer.nextQuoted().getBody());
		}
		else if (tokenizer.skipWordIgnoreCase(KW.NULL.name())) {
			return new StringConstant(null);
		}
		else if (hasNextFunction()) {
			String name = tokenizer.nextWordUpperCase();

			if (name.equals(KW.ISNULL.name())) {
				return stringTermParser.parseIsNull(VariableType.VARCHAR);
			}
			else if (name.equals(KW.IIF.name())) {
				return stringTermParser.parseIf();
			}
			else if (name.equals(KW.FORMAT.name())) {

				tokenizer.skipDelimiter("(");
				ExpressionBase source = parseFormatableExpression();
				tokenizer.skipDelimiter(",");
				Expression<String> format = parseStringExpression();
				tokenizer.skipDelimiter(")");

				if (source.getType() == VariableType.INTEGER) {
					return new StringFromInteger((Expression<Integer>)source, format);
				}
				else if (source.getType() == VariableType.DATETIME) {
					return new StringFromDatetime((Expression<LocalDateTime>)source, format);
				}
				else {
					throw new InputMismatchException("Internal error - unsupported argument type for " + KW.FORMAT.name());
				}
			}
			else {
				throw new InputMismatchException("Unrecognized " + KW.VARCHAR.name() + " function: " + name);
			}
		}
		else if (hasNextVariable()) {
			return stringTermParser.parseReference(VariableType.VARCHAR);
		}
		else if (tokenizer.skipDelimiter("(")) {
			return stringTermParser.parseParenthesized();
		}
		else {
			throw new InputMismatchException("Invalid " + KW.VARCHAR.name() + " expression term: " + tokenizer.nextToken().render());
		}
	}

	Expression<LocalDateTime> parseDatetimeExpression()
			throws InputMismatchException, NoSuchElementException, IOException {

		if (tokenizer.skipWordIgnoreCase(KW.NULL.name())) {
			return new DatetimeConstant(null);
		}
		else if (hasNextFunction()) {
			BacktrackingTokenizerMark mark = tokenizer.mark();
			String name = tokenizer.nextWordUpperCase();

			if (name.equals(KW.ISNULL.name())) {
				return datetimeTermParser.parseIsNull(VariableType.DATETIME);
			}
			else if (name.equals(KW.IIF.name())) {
				return datetimeTermParser.parseIf();
			}
			else if (name.equals(KW.GETDATE.name())) {
				tokenizer.skipDelimiter("(");
				tokenizer.skipDelimiter(")");
				return new DatetimeNullary();
			}
			else if (name.equals(KW.DATEADD.name())) {

				tokenizer.skipDelimiter("(");
				ChronoUnit unit =
						tokenizer.skipWordIgnoreCase(KW.YEAR.name()) ? ChronoUnit.YEARS :
						tokenizer.skipWordIgnoreCase(KW.MONTH.name()) ? ChronoUnit.MONTHS :
						tokenizer.skipWordIgnoreCase(KW.DAY.name()) ? ChronoUnit.DAYS :
						tokenizer.skipWordIgnoreCase(KW.HOUR.name()) ? ChronoUnit.HOURS :
						tokenizer.skipWordIgnoreCase(KW.MINUTE.name()) ? ChronoUnit.MINUTES :
						tokenizer.skipWordIgnoreCase(KW.SECOND.name()) ? ChronoUnit.SECONDS :
						null;
				if (unit == null) {
					throw new InputMismatchException("Unrecognized date part name for " + KW.DATEADD.name());
				}

				tokenizer.skipDelimiter(",");
				Expression<Integer> increment = parseIntegerExpression();
				tokenizer.skipDelimiter(",");
				Expression<LocalDateTime> datetime = parseDatetimeExpression();
				tokenizer.skipDelimiter(")");

				return new DatetimeAdd(datetime, unit, increment);
			}
			else if (name.equals(KW.DATEFROMPARTS.name())) {

				tokenizer.skipDelimiter("(");
				Expression<Integer> year = parseIntegerExpression();
				tokenizer.skipDelimiter(",");
				Expression<Integer> month = parseIntegerExpression();
				tokenizer.skipDelimiter(",");
				Expression<Integer> day = parseIntegerExpression();
				tokenizer.skipDelimiter(")");

				return new DateFromParts(year, month, day);
			}
			else if (name.equals(KW.DATETIMEFROMPARTS.name())) {

				tokenizer.skipDelimiter("(");
				Expression<Integer> year = parseIntegerExpression();
				tokenizer.skipDelimiter(",");
				Expression<Integer> month = parseIntegerExpression();
				tokenizer.skipDelimiter(",");
				Expression<Integer> day = parseIntegerExpression();
				Expression<Integer> hour = parseIntegerExpression();
				tokenizer.skipDelimiter(",");
				Expression<Integer> minute = parseIntegerExpression();
				tokenizer.skipDelimiter(",");
				Expression<Integer> seconds = parseIntegerExpression();
				tokenizer.skipDelimiter(",");
				Expression<Integer> milliseconds = parseIntegerExpression();
				tokenizer.skipDelimiter(")");

				return new DatetimeFromParts(year, month, day, hour, minute, seconds, milliseconds);
			}
			else {
				// Could be a string function

				tokenizer.reset(mark);

				try {
					Expression<String> expression = parseStringExpression();
					return new DatetimeFromString(expression);
				}
				catch (Exception ex) { tokenizer.reset(mark); }

				throw new InputMismatchException("Unrecognized " + KW.DATETIME.name() + " function: " + name);
			}
		}
		else if (tokenizer.skipDelimiter("(")) {
			return datetimeTermParser.parseParenthesized();
		}
		else {
			// Could be a string expression or a single datetime variable reference

			BacktrackingTokenizerMark mark = tokenizer.mark();

			try {
				Expression<String> expression = parseStringExpression();
				return new DatetimeFromString(expression);
			}
			catch (Exception ex) { tokenizer.reset(mark); }

			if (hasNextVariable()) {
				return datetimeTermParser.parseReference(VariableType.DATETIME);
			}
			else {
				throw new InputMismatchException("Invalid " + KW.DATETIME.name() + " expression term: " + tokenizer.nextToken().render());
			}
		}
	}

	private boolean hasNextFunction() throws IOException {

		boolean isFunction = false;
		BacktrackingTokenizerMark mark = tokenizer.mark();

		Token token = tokenizer.nextToken();
		if (token instanceof Word) {
			final Delimiter openParen = new Delimiter(false, "(");
			token = tokenizer.nextToken();
			isFunction = !token.hasLeadingWhitespace() && token.equals(openParen);
		}

		tokenizer.reset(mark);
		return isFunction;
	}

	private boolean hasNextVariable() throws IOException {

		if (!tokenizer.hasNextWord()) {
			return false;
		}

		BacktrackingTokenizerMark mark = tokenizer.mark();
		String name = tokenizer.nextWordUpperCase();

		boolean result = variables.containsKey(name);

		tokenizer.reset(mark);
		return result;
	}

	abstract class TermParser<Type> {

		public abstract Expression<Type> parseExpression() throws IOException;

		public Expression<Type> parseIsNull(VariableType type) throws IOException {

			tokenizer.skipDelimiter("(");
			Expression<Type> left = parseExpression();
			tokenizer.skipDelimiter(",");
			Expression<Type> right = parseExpression();
			tokenizer.skipDelimiter(")");

			return new NullReplace<Type>(left, right);
		}

		public Expression<Type> parseIf()
				throws InputMismatchException, NoSuchElementException, IOException {

			tokenizer.skipDelimiter("(");
			Expression<Boolean> condition = parseBooleanExpression();
			tokenizer.skipDelimiter(",");
			Expression<Type> left = parseExpression();
			tokenizer.skipDelimiter(",");
			Expression<Type> right = parseExpression();
			tokenizer.skipDelimiter(")");

			return new IfExpression<Type>(condition, left, right);
		}

		public Expression<Type> parseReference(VariableType type)
				throws InputMismatchException, NoSuchElementException, IOException {

			Variable<Type> variable = parseVariable(type);
			return new Reference<Type>((Variable<Type>)variable);
		}

		public Expression<Type> parseParenthesized() throws IOException {

			Expression<Type> parenthesized = parseExpression();
			tokenizer.nextDelimiter(")");
			return parenthesized;
		}

		@SuppressWarnings("unchecked")
		private Variable<Type> parseVariable(VariableType type)
				throws InputMismatchException, NoSuchElementException, IOException {

			String name = tokenizer.nextWordUpperCase();

			VariableBase variable = variables.get(name);
			if (variable == null) {
				throw new NoSuchElementException("Variable name not declared: " + name);
			}
			if (variable.getType() != type) {
				throw new InputMismatchException("Variable has wrong type in " + type.getName() + " expression: " + name);
			}

			return (Variable<Type>)variable;
		}
	}
}
