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
import com.hauldata.dbpa.variable.*;
import com.hauldata.util.schedule.ScheduleSet;
import com.hauldata.util.tokenizer.*;


/**
 * Base class parser for a set of tasks at either outer process level or inner nested level
 */
abstract class TaskSetParser {
	
	TaskSetParser thisTaskParser;

	/**
	 * Reserved words
	 */
	public enum RW {

		TASK,
		END,

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
		EMAIL,

		// Task predecessors and conditions

		AFTER,
		SUCCEEDS,
		FAILS,
		COMPLETES,
		AND,
		OR,
		IF,
		NOT,

		// Task types

		SET,
		UPDATE,
		FROM,
		RUN,
		SCRIPT,
		CREATE,
		APPEND,
		WRITE,
		OPEN,
		LOAD,
		READ,
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
		TABLE,
		PREFIX,
		ZIP,
		UNZIP,
		PUT,
		BINARY,
		ASCII,
//		EMAIL,
		THROUGH,
		TO,
		CC,
		SUBJECT,
		BODY,
		ATTACH,
		DELETE,
		RENAME,
		COPY,
		MAKE,
		DIRECTORY,
		LOG,
		GO,
		FAIL,
		PROCESS,
		SYNC,
		SYNCHRONOUSLY,
		ASYNC,
		ASYNCHRONOUSLY,
		DO,
		WHILE,
		FOR,
		VALUES,
		ON,
		SCHEDULE,
		WAITFOR,
		DELAY,
		TIME,
		CONNECT,
		DEFAULT,

		// Task subtypes

		STATEMENT,
		SQL,
		SUBSTITUTING,

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

		// Process sections
		
		PARAMETERS,
		VARIABLES,
		CONNECTIONS,
	}

	/**
	 * Sorted set of reserved word names.
	 */
	public static TreeSet<String> reservedWords;

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
		while (tokenizer.hasNextWordIgnoreCase(RW.TASK.name())) {

			Task task = parseTask();
			if (task.getName() == null) {
				task.setAnonymousIndex(taskIndex);
			}
			tasks.put(task.getName(), task);
			
			++taskIndex;
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

		reservedWords = Stream.of(RW.values()).map(Enum::name).collect(Collectors.toCollection(TreeSet::new));

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

		taskParsers.put(RW.SET.name(), new SetVariablesTaskParser());
		taskParsers.put(RW.UPDATE.name(), new UpdateTaskParser());
		taskParsers.put(RW.RUN.name(), new RunTaskParser());
		taskParsers.put(RW.CREATE.name(), new CreateTaskParser());
		taskParsers.put(RW.APPEND.name(), new AppendTaskParser());
		taskParsers.put(RW.WRITE.name(), new WriteTaskParser());
		taskParsers.put(RW.OPEN.name(), new OpenTaskParser());
		taskParsers.put(RW.LOAD.name(), new LoadTaskParser());
		taskParsers.put(RW.READ.name(), new ReadTaskParser());
		taskParsers.put(RW.ZIP.name(), new ZipTaskParser());
		taskParsers.put(RW.UNZIP.name(), new UnzipTaskParser());
		taskParsers.put(RW.PUT.name(), new PutTaskParser());
		taskParsers.put(RW.EMAIL.name(), new EmailTaskParser());
		taskParsers.put(RW.DELETE.name(), new DeleteTaskParser());
		taskParsers.put(RW.RENAME.name(), new RenameTaskParser());
		taskParsers.put(RW.COPY.name(), new CopyTaskParser());
		taskParsers.put(RW.MAKE.name(), new MakeDirectoryTaskParser());
		taskParsers.put(RW.LOG.name(), new LogTaskParser());
		taskParsers.put(RW.GO.name(), new GoTaskParser());
		taskParsers.put(RW.FAIL.name(), new FailTaskParser());
		taskParsers.put(RW.PROCESS.name(), new ProcessTaskParser());
		taskParsers.put(RW.DO.name(), new DoTaskParser());
		taskParsers.put(RW.FOR.name(), new ForTaskParser());
		taskParsers.put(RW.ON.name(), new OnTaskParser());
		taskParsers.put(RW.WAITFOR.name(), new WaitforTaskParser());
		taskParsers.put(RW.CONNECT.name(), new ConnectTaskParser());

		CsvFile.registerHandler(RW.CSV.name());
		TsvFile.registerHandler(RW.TSV.name());
		TxtFile.registerHandler(RW.TXT.name());
		XlsxBook.registerHandler(RW.XLSX.name());
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

	private Task parseTask()
			throws IOException, InputMismatchException, NoSuchElementException, NamingException {

		// Parse the common clauses that can introduce any task.

		String section = RW.TASK.name();
		if (!tokenizer.skipWordIgnoreCase(section)) {
			throw new InputMismatchException(section + " not found where expected");
		}

		BacktrackingTokenizerMark nameMark = tokenizer.mark();
		
		String name = tokenizer.nextWordUpperCase();
		if (tasks.containsKey(name)) {
			throw new NameAlreadyBoundException("Duplicate " + RW.TASK.name() + " name: " + name);
		}
		else if (reservedWords.contains(name)) {
			// If token is a reserved word, task is anonymous.
			name = null;
			tokenizer.reset(nameMark);
		}

		Map<Task, Task.Result> predecessors = new HashMap<Task, Task.Result>();
		Expression.Combination combination = null;

		if (tokenizer.skipWordIgnoreCase(RW.AFTER.name())) {
			combination = parsePredecessors(predecessors);
		}

		Expression<Boolean> condition = null;

		if (tokenizer.skipWordIgnoreCase(RW.IF.name())) {
			condition = parseBooleanExpression();
		}

		String taskTypeName = tokenizer.nextWordUpperCase();
		TaskParser parser = taskParsers.get(taskTypeName);

		if (parser == null) {
			throw new InputMismatchException("Invalid " + RW.TASK.name() +" type: " + taskTypeName);
		}

		Task task = parser.parse(new Task.Prologue(name, predecessors, combination, condition));

		nextEnd(section);

		return task;
	}

	private Expression.Combination parsePredecessors(Map<Task, Task.Result> predecessors)
			throws InputMismatchException, NoSuchElementException, IOException, NameNotFoundException {

		SimpleEntry<Task, Task.Result> firstPredecessor = parsePredecessor();
		predecessors.put(firstPredecessor.getKey(), firstPredecessor.getValue());

		Expression.Combination combination = null;

		if (tokenizer.skipWordIgnoreCase(RW.AND.name())) {
			combination = Expression.Combination.and;
		}
		else if (tokenizer.skipWordIgnoreCase(RW.OR.name())) {
			combination = Expression.Combination.or;
		}

		if (combination != null) {
			do {
				SimpleEntry<Task, Task.Result> nextPredecessor = parsePredecessor();
				predecessors.put(nextPredecessor.getKey(), nextPredecessor.getValue());
			} while (
					((combination == Expression.Combination.and) && tokenizer.skipWordIgnoreCase(RW.AND.name())) ||
					((combination == Expression.Combination.or) && tokenizer.skipWordIgnoreCase(RW.OR.name())));

			if (tokenizer.skipWordIgnoreCase(RW.AND.name()) || tokenizer.skipWordIgnoreCase(RW.OR.name())) {
				throw new InputMismatchException("Combinations mixing both " + RW.AND.name() + " and " + RW.OR.name() +" are not allowed with " + RW.AFTER.name());
			}
		}

		return combination;
	}

	private SimpleEntry<Task, Task.Result> parsePredecessor()
			throws InputMismatchException, NoSuchElementException, IOException, NameNotFoundException {

		String name = tokenizer.nextWordUpperCase();
		if (!tasks.containsKey(name)) {
			throw new NameNotFoundException("Predecessor task not defined: " + name);
		}
		Task task = tasks.get(name);

		Task.Result result = Task.Result.success; 
		
		if (tokenizer.skipWordIgnoreCase(RW.SUCCEEDS.name())) {
			result = Task.Result.success;
		}
		else if (tokenizer.skipWordIgnoreCase(RW.FAILS.name())) {
			result = Task.Result.failure;
		}
		else if (tokenizer.skipWordIgnoreCase(RW.COMPLETES.name())) {
			result = Task.Result.completed;
		}
		
		return new SimpleEntry<Task, Task.Result>(task, result);
	}

	protected void nextEnd(String section) throws IOException {

		if (!tokenizer.skipWordIgnoreCase(RW.END.name()) || !tokenizer.skipWordIgnoreCase(section)) {
			throw new InputMismatchException(RW.END.name() + " " + section + " not found where expected");
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

			DatabaseConnection connection = parseOptionalDatabaseConnection(null);

			if (tokenizer.skipWordIgnoreCase(RW.SCRIPT.name())) {
				return parseRunScript(prologue, connection);
			}
			else if (hasSQL(RW.RUN.name())) {
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
	
			DatabaseConnection connection = parseOptionalDatabaseConnection(RW.FROM.name());

			if (hasSQL(RW.UPDATE.name())) {
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

			WriteHeaderExpressions headers = parseWriteHeaders(RW.END.name());

			return new CreateTask(prologue, page, headers);
		}
	}

	class AppendTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {
	
			PageIdentifierExpression page = parsePageIdentifier(true);
	
			DatabaseConnection connection = parseOptionalDatabaseConnection(RW.FROM.name());

			if (hasSQL(RW.APPEND.name())) {
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

			WriteHeaderExpressions headers = parseWriteHeaders(RW.FROM.name());

			DatabaseConnection connection = parseOptionalDatabaseConnection(RW.FROM.name());

			if (tokenizer.skipWordIgnoreCase(RW.TABLE.name())) {
				return parseWriteFromTable(prologue, page, headers, connection);
			}
			else if (hasSQL(RW.WRITE.name())) {
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
	
			ReadHeaderExpressions headers = parseReadHeaders(RW.END.name());

			return new OpenTask(prologue, page, headers);
		}
	}

	class LoadTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {
	
			PageIdentifierExpression page = parsePageIdentifier(false);
	
			ColumnExpressions columns = parseColumns();

			DatabaseConnection connection = parseOptionalDatabaseConnection(RW.INTO.name());

			if (hasSQL(RW.LOAD.name())) {
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

			ReadHeaderExpressions headers = parseReadHeaders(RW.INTO.name());

			ColumnExpressions columns = parseColumns();
			
			columns.validate(headers);

			DatabaseConnection connection = parseOptionalDatabaseConnection(RW.INTO.name());

			if (tokenizer.skipWordIgnoreCase(RW.TABLE.name())) {
				return parseReadIntoTable(prologue, page, headers, columns, connection);
			}
			else if (hasSQL(RW.READ.name())) {
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
				throw new RuntimeException(RW.READ.name() + " " + RW.INTO.name() + " " + RW.TABLE.name() + " requires column headers");
			}

			Expression<String> table = parseStringExpression();

			Expression<String> prefix = null;
			if (tokenizer.skipWordIgnoreCase(RW.PREFIX.name())) {
				tokenizer.skipWordIgnoreCase(RW.WITH.name());
				prefix = parseStringExpression();
			}

			return new ReadIntoTableTask(prologue, page, headers, columns, connection, table, prefix);
		}
	}

	class ZipTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			tokenizer.skipWordIgnoreCase(RW.FROM.name());
			ArrayList<Expression<String>> sources = new ArrayList<Expression<String>>();
			do {
				sources.add(parseStringExpression());
			} while (tokenizer.skipDelimiter(","));
	
			tokenizer.skipWordIgnoreCase(RW.TO.name());
			Expression<String> target = parseStringExpression();

			return new ZipTask(prologue, sources, target);
		}
	}

	class UnzipTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			tokenizer.skipWordIgnoreCase(RW.FROM.name());
			Expression<String> source = parseStringExpression();

			Expression<String> target = null;
			if (tokenizer.skipWordIgnoreCase(RW.TO.name())) {
				target = parseStringExpression();
			}
			else {
				target = new StringConstant(".");
			}
	
			return new UnzipTask(prologue, source, target);
		}
	}

	class PutTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			boolean isBinary = tokenizer.skipWordIgnoreCase(RW.BINARY.name());
			if (!isBinary) {
				tokenizer.skipWordIgnoreCase(RW.ASCII.name());
			}
			
			List<Expression<String>> localNames = new LinkedList<Expression<String>>();
			do {
				localNames.add(parseStringExpression());
			} while (tokenizer.skipDelimiter(","));

			tokenizer.skipWordIgnoreCase(RW.TO.name());
			FtpConnection connection = (FtpConnection)parseOptionalConnection(RW.FTP.name(), FtpConnection.class.getName());
			Expression<String> remoteName = parseStringExpression();
	
			return new PutTask(prologue, isBinary, localNames, connection, remoteName);
		}
	}

	class EmailTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {
	
			EmailConnection connection = null;
			if (tokenizer.skipWordIgnoreCase(RW.THROUGH.name())) {
				connection = (EmailConnection)parseOptionalConnection(RW.EMAIL.name(), EmailConnection.class.getName());
				if (connection == null) {
					throw new RuntimeException("Missing connection on " + RW.THROUGH.name() + " in " + RW.EMAIL.name());
				}
			}
			if (!tokenizer.skipWordIgnoreCase(RW.FROM.name())) {
				throw new RuntimeException("Missing " + RW.FROM.name() + " in " + RW.EMAIL.name());
			}
			Expression<String> from = parseStringExpression();
	
			if (!tokenizer.skipWordIgnoreCase(RW.TO.name())) {
				throw new RuntimeException("Missing " + RW.TO.name() + " in " + RW.EMAIL.name());
			}
			List<Expression<String>> to = new LinkedList<Expression<String>>();
			do {
				to.add(parseStringExpression());
			} while (tokenizer.skipDelimiter(","));
	
			List<Expression<String>> cc = new LinkedList<Expression<String>>();
			if (tokenizer.skipWordIgnoreCase(RW.CC.name())) {
				do {
					cc.add(parseStringExpression());
				} while (tokenizer.skipDelimiter(","));
			}
	
			Expression<String> subject = null;
			if (tokenizer.skipWordIgnoreCase(RW.SUBJECT.name())) {
				subject = parseStringExpression();
			}
	
			Expression<String> body = null;
			if (tokenizer.skipWordIgnoreCase(RW.BODY.name())) {
				body = parseStringExpression();
			}
	
			List<Expression<String>> attachments = new LinkedList<Expression<String>>();
			if (tokenizer.skipWordIgnoreCase(RW.ATTACH.name())) {
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
	
			tokenizer.skipWordIgnoreCase(RW.FROM.name());
			Expression<String> from = parseStringExpression();
			tokenizer.skipWordIgnoreCase(RW.TO.name());
			Expression<String> to = parseStringExpression();
			return new RenameTask(prologue, from, to);
		}
	}

	class CopyTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			tokenizer.skipWordIgnoreCase(RW.FROM.name());
			List<Expression<String>> from = new LinkedList<Expression<String>>();
			do {
				from.add(parseStringExpression());
			} while (tokenizer.skipDelimiter(","));

			tokenizer.skipWordIgnoreCase(RW.TO.name());
			Expression<String> to = parseStringExpression();

			return new CopyTask(prologue, from, to);
		}
	}

	class MakeDirectoryTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {
	
			tokenizer.skipWordIgnoreCase(RW.DIRECTORY.name());
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
			if (!hasNextEnd(RW.TASK.name())) {
				expression = parseStringExpression();
			}
			return new GoTask(prologue, expression);
		}
	}

	class FailTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {
	
			Expression<String> expression = null;
			if (!hasNextEnd(RW.TASK.name())) {
				expression = parseStringExpression();
			}
			return new FailTask(prologue, expression);
		}
	}

	class ProcessTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			boolean isSynchronous;
			if (tokenizer.skipWordIgnoreCase(RW.ASYNC.name()) || tokenizer.skipWordIgnoreCase(RW.ASYNCHRONOUSLY.name())) {
				isSynchronous = false;
			}
			else if (tokenizer.skipWordIgnoreCase(RW.SYNC.name()) || tokenizer.skipWordIgnoreCase(RW.SYNCHRONOUSLY.name()) || true) {
				isSynchronous = true;
			}

			Expression<String> name = parseStringExpression();

			List<ExpressionBase> arguments = new LinkedList<ExpressionBase>();
			if (tokenizer.skipWordIgnoreCase(RW.WITH.name()) || !hasNextEnd(RW.TASK.name())) {
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
			if (tokenizer.skipWordIgnoreCase(RW.WHILE.name())) {
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

			tokenizer.skipWordIgnoreCase(RW.FROM.name());

			if (hasFileType(false)) {
				return parseForReadFile(prologue, variables);
			}
			else if (tokenizer.skipWordIgnoreCase(RW.VALUES.name())) {
				return parseForValues(prologue, variables);
			}
			else {
				DatabaseConnection connection = parseOptionalDatabaseConnection(null);

				if (hasSQL(RW.FOR.name())) {
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

			ReadHeaderExpressions headers = parseReadHeaders(RW.TASK.name());

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
	}

	class OnTaskParser implements TaskParser {
		
		public Task parse(Task.Prologue prologue)
				throws InputMismatchException, NoSuchElementException, IOException, NamingException {

			if (tokenizer.skipWordIgnoreCase(RW.SCHEDULE.name())) {
				return parseOnSchedule(prologue);
			}
			else {
				return parseOn(prologue);
			}
		}
		
		private Task parseOnSchedule(Task.Prologue prologue) throws IOException, NamingException {

			Expression<String> schedule = parseStringExpression();

			NestedTaskSet taskSet = NestedTaskSet.parse(thisTaskParser);

			return new OnScheduleTask(prologue, schedule, taskSet);
		}
		
		private Task parseOn(Task.Prologue prologue) throws IOException, NamingException {

			ScheduleSet schedules = ScheduleSet.parse(tokenizer);

			NestedTaskSet taskSet = NestedTaskSet.parse(thisTaskParser);

			return new OnTask(prologue, schedules, taskSet);
		}
	}

	class WaitforTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue)
				throws InputMismatchException, NoSuchElementException, IOException, NamingException {

			if (tokenizer.skipWordIgnoreCase(RW.DELAY.name())) {
				return parseWaitforDelay(prologue);
			}
			if (tokenizer.skipWordIgnoreCase(RW.TIME.name())) {
				return parseWaitforTime(prologue);
			}
			else {
				throw new InputMismatchException("Invalid argument in " + RW.WAITFOR.name() + " " + RW.TASK.name());
			}
		}

		private Task parseWaitforDelay(Task.Prologue prologue) throws IOException, NamingException {

			Expression<String> delay = parseStringExpression();

			return new WaitforDelayTask(prologue, delay);
		}

		private Task parseWaitforTime(Task.Prologue prologue) throws IOException, NamingException {

			Expression<String> time = parseStringExpression();

			return new WaitforTimeTask(prologue, time);
		}
	}

	class ConnectTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue)
				throws InputMismatchException, NoSuchElementException, IOException, NamingException {

			Connection connection = parseConnection();
			boolean inherit = false;
			Expression<String> properties = null;

			tokenizer.skipWordIgnoreCase(RW.TO.name());
			if (tokenizer.skipWordIgnoreCase(RW.DEFAULT.name())) {
				inherit = true;
				if (tokenizer.skipWordIgnoreCase(RW.WITH.name())) {
					properties = parseStringExpression();
				}
			}
			else {
				properties = parseStringExpression();
			}

			return new ConnectTask(prologue, connection, inherit, properties);
		}
	}

	private boolean hasSQL(String taskTypeName) throws IOException {

		if (tokenizer.skipWordIgnoreCase(RW.STATEMENT.name())) {
			return false;
		}
		else if (tokenizer.skipWordIgnoreCase(RW.SQL.name())) {
			return true;
		}
		else {
			throw new InputMismatchException("Invalid data source in " + taskTypeName + " " + RW.TASK.name());
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
			tokenizer.skipWordIgnoreCase(RW.SHEET.name());
			return new SheetIdentifierExpression(handler, filePath, sheetName);
		}
	}

	private WriteHeaderExpressions parseWriteHeaders(String wordAfterHeaders) throws IOException {
		
		tokenizer.skipWordIgnoreCase(RW.WITH.name());
		boolean noHeaders = tokenizer.skipWordIgnoreCase(RW.NO.name());
		tokenizer.skipWordIgnoreCase(RW.HEADERS.name());

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
		boolean ignoreHeaders = tokenizer.skipWordIgnoreCase(RW.IGNORE.name());
		if (!ignoreHeaders) {
			tokenizer.skipWordIgnoreCase(RW.WITH.name());
			noHeaders = tokenizer.skipWordIgnoreCase(RW.NO.name());
		}
		tokenizer.skipWordIgnoreCase(RW.HEADERS.name());

		boolean validateHeaders = !noHeaders && !ignoreHeaders &&
				!tokenizer.hasNextWordIgnoreCase(RW.COLUMNS.name()) && !tokenizer.hasNextWordIgnoreCase(wordAfterHeaders); 

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
		if (tokenizer.skipWordIgnoreCase(RW.COLUMNS.name())) {
			do {
				columns.add(parseAnyExpression());
			} while (tokenizer.skipDelimiter(","));
		}
		return new ColumnExpressions(columns);
	}

	private void parseParameterizedStatement(
			List<ExpressionBase> expressions,
			StringBuilder statement) throws IOException {
		
		if (tokenizer.skipWordIgnoreCase(RW.SUBSTITUTING.name())) {
			do {
				expressions.add(parseAnyExpression());
			} while (tokenizer.skipDelimiter(","));
			tokenizer.skipWordIgnoreCase(RW.INTO.name());
		}

		parseTokenizedStatement(statement);
	}

	private void parseTokenizedStatement(StringBuilder statement) throws IOException {

		BacktrackingTokenizerMark mark = tokenizer.mark();

		while (!hasNextBeginOrEnd(RW.TASK.name())) {
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

	private Connection parseOptionalConnection(String typeName, String className) throws InputMismatchException, NoSuchElementException, IOException {
		
		Connection connection = null;
		if (tokenizer.hasNextWord()) {
			if (tokenizer.skipWordIgnoreCase(RW.DEFAULT.name())) {
				tokenizer.skipWordIgnoreCase(typeName);
			}
			else {
				if (tokenizer.skipWordIgnoreCase(typeName)) {
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

	private DatabaseConnection parseOptionalDatabaseConnection(String introWord) throws InputMismatchException, NoSuchElementException, IOException {

		if (introWord != null) {
			tokenizer.skipWordIgnoreCase(introWord);
		}
		return (DatabaseConnection)parseOptionalConnection(RW.DATABASE.name(), DatabaseConnection.class.getName());
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

		if (!tokenizer.hasNextWordIgnoreCase(RW.END.name())) {
			return false;
		}

		BacktrackingTokenizerMark mark = tokenizer.mark();
		tokenizer.nextToken();

		boolean result = tokenizer.hasNextWordIgnoreCase(section);

		tokenizer.reset(mark);
		return result;
	}

	private boolean hasNextBeginOrEnd(String section) throws IOException {

		if (tokenizer.hasNextWordIgnoreCase(RW.END.name())) {
			return hasNextEnd(section);
		}
		else if (tokenizer.hasNextWordIgnoreCase(section)) {
			if (section == RW.TASK.name()) {
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
			throw new InputMismatchException("Internal error - unsupported " + RW.DATETIME.name() + " compatible expression");
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

		throw new InputMismatchException("Invalid expression for use with " + RW.FORMAT.name());
	}

	private Expression<Boolean> parseBooleanExpression() throws IOException {

		// See https://en.wikipedia.org/wiki/Recursive_descent_parser

		Expression<Boolean> left = parseBooleanAnd();
		while (tokenizer.skipWordIgnoreCase(RW.OR.name())) {
			Expression<Boolean> right = parseBooleanAnd();
			left = new BooleanBinary(left, right, Expression.Combination.or);
		}
		return left;
	}
	
	private Expression<Boolean> parseBooleanAnd() throws IOException {

		Expression<Boolean> left = parseBooleanInvert();
		while (tokenizer.skipWordIgnoreCase(RW.AND.name())) {
			Expression<Boolean> right = parseBooleanInvert();
			left = new BooleanBinary(left, right, Expression.Combination.and);
		}
		return left;
	}

	private Expression<Boolean> parseBooleanInvert() throws IOException {
	
		boolean invert = false;
		while (tokenizer.skipWordIgnoreCase(RW.NOT.name())) {
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
			if (tokenizer.skipWordIgnoreCase(RW.IS.name())) {

				boolean invert = tokenizer.skipWordIgnoreCase(RW.NOT.name());
				if (!tokenizer.skipWordIgnoreCase(RW.NULL.name())) {
					throw new InputMismatchException("NULL not found after IS " + (invert ? RW.NOT.name() + " " : "") + "as expected");
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
				throw new InputMismatchException("Internal error - unsupported " + RW.DATETIME.name() + " compatible comparison");
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
		else if (tokenizer.skipWordIgnoreCase(RW.NULL.name())) {
			return new IntegerConstant(null);
		}
		else if (tokenizer.skipWordIgnoreCase(RW.ISNULL.name())) {
			return integerTermParser.parseIsNull(VariableType.INTEGER);
		}
		else if (tokenizer.skipWordIgnoreCase(RW.IIF.name())) {
			return integerTermParser.parseIf();
		}
		else if (tokenizer.skipWordIgnoreCase(RW.DATEPART.name())) {

			tokenizer.skipDelimiter("(");
			ChronoField field =
					tokenizer.skipWordIgnoreCase(RW.YEAR.name()) ? ChronoField.YEAR :
					tokenizer.skipWordIgnoreCase(RW.MONTH.name()) ? ChronoField.MONTH_OF_YEAR :
					tokenizer.skipWordIgnoreCase(RW.DAY.name()) ? ChronoField.DAY_OF_MONTH :
					tokenizer.skipWordIgnoreCase(RW.WEEKDAY.name()) ? ChronoField.DAY_OF_WEEK :
					tokenizer.skipWordIgnoreCase(RW.HOUR.name()) ? ChronoField.HOUR_OF_DAY :
					tokenizer.skipWordIgnoreCase(RW.MINUTE.name()) ? ChronoField.MINUTE_OF_HOUR :
					tokenizer.skipWordIgnoreCase(RW.SECOND.name()) ? ChronoField.SECOND_OF_MINUTE :
					null;
			if (field == null) {
				throw new InputMismatchException("Unrecognized date part name for " + RW.DATEPART.name());
			}

			tokenizer.skipDelimiter(",");
			Expression<LocalDateTime> datetime = parseDatetimeExpression();
			tokenizer.skipDelimiter(")");
			
			return new IntegerFromDatetime(datetime, field);
		}
		else if (hasNextVariable()) {
			return integerTermParser.parseReference(VariableType.INTEGER);
		}
		else if (tokenizer.skipDelimiter("(")) {
			return integerTermParser.parseParenthesized();
		}
		else {
			throw new InputMismatchException("Invalid " + RW.INTEGER.name() + " expression term");
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
		else if (tokenizer.skipWordIgnoreCase(RW.NULL.name())) {
			return new StringConstant(null);
		}
		else if (tokenizer.skipWordIgnoreCase(RW.ISNULL.name())) {
			return stringTermParser.parseIsNull(VariableType.VARCHAR);
		}
		else if (tokenizer.skipWordIgnoreCase(RW.IIF.name())) {
			return stringTermParser.parseIf();
		}
		else if (tokenizer.skipWordIgnoreCase(RW.FORMAT.name())) {

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
				throw new InputMismatchException("Internal error - unsupported argument type for " + RW.FORMAT.name());
			}
		}
		else if (hasNextVariable()) {
			return stringTermParser.parseReference(VariableType.VARCHAR);
		}
		else if (tokenizer.skipDelimiter("(")) {
			return stringTermParser.parseParenthesized();
		}
		else {
			throw new InputMismatchException("Invalid " + RW.VARCHAR.name() + " expression term");
		}
	}

	Expression<LocalDateTime> parseDatetimeExpression()
			throws InputMismatchException, NoSuchElementException, IOException {

		if (tokenizer.skipWordIgnoreCase(RW.NULL.name())) {
			return new DatetimeConstant(null);
		}
		else if (tokenizer.skipWordIgnoreCase(RW.ISNULL.name())) {
			return datetimeTermParser.parseIsNull(VariableType.DATETIME);
		}
		else if (tokenizer.skipWordIgnoreCase(RW.IIF.name())) {
			return datetimeTermParser.parseIf();
		}
		else if (tokenizer.skipWordIgnoreCase(RW.GETDATE.name())) {
			tokenizer.skipDelimiter("(");
			tokenizer.skipDelimiter(")");
			return new DatetimeNullary();
		}
		else if (tokenizer.skipWordIgnoreCase(RW.DATEADD.name())) {

			tokenizer.skipDelimiter("(");
			ChronoUnit unit =
					tokenizer.skipWordIgnoreCase(RW.YEAR.name()) ? ChronoUnit.YEARS :
					tokenizer.skipWordIgnoreCase(RW.MONTH.name()) ? ChronoUnit.MONTHS :
					tokenizer.skipWordIgnoreCase(RW.DAY.name()) ? ChronoUnit.DAYS :
					tokenizer.skipWordIgnoreCase(RW.HOUR.name()) ? ChronoUnit.HOURS :
					tokenizer.skipWordIgnoreCase(RW.MINUTE.name()) ? ChronoUnit.MINUTES :
					tokenizer.skipWordIgnoreCase(RW.SECOND.name()) ? ChronoUnit.SECONDS :
					null;
			if (unit == null) {
				throw new InputMismatchException("Unrecognized date part name for " + RW.DATEADD.name());
			}

			tokenizer.skipDelimiter(",");
			Expression<Integer> increment = parseIntegerExpression();
			tokenizer.skipDelimiter(",");
			Expression<LocalDateTime> datetime = parseDatetimeExpression();
			tokenizer.skipDelimiter(")");
			
			return new DatetimeAdd(datetime, unit, increment);
		}
		else if (tokenizer.skipWordIgnoreCase(RW.DATEFROMPARTS.name())) {

			tokenizer.skipDelimiter("(");
			Expression<Integer> year = parseIntegerExpression();
			tokenizer.skipDelimiter(",");
			Expression<Integer> month = parseIntegerExpression();
			tokenizer.skipDelimiter(",");
			Expression<Integer> day = parseIntegerExpression();
			tokenizer.skipDelimiter(")");
			
			return new DateFromParts(year, month, day);
		}
		else if (tokenizer.skipWordIgnoreCase(RW.DATETIMEFROMPARTS.name())) {

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
				throw new InputMismatchException("Invalid " + RW.VARCHAR.name() + " expression term");
			}
		}
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
