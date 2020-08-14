/*
 * Copyright (c) 2016 - 2020, Ronald DeSantis
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
import java.util.Deque;
import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.AbstractMap.SimpleEntry;

import javax.naming.NameAlreadyBoundException;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;

import com.hauldata.dbpa.connection.*;
import com.hauldata.dbpa.datasource.*;
import com.hauldata.dbpa.datasource.ProcedureDataSource.*;
import com.hauldata.dbpa.expression.*;
import com.hauldata.dbpa.expression.Expression.Combination;
import com.hauldata.dbpa.expression.strings.*;
import com.hauldata.dbpa.file.*;
import com.hauldata.dbpa.file.book.XlsHandler;
import com.hauldata.dbpa.file.book.XlsxHandler;
import com.hauldata.dbpa.file.flat.CsvFile;
import com.hauldata.dbpa.file.flat.TsvFile;
import com.hauldata.dbpa.file.flat.TxtFile;
import com.hauldata.dbpa.file.html.HtmlOptions;
import com.hauldata.dbpa.task.*;
import com.hauldata.dbpa.task.RequestTask.Header;
import com.hauldata.dbpa.task.RequestTask.SourceWithAliases;
import com.hauldata.dbpa.task.RequestTask.TargetWithKeepers;
import com.hauldata.dbpa.task.Task.Prologue;
import com.hauldata.dbpa.task.Task.Result;
import com.hauldata.dbpa.task.expression.ColumnExpressions;
import com.hauldata.dbpa.task.expression.FileIdentifierExpression;
import com.hauldata.dbpa.task.expression.HtmlPageIdentifierExpression;
import com.hauldata.dbpa.task.expression.PageIdentifierExpression;
import com.hauldata.dbpa.task.expression.SheetIdentifierExpression;
import com.hauldata.dbpa.task.expression.SourceHeaderExpressions;
import com.hauldata.dbpa.task.expression.TargetHeaderExpressions;
import com.hauldata.dbpa.task.expression.fixed.DataFixedFieldExpressionsTarget;
import com.hauldata.dbpa.task.expression.fixed.FixedFieldExpressions;
import com.hauldata.dbpa.task.expression.fixed.LineNumberKeeperFixedFieldExpression;
import com.hauldata.dbpa.task.expression.fixed.ColumnKeeperFixedFieldExpression;
import com.hauldata.dbpa.task.expression.fixed.SetterFixedFieldExpression;
import com.hauldata.dbpa.task.expression.fixed.ValidatorFixedFieldExpression;
import com.hauldata.dbpa.variable.*;
import com.hauldata.util.schedule.ScheduleSet;
import com.hauldata.util.tokenizer.*;

/**
 * Base class parser for a set of tasks at either outer process level or inner nested level
 */
public abstract class TaskSetParser {

	TaskSetParser thisTaskSetParser;

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
		BIT,
		VARCHAR,
		CHAR,
		CHARACTER,
		DATETIME,
		DATE,
		//TABLE,	// Also a data source

		// Connection types

		DATABASE,
		FTP,
		//EMAIL,	// Also a task type

		// Task delimiters

		TASK,
		END,

		// Task predecessors and conditions

		CONCURRENTLY,
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
		INSERT,
		TRUNCATE,
		UPDATE,
		RUN,
		CREATE,
		APPEND,
		WRITE,
		CLOSE,
		OPEN,
		LOAD,
		READ,
		ZIP,
		UNZIP,
		PUT,
		GET,
		MOVE,
		EMAIL,
		DELETE,
		RENAME,
		COPY,
		MAKE,
		LOG,
		GO,
		STOP,
		FAIL,
		BREAK,
		PROCESS,
		DO,
		FOR,
		ON,
		WAITFOR,
		CONNECT,
		REQUEST,
		PROMPT,
		// Future:
		REMOVE,
		SEND,
		RECEIVE,
		SYSTEM,
		EXECUTE,
		CALL,


		// Task data sources and targets

		STATEMENT,
		SQL,
		PROCEDURE,
		TABLE,
		SCRIPT,
		VALUES,
		PROPERTIES,
		SOURCE,
		TARGET,
		FILES,
		// Future:
		FILE,
		VARIABLE,

		// Task parameters

		CONNECTION,
		NAME,
		OF,
		FROM,
		IN,
		INOUT,
		OUT,
		OUTPUT,
		RETURNING,
		INTO,
		CSV,
		TSV,
		TXT,
		XLS,
		XLSX,
		FIXED,
		SHEET,
		WITH,
		NO,
		IGNORE,
		HEADERS,
		COLUMNS,
		CONTAIN,
		DATA,
		LINE,
		NUMBER,
		TRAILER,
		PREFIX,
		BATCH,
		SIZE,
		BINARY,
		ASCII,
		THROUGH,
		TO,
		CC,
		BCC,
		SUBJECT,
		BODY,
		TEXT,
		HTML,
		ATTACH,
		ATTACHMENT,
		SELECT,
		WHERE,
		UNREAD,
		//READ,		// Also a task type
		COUNT,
		FOLDER,
		SENDER,
		RECEIVED,
		BEFORE,
		CONTAINS,
		DETACH,
		MARK,
		ALL,
		DIRECTORY,
		COMMAND,
		SYNC,
		SYNCHRONOUSLY,
		ASYNC,
		ASYNCHRONOUSLY,
		WHILE,
		SCHEDULE,
		DELAY,
		TIME,
		USING,
		DEFAULT,
		SUBSTITUTING,
		SOCKET,
		TIMEOUT,
		HEADER,
		POST,
		NOTHING,
		AS,
		JOIN,
		RESPONSE,
		KEEP,
		PASSWORD,
		EXISTS,

		// Functions

		ISNULL,
		IIF,
		CHOOSE,
		DATEPART,
		YEAR,
		MONTH,
		DAY,
		DAYOFYEAR,
		WEEKDAY,
		HOUR,
		MINUTE,
		SECOND,
		FORMAT,
		GETDATE,
		DATEADD,
		DATEFROMPARTS,
		DATETIMEFROMPARTS,
		ERROR_MESSAGE,

		BASE64STRING,
		CHARINDEX,
		LEFT,
		LTRIM,
		LEN,
		LOWER,
		REPLACE,
		REPLICATE,
		REVERSE,
		RIGHT,
		RTRIM,
		SPACE,
		SUBSTRING,
		UPPER,

		IS,
		NULL,

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
						KW.OF,
						KW.END,
						KW.TASK,

						KW.CONCURRENTLY,
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
						KW.DATABASE,
						KW.FTP,
						KW.EMAIL,

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

	protected VersionDependencies versionDependencies;

	protected TermParser<Integer> integerTermParser;
	protected TermParser<String> stringTermParser;
	protected TermParser<LocalDateTime> datetimeTermParser;
	protected TermParser<Table> tableTermParser;

	protected Map<String, TaskParser> taskParsers;
	protected Map<String, RequestTaskTrailingParser> requestTaskParsers;

	protected Map<String, FunctionParser<Integer>> integerFunctionParsers;
	protected Map<String, FunctionParser<String>> stringFunctionParsers;
	protected Map<String, FunctionParser<LocalDateTime>> datetimeFunctionParsers;
	protected Map<String, FunctionParser<Table>> tableFunctionParsers;

	protected PhraseParser<EmailSource.Field> emailSourceFieldParser;
	protected PhraseParser<EmailSource.Status> emailSourceStatusParser;

	protected Map<String, VariableBase> variables;
	protected Map<String, Connection> connections;
	protected Map<String, DbProcess> siblingProcesses;

	private static class State {

		private Task parentTask;
		private Task.Reference taskReference;

		public State(Task parentTask) {
			this.parentTask = parentTask;
			this.taskReference = null;
		}

		public void prepareTaskReference() {
			taskReference = new Task.Reference();
		}

		public Task.Reference getTaskReference() {
			return taskReference;
		}

		public void resolveTaskReference(Task task) {
			taskReference.task = task;
		}

		public Task getParentTask() {
			return parentTask;
		}
	}

	Deque<State> states;

	private void initializeState() { states = new LinkedList<State>(); }
	private void pushState(Task parentTask) { states.addFirst(new State(parentTask)); }
	private void prepareTaskReference() { states.peekFirst().prepareTaskReference(); }
	private Task.Reference getTaskReference() { return states.peekFirst().getTaskReference(); }
	private void resolveTaskReference(Task task) { states.peekFirst().resolveTaskReference(task); }
	private Task getParentTask() { return states.peekFirst().getParentTask(); }
	private void popState() { states.removeFirst(); }

	/**
	 * Constructor.
	 */
	public TaskSetParser(
			Reader r,
			Map<String, VariableBase> variables,
			Map<String, Connection> connections,
			Map<String, DbProcess> siblingProcesses) throws IOException {

		thisTaskSetParser = this;

		tokenizer = new BacktrackingTokenizer(r);

		tokenizer.useDelimiter("<=");
		tokenizer.useDelimiter(">=");
		tokenizer.useDelimiter("<>");
		tokenizer.useEndLineCommentDelimiter("--");

		tokenizer.wordChars('_', '_');

		tokenizer.eolIsSignificant(false);

		versionDependencies = tokenizer.skipWordIgnoreCase(KW.PROCESS.name()) ?
				new LatestVersionDependencies() :
				new LegacyVersionDependencies();

		integerTermParser = new TermParser<Integer>(VariableType.INTEGER) { public Expression<Integer> parseExpression() throws IOException { return parseIntegerExpression(); } };
		stringTermParser = new TermParser<String>(VariableType.VARCHAR) { public Expression<String> parseExpression() throws IOException { return parseStringExpression(); } };
		datetimeTermParser = new TermParser<LocalDateTime>(VariableType.DATETIME) { public Expression<LocalDateTime> parseExpression() throws IOException { return parseDatetimeExpression(); } };
		tableTermParser = new TermParser<Table>(VariableType.TABLE) { public Expression<Table> parseExpression() throws IOException { return parseTableVariableExpression(); } };

		taskParsers = new HashMap<String, TaskParser>();

		taskParsers.put(KW.SET.name(), new SetVariablesTaskParser());
		taskParsers.put(KW.INSERT.name(), new InsertTaskParser());
		taskParsers.put(KW.TRUNCATE.name(), new TruncateTaskParser());
		taskParsers.put(KW.UPDATE.name(), new UpdateTaskParser());
		taskParsers.put(KW.RUN.name(), new RunTaskParser());
		taskParsers.put(KW.CREATE.name(), new CreateTaskParser());
		taskParsers.put(KW.APPEND.name(), new AppendTaskParser());
		taskParsers.put(KW.WRITE.name(), new WriteTaskParser());
		taskParsers.put(KW.CLOSE.name(), new CloseTaskParser());
		taskParsers.put(KW.OPEN.name(), new OpenTaskParser());
		taskParsers.put(KW.LOAD.name(), new LoadTaskParser());
		taskParsers.put(KW.READ.name(), new ReadTaskParser());
		taskParsers.put(KW.ZIP.name(), new ZipTaskParser());
		taskParsers.put(KW.UNZIP.name(), new UnzipTaskParser());
		taskParsers.put(KW.PUT.name(), new PutTaskParser());
		taskParsers.put(KW.GET.name(), new GetTaskParser());
		taskParsers.put(KW.MOVE.name(), new MoveTaskParser());
		taskParsers.put(KW.EMAIL.name(), new EmailTaskParser());
		taskParsers.put(KW.DELETE.name(), new DeleteTaskParser());
		taskParsers.put(KW.RENAME.name(), new RenameTaskParser());
		taskParsers.put(KW.COPY.name(), new CopyTaskParser());
		taskParsers.put(KW.MAKE.name(), new MakeDirectoryTaskParser());
		taskParsers.put(KW.LOG.name(), new LogTaskParser());
		taskParsers.put(KW.GO.name(), new GoTaskParser());
		taskParsers.put(KW.STOP.name(), new StopTaskParser());
		taskParsers.put(KW.FAIL.name(), new FailTaskParser());
		taskParsers.put(KW.BREAK.name(), new BreakTaskParser());
		taskParsers.put(KW.PROCESS.name(), new ProcessTaskParser());
		taskParsers.put(KW.DO.name(), new DoTaskParser());
		taskParsers.put(KW.FOR.name(), new ForTaskParser());
		taskParsers.put(KW.ON.name(), new OnTaskParser());
		taskParsers.put(KW.WAITFOR.name(), new WaitforTaskParser());
		taskParsers.put(KW.CONNECT.name(), new ConnectTaskParser());
		taskParsers.put(KW.REQUEST.name(), new RequestTaskParser());
		taskParsers.put(KW.PROMPT.name(), new PromptTaskParser());

		requestTaskParsers = new HashMap<String, RequestTaskTrailingParser>();

		requestTaskParsers.put(KW.GET.name(), new GetRequestTaskParser());
		requestTaskParsers.put(KW.PUT.name(), new PutRequestTaskParser());
		requestTaskParsers.put(KW.POST.name(), new PostRequestTaskParser());
		requestTaskParsers.put(KW.DELETE.name(), new DeleteRequestTaskParser());

		KW firstNotImplemented = KW.REMOVE;
		KW lastNotImplemented = KW.CALL;

		for (KW notImplemented : Stream.of(KW.values()).filter(
				kw -> (kw.compareTo(firstNotImplemented) >= 0) && (kw.compareTo(lastNotImplemented) <= 0)).collect(Collectors.toList())) {
			taskParsers.put(notImplemented.name(), new NotImplementedTaskParser(notImplemented));
		}

		integerFunctionParsers = new HashMap<String, FunctionParser<Integer>>();
		integerFunctionParsers.put(KW.ISNULL.name(), new IsNullParser<Integer>(integerTermParser));
		integerFunctionParsers.put(KW.IIF.name(), new IfParser<Integer>(integerTermParser));
		integerFunctionParsers.put(KW.CHOOSE.name(), new ChooseParser<Integer>(integerTermParser));
		integerFunctionParsers.put(KW.CALL.name(), new CallParser<Integer>(integerTermParser));
		integerFunctionParsers.put(KW.DATEPART.name(), new DatePartParser());
		integerFunctionParsers.put(KW.CHARINDEX.name(), new CharIndexParser());
		integerFunctionParsers.put(KW.LEN.name(), new LengthParser());

		stringFunctionParsers = new HashMap<String, FunctionParser<String>>();
		stringFunctionParsers.put(KW.ISNULL.name(), new IsNullParser<String>(stringTermParser));
		stringFunctionParsers.put(KW.IIF.name(), new IfParser<String>(stringTermParser));
		stringFunctionParsers.put(KW.CHOOSE.name(), new ChooseParser<String>(stringTermParser));
		stringFunctionParsers.put(KW.CALL.name(), new CallParser<String>(stringTermParser));
		stringFunctionParsers.put(KW.FORMAT.name(), new FormatParser());
		stringFunctionParsers.put(KW.LEFT.name(), new LeftParser());
		stringFunctionParsers.put(KW.LTRIM.name(), new LeftTrimParser());
		stringFunctionParsers.put(KW.LOWER.name(), new LowerParser());
		stringFunctionParsers.put(KW.REPLACE.name(), new ReplaceParser());
		stringFunctionParsers.put(KW.REPLICATE.name(), new ReplicateParser());
		stringFunctionParsers.put(KW.REVERSE.name(), new ReverseParser());
		stringFunctionParsers.put(KW.RIGHT.name(), new RightParser());
		stringFunctionParsers.put(KW.RTRIM.name(), new RightTrimParser());
		stringFunctionParsers.put(KW.SPACE.name(), new SpaceParser());
		stringFunctionParsers.put(KW.SUBSTRING.name(), new SubstringParser());
		stringFunctionParsers.put(KW.UPPER.name(), new UpperParser());
		stringFunctionParsers.put(KW.ERROR_MESSAGE.name(), new ErrorMessageParser());
		stringFunctionParsers.put(KW.CHAR.name(), new CharParser());
		stringFunctionParsers.put(KW.BASE64STRING.name(), new Base64StringParser());

		datetimeFunctionParsers = new HashMap<String, FunctionParser<LocalDateTime>>();
		datetimeFunctionParsers.put(KW.ISNULL.name(), new IsNullParser<LocalDateTime>(datetimeTermParser));
		datetimeFunctionParsers.put(KW.IIF.name(), new IfParser<LocalDateTime>(datetimeTermParser));
		datetimeFunctionParsers.put(KW.CHOOSE.name(), new ChooseParser<LocalDateTime>(datetimeTermParser));
		datetimeFunctionParsers.put(KW.CALL.name(), new CallParser<LocalDateTime>(datetimeTermParser));
		datetimeFunctionParsers.put(KW.GETDATE.name(), new GetDateParser());
		datetimeFunctionParsers.put(KW.DATEADD.name(), new DateAddParser());
		datetimeFunctionParsers.put(KW.DATEFROMPARTS.name(), new DateFromPartsParser());
		datetimeFunctionParsers.put(KW.DATETIMEFROMPARTS.name(), new DateTimeFromPartsParser());

		tableFunctionParsers = new HashMap<String, FunctionParser<Table>>();
		tableFunctionParsers.put(KW.IIF.name(), new IfParser<Table>(tableTermParser));
		tableFunctionParsers.put(KW.CHOOSE.name(), new ChooseParser<Table>(tableTermParser));
		tableFunctionParsers.put(KW.CALL.name(), new CallParser<Table>(tableTermParser));

		emailSourceFieldParser = new PhraseParser<EmailSource.Field>(
				new String[] {
						KW.COUNT.name(),
						KW.SENDER.name(),
						KW.RECEIVED.name(),
						KW.SUBJECT.name(),
						KW.BODY.name(),
						KW.ATTACHMENT.name() + " " + KW.COUNT.name(),
						KW.ATTACHMENT.name() + " " + KW.NAME.name()},
				EmailSource.Field.values());

		emailSourceStatusParser = new PhraseParser<EmailSource.Status>(
				new String[] {
						KW.UNREAD.name(),
						KW.READ.name(),
						KW.ALL.name()},
				EmailSource.Status.values());

		this.variables = variables;
		this.connections = connections;
		this.siblingProcesses = siblingProcesses;

		initializeState();

		CsvFile.registerHandler(KW.CSV.name());
		TsvFile.registerHandler(KW.TSV.name());
		TxtFile.registerHandler(KW.TXT.name());
		XlsHandler.register(KW.XLS.name());
		XlsxHandler.register(KW.XLSX.name());
	}

	public BacktrackingTokenizer getTokenizer() {
		return tokenizer;
	}

	public void close() {
		try { tokenizer.close(); } catch (IOException e) {}
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
	 public Map<String, Task> parseTasks(Task parentTask, String structureName)
			throws IOException, InputMismatchException, NoSuchElementException, NameNotFoundException, NameAlreadyBoundException, NamingException {

		pushState(parentTask);

		Map<String, Task> tasks = new HashMap<String, Task>();

		int taskIndex = 1;
		Task previousTask = null;
		while (hasTask()) {

			Task task = parseTask(tasks, previousTask);
			if (task.getTaskName() == null) {
				task.setTaskNameFromIndex(taskIndex, tokenizer.lineno());
			}
			tasks.put(task.getTaskName(), task);

			++taskIndex;
			previousTask = task;
		}

		endStructure(structureName);

		determineSuccessors(tasks);

		popState();

		return tasks;
	}

	static private void determineSuccessors(Map<String, Task> tasks) {
		for (Task task : tasks.values()) {
			for (Task potentialSuccessor : tasks.values()) {
				if (potentialSuccessor.getPredecessors().containsKey(task)) {
					task.addSuccessor(potentialSuccessor);
				}
			}
		}
	}

	protected String processStructureName() {
		return KW.PROCESS.name();
	}

	interface TaskParser {
		Task parse(Task.Prologue prologue) throws IOException, NamingException;
	}

	private Task parseTask(Map<String, Task> tasks, Task previousTask)
			throws IOException, InputMismatchException, NoSuchElementException, NamingException {

		startTask();

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

		Expression<String> qualifier = null;
		if (tokenizer.skipWordIgnoreCase(KW.OF.name())) {
			qualifier = parseStringExpression();
		}

		if ((name != null) || (qualifier != null)) {
			delimitTaskName();
		}

		Map<Task, Task.Result> predecessors = new HashMap<Task, Task.Result>();
		Expression.Combination combination = null;

		if (tokenizer.skipWordIgnoreCase(KW.CONCURRENTLY.name())) {
			// No predecessors
		}
		else if (tokenizer.skipWordIgnoreCase(KW.AFTER.name())) {
			combination = parsePredecessors(tasks, predecessors, previousTask);
		}
		else {
			combination = addDefaultPredecessors(predecessors, previousTask);
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

		prepareTaskReference();

		Task task = parser.parse(new Task.Prologue(name, qualifier, predecessors, combination, condition, getParentTask()));

		resolveTaskReference(task);

		endTask(task);

		return task;
	}

	private Expression.Combination parsePredecessors(Map<String, Task> tasks, Map<Task, Task.Result> predecessors, Task previousTask)
			throws InputMismatchException, NoSuchElementException, IOException, NameNotFoundException {

		SimpleEntry<Task, Task.Result> firstPredecessor = parseFirstPredecessor(tasks, previousTask);
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
				SimpleEntry<Task, Task.Result> nextPredecessor = parsePredecessor(tasks);
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

	private SimpleEntry<Task, Task.Result> parseFirstPredecessor(Map<String, Task> tasks, Task previousTask)
			throws InputMismatchException, NoSuchElementException, IOException, NameNotFoundException {

		if (tokenizer.skipWordIgnoreCase(KW.PREVIOUS.name())) {
			return parsePredecessorResult(previousTask);
		}

		BacktrackingTokenizerMark nameMark = tokenizer.mark();
		String name = tokenizer.nextWordUpperCase();
		tokenizer.reset(nameMark);

		if (!tasks.containsKey(name)) {
			// If token is not a task name, this is a naked AFTER clause.  Predecessor is the preceding task.
			return parsePredecessorResult(previousTask);
		}
		else {
			return parsePredecessor(tasks);
		}
	}

	private SimpleEntry<Task, Task.Result> parsePredecessor(Map<String, Task> tasks)
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

		if (task == null) {
			throw new InputMismatchException(KW.AFTER.name() + " [" + KW.PREVIOUS.name() + "] with no previous task");
		}

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

	abstract class VersionDependencies {
		public void endEntity(String entity) throws IOException {
			if (!tokenizer.skipWordIgnoreCase(KW.END.name()) || !tokenizer.skipWordIgnoreCase(entity)) {
				throw new InputMismatchException(KW.END.name() + " " + entity + " not found where expected");
			}
		}

		public abstract void endSection(String section) throws IOException;
		public abstract boolean hasTask() throws IOException;
		public abstract void startTask() throws IOException;
		public abstract void delimitTaskName() throws IOException;
		public abstract Combination addDefaultPredecessors(Map<Task, Result> predecessors, Task previousTask);
		public abstract boolean atEndOfTask() throws IOException;
		public abstract void endTask(Task task) throws IOException;
		public abstract boolean atEndOrStartOfLegacyTask() throws IOException;
		public abstract boolean atEndOfProcedure() throws IOException;
		public abstract void endStructure(String section) throws IOException;
		public abstract void endProcess(boolean isMain) throws IOException;
	}

	class LegacyVersionDependencies extends VersionDependencies {
		@Override
		public void endSection(String section) throws IOException {
			endEntity(section);
		}

		@Override
		public boolean hasTask() throws IOException {
			return tokenizer.hasNextWordIgnoreCase(KW.TASK.name());
		}

		@Override
		public void startTask() throws IOException {
			if (!tokenizer.skipWordIgnoreCase(KW.TASK.name())) {
				throw new InputMismatchException(KW.TASK.name() + " not found where expected");
			}
		}

		@Override
		public void delimitTaskName() {}

		@Override
		public Combination addDefaultPredecessors(Map<Task, Result> predecessors, Task previousTask) {
			return null;
		}

		@Override
		public boolean atEndOfTask() throws IOException {
			return atEndOf(KW.TASK);
		}

		@Override
		public void endTask(Task task) throws IOException {
			endSection(KW.TASK.name());
		}

		private boolean atStartOfTask() throws IOException {

			if (!tokenizer.hasNextWordIgnoreCase(KW.TASK.name())) {
				return false;
			}

			BacktrackingTokenizerMark mark = tokenizer.mark();
			tokenizer.nextToken();

			boolean result = tokenizer.hasNextWord();

			tokenizer.reset(mark);
			return result;
		}

		@Override
		public boolean atEndOrStartOfLegacyTask() throws IOException {
			return atEndOfTask() || atStartOfTask();
		}

		@Override
		public boolean atEndOfProcedure() throws IOException {
			return atEndOrStartOfLegacyTask();
		}

		@Override
		public void endStructure(String section) {}

		@Override
		public void endProcess(boolean isMain) throws IOException {
			if (!isMain) {
				endEntity(processStructureName());
			}
		}
	}

	class LatestVersionDependencies extends  VersionDependencies {
		@Override
		public void endSection(String section) throws IOException {
			if (!tokenizer.skipDelimiter(";")) {
				throw new InputMismatchException("Semicolon \";\" not found where expected");
			}
		}

		@Override
		public boolean hasTask() throws IOException {
			return !tokenizer.hasNextWordIgnoreCase(KW.END.name());
		}

		@Override
		public void startTask() {}

		@Override
		public void delimitTaskName() throws IOException {
			if (!tokenizer.skipDelimiter(":")) {
				throw new InputMismatchException("Colon \":\" not found after task name");
			}
		}

		@Override
		public Combination addDefaultPredecessors(Map<Task, Result> predecessors, Task previousTask) {
			if (previousTask != null) {
				predecessors.put(previousTask, Result.success);
				return Combination.and;
			}
			else {
				return null;
			}
		}

		@Override
		public boolean atEndOfTask() throws IOException {
			return tokenizer.hasNextDelimiter(";");
		}

		@Override
		public void endTask(Task task) throws IOException {
			// Semicolon is optional after END entity-name
			if (TaskSetParent.class.isInstance(task)) {
				tokenizer.skipDelimiter(";");
			}
			else {
				endSection(KW.TASK.name());
			}
		}

		@Override
		public boolean atEndOrStartOfLegacyTask() throws IOException {
			return false;
		}

		@Override
		public boolean atEndOfProcedure() throws IOException {
			return atEndOfTask();
		}

		@Override
		public void endStructure(String structure) throws IOException {
			endEntity(structure);
		}

		@Override
		public void endProcess(boolean isMain) throws IOException {
			tokenizer.skipDelimiter(";");
		}
	}

	protected void endSection(String section) throws IOException {
		versionDependencies.endSection(section);
	}

	private boolean hasTask() throws IOException {
		return versionDependencies.hasTask();
	}

	private void startTask() throws IOException {
		versionDependencies.startTask();
	}

	private void delimitTaskName() throws IOException {
		versionDependencies.delimitTaskName();
	}

	private Combination addDefaultPredecessors(Map<Task, Result> predecessors, Task previousTask) {
		return versionDependencies.addDefaultPredecessors(predecessors, previousTask);
	}

	private boolean atEndOfTask() throws IOException {
		return versionDependencies.atEndOfTask();
	}

	private void endTask(Task task) throws IOException {
		versionDependencies.endTask(task);
	}

	private boolean atEndOfProcedure() throws IOException {
		return versionDependencies.atEndOfProcedure();
	}

	private boolean atEndOrStartOfLegacyTask() throws IOException {
		return versionDependencies.atEndOrStartOfLegacyTask();
	}

	protected void endStructure(String structure) throws IOException {
		versionDependencies.endStructure(structure);
	}

	protected void endProcess(boolean isMain) throws IOException {
		versionDependencies.endProcess(isMain);
	}

	class SetVariablesTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			List<SetVariablesTask.Assignment> assignments = new LinkedList<SetVariablesTask.Assignment>();

			do { assignments.add(parseAssignment());
			} while (tokenizer.skipDelimiter(","));

			return new SetVariablesTask(prologue, assignments);
		}

		private SetVariablesTask.Assignment parseAssignment()
				throws InputMismatchException, NoSuchElementException, IOException {

			VariableBase variable = parseVariableReference();

			tokenizer.nextDelimiter("=");

			ExpressionBase expression = parseExpression(variable.getType(), true);

			return new SetVariablesTask.Assignment(variable, expression);
		}
	}

	class InsertTaskParser implements TaskParser {

		@SuppressWarnings("unchecked")
		public Task parse(Task.Prologue prologue) throws IOException {

			tokenizer.skipWordIgnoreCase(KW.INTO.name());

			VariableBase variable = parseVariableReference();
			if (variable.getType() != VariableType.TABLE) {
				throw new InputMismatchException("Variable must be of " + KW.TABLE.name() + " type: " + variable.getName());
			}

			Source source = parseSource(KW.INSERT.name(), null, false, true, null);
			if (source instanceof TableVariableSource && (((TableVariableSource)source).getVariable() == variable)) {
				throw new InputMismatchException("Cannot " + KW.INSERT.name() +" a variable into itself");
			}

			variable.setValueObject(new Table());

			return new InsertTask(prologue, (Variable<Table>)variable, source);
		}
	}

	class TruncateTaskParser implements TaskParser {

		@SuppressWarnings("unchecked")
		public Task parse(Task.Prologue prologue) throws IOException {

			VariableBase variable = parseVariableReference();
			if (variable.getType() != VariableType.TABLE) {
				throw new InputMismatchException("Variable must be of " + KW.TABLE.name() + " type: " + variable.getName());
			}

			return new TruncateTask(prologue, (Variable<Table>)variable);
		}
	}

	class RunTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException, NamingException {

			if (tokenizer.skipWordIgnoreCase(KW.PROCESS.name())) {
				return taskParsers.get(KW.PROCESS.name()).parse(prologue);
			}
			else if (tokenizer.skipWordIgnoreCase(KW.COMMAND.name())) {
				return parseRunCommand(prologue);
			}
			else if (tokenizer.skipWordIgnoreCase(KW.SCRIPT.name())) {
				return parseRunScript(prologue);
			}
			else {
				return parseRun(prologue);
			}
		}

		private Task parseRunCommand(Task.Prologue prologue) throws IOException {

			Expression<String> name = parseStringExpression();

			List<ExpressionBase> arguments = new LinkedList<ExpressionBase>();
			if (tokenizer.skipWordIgnoreCase(KW.WITH.name()) || (!atEndOfTask() && !tokenizer.hasNextWordIgnoreCase(KW.RETURNING.name()))) {
				do {
					arguments.add(parseAnyExpression());
				} while (tokenizer.skipDelimiter(","));
			}

			VariableBase resultParam = null;
			if (tokenizer.skipWordIgnoreCase(KW.RETURNING.name())) {
				resultParam = parseVariableReference();
				if (resultParam.getType() != VariableType.INTEGER) {
					throw new InputMismatchException(
							KW.RETURNING.name() + " variable for " + KW.RUN.name() + " " + KW.TASK.name() + " must be of type " + KW.INTEGER.name());
				}
			}

			return new RunCommandTask(prologue, name, arguments, resultParam);
		}

		private Task parseRunScript(Task.Prologue prologue) throws IOException {

			Expression<String> source = parseStringExpression();

			DatabaseConnection connection = parseDatabaseConnection(KW.ON.name());

			DataExecutor executor = new DataExecutor(connection);

			return new RunScriptTask(prologue, executor, source);
		}

		private Task parseRun(Task.Prologue prologue) throws IOException {

			DatabaseConnection connection = parseDatabaseConnection(null);

			DataSource source = parseDataSource(KW.RUN.name(), connection, false, false);

			return new RunTask(prologue, source);
		}
	}

	class UpdateTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue)
						throws InputMismatchException, NoSuchElementException, IOException {

			List<VariableBase> variables = new ArrayList<VariableBase>();
			do {
				variables.add(parseVariableReference());
			} while (tokenizer.skipDelimiter(","));

			ArrayList<VariableType> columnTypes = variables
					.stream()
					.map(v -> v.getType())
					.collect(Collectors.toCollection(ArrayList::new));
			Source source = parseSource(KW.UPDATE.name(), KW.FROM.name(), true, true, columnTypes);

			return new UpdateTask(prologue, variables, source);
		}
	}

	class CreateTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			FileHandler handler = parseFileHandler(true);

			PageIdentifierExpression page = parsePageIdentifier(handler);

			PageOptions options = parseFileOptions(true, handler);

			TargetHeaderExpressions headers = parseTargetHeaders(KW.END.name());

			return new CreateTask(prologue, page, options, headers);
		}
	}

	class AppendTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			FileHandler handler = parseFileHandler(true);

			PageIdentifierExpression page = parsePageIdentifier(handler);

			Source source = parseSource(KW.APPEND.name(), KW.FROM.name(), false, true, null);

			return new AppendTask(prologue, page, source);
		}
	}

	class WriteTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			if (tokenizer.skipWordIgnoreCase(KW.HTML.name())) {
				return parseWriteHtmlTask(prologue);
			}
			else {
				return parseWriteFileTask(prologue);
			}
		}

		private Task parseWriteFileTask(Task.Prologue prologue) throws IOException {

			FileHandler handler = parseFileHandler(true);

			PageIdentifierExpression page = parsePageIdentifier(handler);

			PageOptions options = parseFileOptions(true, handler);

			TargetHeaderExpressions headers = parseTargetHeaders(KW.FROM.name());

			Source source = parseSource(KW.WRITE.name(), KW.FROM.name(), false, true, null);

			return new WriteTask(prologue, page, options, headers, source);
		}

		@SuppressWarnings("unchecked")
		private Task parseWriteHtmlTask(Task.Prologue prologue) throws IOException {

			VariableBase variable = parseVariableReference();

			if (!variable.getType().equals(VariableType.VARCHAR)) {
				throw new InputMismatchException(
						"Target variable for " + KW.WRITE.name() + " " + KW.HTML.name() + " " + KW.TASK.name() + " must be of type " + KW.VARCHAR.name());
			}

			PageIdentifierExpression page = new HtmlPageIdentifierExpression((Variable<String>)variable);

			final HtmlOptions.Parser optionsParser = new HtmlOptions.Parser();

			PageOptions options = optionsParser.parse(thisTaskSetParser);

			TargetHeaderExpressions headers = parseTargetHeaders(KW.FROM.name());

			Source source = parseSource(KW.WRITE.name(), KW.FROM.name(), false, true, null);

			return new WriteTask(prologue, page, options, headers, source);
		}
	}

	class CloseTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			Expression<String> file = parseStringExpression();

			return new CloseTask(prologue, file);
		}
	}

	class OpenTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			FileHandler handler = parseFileHandler(false);

			PageIdentifierExpression page = parsePageIdentifier(handler, false);

			PageOptions options = parseFileOptions(false, handler);

			SourceHeaderExpressions headers = parseSourceHeaders(KW.END.name());

			return new OpenTask(prologue, page, options, headers);
		}
	}

	class LoadTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			FileHandler handler = parseFileHandler(false);

			PageIdentifierExpression page = parsePageIdentifier(handler, false);

			ColumnExpressions columns = parseColumns();

			DataTarget target = parseDataTarget(KW.LOAD.name(), KW.INTO.name(), false, false);

			return new LoadTask(prologue, page, columns, target);
		}
	}

	class ReadTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			if (tokenizer.skipWordIgnoreCase(KW.FIXED.name())) {
				return parseReadFixed(prologue);
			}
			else {
				return parseReadDelimited(prologue);
			}
		}

		public Task parseReadDelimited(Task.Prologue prologue) throws IOException {

			FileHandler handler = parseFileHandler(false);

			PageIdentifierExpression page = parsePageIdentifier(handler, false);

			PageOptions options = parseFileOptions(false, handler);

			SourceHeaderExpressions headers = parseSourceHeaders(KW.INTO.name());

			ColumnExpressions columns = parseColumns();

			columns.validate(headers);

			DataTarget target = parseDataTarget(KW.READ.name(), KW.INTO.name(), true, headers.exist());

			return new ReadTask(prologue, page, options, headers, columns, target);
		}

		private Task parseReadFixed(Task.Prologue prologue) throws IOException {

			PageIdentifierExpression page = parsePageIdentifier(null);

			List<FixedFieldExpressions> headers = parseFixedHeaders();

			List<DataFixedFieldExpressionsTarget> dataRecordTargets = parseFixedDataRecordTargets();

			List<FixedFieldExpressions> trailers = parseFixedTrailers();

			return new ReadFixedTask(prologue, page, headers, dataRecordTargets, trailers);
		}

		private List<FixedFieldExpressions> parseFixedHeaders() throws IOException {

			List<FixedFieldExpressions> headers = new LinkedList<FixedFieldExpressions>();
			while (tokenizer.skipWordIgnoreCase(KW.HEADER.name())) {
				headers.add(parseFixedFields());
			}
			return headers;
		}

		private List<DataFixedFieldExpressionsTarget> parseFixedDataRecordTargets() throws InputMismatchException, IOException {

			if (!tokenizer.skipWordIgnoreCase(KW.DATA.name())) {
				throw new InputMismatchException("Expecting " + KW.DATA.name() + ", found " + tokenizer.nextToken().getImage());
			}

			List<DataFixedFieldExpressionsTarget> dataRecordTargets = new LinkedList<DataFixedFieldExpressionsTarget>();
			if (!tokenizer.skipWordIgnoreCase(KW.IGNORE.name())) {
				do {
					DataFixedFieldExpressionsTarget dataFields = parseFixedDataFieldsTarget();

					dataRecordTargets.add(dataFields);
				} while (tokenizer.skipWordIgnoreCase(KW.DATA.name()));

				if (1 < dataRecordTargets.size()) {
					if (!dataRecordTargets.get(0).hasJoin()) {
						throw new InputMismatchException("When there are multiple " + KW.DATA.name() + " clauses the first must include at least one " + KW.JOIN.name() + " field");
					}
					if (!dataRecordTargets.subList(1, dataRecordTargets.size()).stream().allMatch(record -> record.hasValidator())) {
						throw new InputMismatchException("When there are multiple " + KW.DATA.name() + " clauses each after the first must include at least one " + KW.CONTAIN.name() + " field");
					}
				}
			}
			return dataRecordTargets;
		}

		private DataFixedFieldExpressionsTarget parseFixedDataFieldsTarget() throws IOException {

			DataFixedFieldExpressionsTarget fieldsTarget = new DataFixedFieldExpressionsTarget();

			do {
				if (tokenizer.skipWordIgnoreCase(KW.LINE.name())) {
					if (!tokenizer.skipWordIgnoreCase(KW.NUMBER.name())) {
						throw new InputMismatchException("Expecting " + KW.LINE.name() + " to be followed by " + KW.NUMBER.name());
					}
					tokenizer.skipWordIgnoreCase(KW.KEEP.name());
					boolean join = tokenizer.skipWordIgnoreCase(KW.JOIN.name());
					fieldsTarget.add(new LineNumberKeeperFixedFieldExpression(join));
				}
				else {
					FixedColumns columns = parseFixedColumns();
					if (tokenizer.skipWordIgnoreCase(KW.CONTAIN.name())) {
						Expression<String> validator = parseStringExpression();
						fieldsTarget.add(new ValidatorFixedFieldExpression(columns.startColumn, columns.endColumn, validator));
					}
					else {
						tokenizer.skipWordIgnoreCase(KW.KEEP.name());
						boolean join = tokenizer.skipWordIgnoreCase(KW.JOIN.name());
						fieldsTarget.add(new ColumnKeeperFixedFieldExpression(columns.startColumn, columns.endColumn, join));
					}
				}
			} while (tokenizer.skipDelimiter(","));

			DataTarget target = parseDataTarget(KW.READ.name(), KW.INTO.name(), false, false);
			fieldsTarget.setTarget(target);

			return fieldsTarget;
		}

		private List<FixedFieldExpressions> parseFixedTrailers() throws IOException {

			List<FixedFieldExpressions> trailers = new LinkedList<FixedFieldExpressions>();
			while (tokenizer.skipWordIgnoreCase(KW.TRAILER.name())) {
				trailers.add(parseFixedFields());
			}
			if (!trailers.isEmpty() && !trailers.get(0).hasValidator()) {
				throw new InputMismatchException("The first trailer must include at least one " + KW.CONTAIN.name() + " clause");
			}
			return trailers;
		}

		private FixedFieldExpressions parseFixedFields() throws IOException {

			FixedFieldExpressions fields = new FixedFieldExpressions();

			if (tokenizer.skipWordIgnoreCase(KW.IGNORE.name())) {
				return fields;
			}

			do {
				FixedColumns columns = parseFixedColumns();
				if (tokenizer.skipWordIgnoreCase(KW.CONTAIN.name())) {
					Expression<String> validator = parseStringExpression();
					fields.add(new ValidatorFixedFieldExpression(columns.startColumn, columns.endColumn, validator));
				}
				else if (tokenizer.skipWordIgnoreCase(KW.KEEP.name())) {
					VariableBase variable = parseVariableReference();
					fields.add(new SetterFixedFieldExpression(columns.startColumn, columns.endColumn, variable));
				}
				else {
					throw new InputMismatchException("Expecting " + KW.CONTAIN.name() + " or " + KW.KEEP.name() + ", found " + tokenizer.nextToken().getImage());
				}
			} while (tokenizer.skipDelimiter(","));

			return fields;
		}

		private FixedColumns parseFixedColumns() throws IOException {
			tokenizer.skipWordIgnoreCase(KW.COLUMNS.name());
			Expression<Integer> startColumn = parseIntegerExpression();
			Expression<Integer> endColumn = parseIntegerExpression();
			return new FixedColumns(startColumn, endColumn);
		}

		private class FixedColumns {
			Expression<Integer> startColumn;
			Expression<Integer> endColumn;

			public FixedColumns(Expression<Integer> startColumn, Expression<Integer> endColumn) {
				this.startColumn = startColumn;
				this.endColumn = endColumn;
			}
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
			if (!atEndOfTask()) {
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

	class MoveTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			boolean isBinary = tokenizer.skipWordIgnoreCase(KW.BINARY.name());
			if (!isBinary) {
				tokenizer.skipWordIgnoreCase(KW.ASCII.name());
			}

			tokenizer.skipWordIgnoreCase(KW.FROM.name());
			FtpConnection connection = parseFtpConnection();
			Expression<String> fromName = parseStringExpression();

			tokenizer.skipWordIgnoreCase(KW.TO.name());
			Expression<String> toName = parseStringExpression();

			return new MoveTask(prologue, isBinary, connection, fromName, toName);
		}
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

			List<Expression<String>> bcc = new LinkedList<Expression<String>>();
			if (tokenizer.skipWordIgnoreCase(KW.BCC.name())) {
				do {
					bcc.add(parseStringExpression());
				} while (tokenizer.skipDelimiter(","));
			}

			Expression<String> subject = null;
			if (tokenizer.skipWordIgnoreCase(KW.SUBJECT.name())) {
				subject = parseStringExpression();
			}

			Expression<String> body = null;
			boolean isHtml = false;
			if (tokenizer.skipWordIgnoreCase(KW.BODY.name())) {
				body = parseStringExpression();
				if (tokenizer.skipWordIgnoreCase(KW.TEXT.name())) {
					isHtml = false;
				}
				else if (tokenizer.skipWordIgnoreCase(KW.HTML.name())) {
					isHtml = true;
				}
			}

			List<Expression<String>> attachments = new LinkedList<Expression<String>>();
			if (tokenizer.skipWordIgnoreCase(KW.ATTACH.name()) || tokenizer.skipWordIgnoreCase(KW.ATTACHMENT.name())) {
				do {
					attachments.add(parseStringExpression());
				} while (tokenizer.skipDelimiter(","));
			}

			return new EmailTask(prologue, connection, from, to, cc, bcc, subject, body, isHtml, attachments);
		}
	}

	class DeleteTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			if (tokenizer.skipWordIgnoreCase(KW.SOURCE.name())) {
				return parseDeleteFiles(prologue, false);
			}
			else if (tokenizer.skipWordIgnoreCase(KW.TARGET.name())) {
				return parseDeleteFiles(prologue, true);
			}
			else {
				FtpConnection connection = parseFtpConnection();
				if (tokenizer.skipWordIgnoreCase(KW.FTP.name()) || (connection != null)) {
					return parseDeleteFtpFiles(prologue, connection);
				}
				else {
					return parseDeleteFiles(prologue, true);
				}
			}
		}

		private Task parseDeleteFiles(Task.Prologue prologue, boolean writeNotRead) throws IOException {

			List<Expression<String>> files = new LinkedList<Expression<String>>();
			do {
				files.add(parseStringExpression());
			} while (tokenizer.skipDelimiter(","));
			return new DeleteTask(prologue, files, writeNotRead);
		}

		private Task parseDeleteFtpFiles(Task.Prologue prologue, FtpConnection connection) throws IOException {

			List<Expression<String>> files = new LinkedList<Expression<String>>();
			do {
				files.add(parseStringExpression());
			} while (tokenizer.skipDelimiter(","));
			return new DeleteFtpTask(prologue, connection, files);
		}
	}

	class RenameTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			boolean ifExists = parseIfExists();
			tokenizer.skipWordIgnoreCase(KW.FROM.name());
			Expression<String> from = parseStringExpression();
			tokenizer.skipWordIgnoreCase(KW.TO.name());
			Expression<String> to = parseStringExpression();
			return new RenameTask(prologue, ifExists, from, to);
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

			List<Expression<String>> messages = new LinkedList<Expression<String>>();
			do {
				Expression<String> message = parseStringExpression();
				messages.add(message);
			} while (tokenizer.skipDelimiter(","));

			return new LogTask(prologue, messages);
		}
	}

	class GoTaskParser extends OptionalMessageTaskParser {
		@Override
		protected Task newTask(Task.Prologue prologue, Expression<String> message) {
			return new GoTask(prologue, message);
		}
	}

	class StopTaskParser extends OptionalMessageTaskParser {
		@Override
		protected Task newTask(Task.Prologue prologue, Expression<String> message) {
			return new StopTask(prologue, message);
		}
	}

	class FailTaskParser extends OptionalMessageTaskParser {
		@Override
		protected Task newTask(Task.Prologue prologue, Expression<String> message) {
			return new FailTask(prologue, message);
		}
	}

	class BreakTaskParser extends OptionalMessageTaskParser {

		@Override
		public Task parse(Task.Prologue prologue) throws IOException {

			if (states.size() == 1) {
				throw new InputMismatchException(KW.BREAK.name() + " may only appear in a looping construct");
			}
			return super.parse(prologue);
		}

		@Override
		protected Task newTask(Task.Prologue prologue, Expression<String> message) {
			return new BreakTask(prologue, message);
		}
	}

	abstract class OptionalMessageTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException {

			Expression<String> message = null;
			if (!atEndOfTask()) {
				message = parseStringExpression();
			}
			return newTask(prologue, message);
		}

		protected abstract Task newTask(Task.Prologue prologue, Expression<String> message);
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
			if (tokenizer.skipWordIgnoreCase(KW.WITH.name()) || !atEndOfTask()) {
				do {
					arguments.add(parseAnyExpression(true));
				} while (tokenizer.skipDelimiter(","));
			}

			if (isSynchronous) {
				return new SyncProcessTask(prologue, name, arguments, siblingProcesses);
			}
			else {
				return new AsyncProcessTask(prologue, name, arguments, siblingProcesses);
			}
		}
	}

	class DoTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue) throws IOException, NamingException {

			Expression<Boolean> whileCondition = null;
			if (tokenizer.skipWordIgnoreCase(KW.WHILE.name())) {
				whileCondition = parseBooleanExpression();
			}

			DoTask thisTask = new DoTask(prologue, whileCondition);

			return thisTask.setTaskSet(NestedTaskSet.parse(thisTaskSetParser, thisTask, KW.DO.name()));
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

			TaskSetParent thisTask;
			if (hasFileType(false)) {
				thisTask = parseForReadFile(prologue, variables);
			}
			else {
				thisTask  = parseForData(prologue, variables);
			}

			return thisTask.setTaskSet(NestedTaskSet.parse(thisTaskSetParser, (Task)thisTask, KW.FOR.name()));
		}

		private ForReadTask parseForReadFile(
				Task.Prologue prologue,
				ArrayList<VariableBase> variables) throws IOException, NamingException {

			FileHandler handler = parseFileHandler(false);

			PageIdentifierExpression page = parsePageIdentifier(handler, false);

			PageOptions options = parseFileOptions(false, handler);

			SourceHeaderExpressions headers = parseSourceHeaders(KW.TASK.name());

			ColumnExpressions columns = parseColumns();

			columns.validate(headers);

			return new ForReadTask(prologue, variables, page, options, headers, columns);
		}

		private ForDataTask parseForData(
				Task.Prologue prologue,
				ArrayList<VariableBase> variables) throws IOException, NamingException {

			ArrayList<VariableType> columnTypes = variables
					.stream()
					.map(v -> v.getType())
					.collect(Collectors.toCollection(ArrayList::new));
			Source dataSource = parseSource(KW.FOR.name(), null, false, true, columnTypes);

			return new ForDataTask(prologue, variables, dataSource);
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

			OnScheduleTask thisTask = new OnScheduleTask(prologue, schedule, connections);

			return thisTask.setTaskSet(NestedTaskSet.parse(thisTaskSetParser, thisTask, KW.ON.name()));
		}

		private Task parseOn(Task.Prologue prologue) throws IOException, NamingException {

			ScheduleSet schedules = ScheduleSet.parse(tokenizer);

			OnTask thisTask = new OnTask(prologue, schedules, connections);

			return thisTask.setTaskSet(NestedTaskSet.parse(thisTaskSetParser, thisTask, KW.ON.name()));
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
			if (tokenizer.skipWordIgnoreCase(KW.ASYNC.name())) {
				return parseWaitforAsync(prologue);
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

		private Task parseWaitforAsync(Task.Prologue prologue) throws IOException, NamingException {

			GoTask thisTask = new GoTask(prologue, null);

			if (!updateAsyncPredecessors(thisTask, thisTask, false)) {
				throw new InputMismatchException(
						KW.WAITFOR.name() + " " + KW.ASYNC.name() + " " + KW.TASK.name() +
						" must have direct or indirect predecessor or predecessor descendent that is " +
						KW.PROCESS.name() + " " + KW.ASYNC.name() + " " + KW.TASK.name());
			}

			return thisTask;
		}

		/**
		 * Find all AsyncProcessTask instances that are direct and indirect predecessors of this task
		 * and descendants of direct and indirect predecessors of this task.
		 * Call predecessor.addAsyncSuccessor(thisTask) on each such AsyncProcessTask instance.
		 * <p>
		 * Indirect predecessors are predecessors of direct predecessors of this task
		 * and direct or indirect predecessors of enclosing tasks of this task.
		 * <p>
		 * When this task runs, we can't allow any doubt about whether any AsyncProcessTask instances
		 * that it waits for have been launched or not.  If there is doubt, we can have race conditions.
		 * To avoid this, the path through direct and indirect predecessors to any AsyncProcessTask
		 * or its ancestor must be a pure AND path.
		 * <p>
		 * On the other hand, the path from an ancestor through descendants to an AsyncProcessTask
		 * may be any mix of AND or OR paths, because the ancestor cannot complete until all its
		 * descendants have either completed or become orphaned, which means any AsyncProcessTask
		 * that could be launched has been launched already.
		 * <p>
		 * @param waitTask is the WAITFOR ASYNC task
		 * @param task is waitTask or a direct or indirect predecessor
		 * @param updated is true if any AsyncProcessTask has been updated before this call
		 * @return true if any AsyncProcessTask has been updated before or by this call
		 */
		private boolean updateAsyncPredecessors(Task waitTask, Task task, boolean updated) {

			do {
				if (!task.getPredecessors().isEmpty()) {

					if (Expression.Combination.or.equals(task.getCombination())) {
						throw new InputMismatchException(
								KW.WAITFOR.name() + " " + KW.ASYNC.name() + " " + KW.TASK.name() +
								" or direct or indirect predecessor cannot combine predecessors with " + KW.OR.name());
					}

					for (Task predecessor : task.getPredecessors().keySet()) {
						if (predecessor instanceof AsyncProcessTask) {
							((AsyncProcessTask)predecessor).addAsyncSuccessor(waitTask);
							updated = true;
						}
						else {
							updated = updated || updateAsyncPredecessors(waitTask, predecessor, updated);
						}

						if (predecessor instanceof TaskSetParent) {
							updated = updated || updateNestedAsyncPredecessors(waitTask, (TaskSetParent)predecessor, updated);
						}
					}
				}
			}
			while ((task = task.getParent()) != null);

			return updated;
		}

		private boolean updateNestedAsyncPredecessors(Task waitTask, TaskSetParent parent, boolean updated) {

			for (Task task : parent.getTaskSet().getTasks().values()) {

				if (task instanceof AsyncProcessTask) {
					((AsyncProcessTask)task).addAsyncSuccessor(waitTask);
					updated = true;
				}

				if (task instanceof TaskSetParent) {
					updated = updated || updateNestedAsyncPredecessors(waitTask, (TaskSetParent)task, updated);
				}
			}

			return updated;
		}
	}

	class ConnectTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue)
				throws InputMismatchException, NoSuchElementException, IOException, NamingException {

			ConnectionReference reference = parseConnectionReference();

			if (tokenizer.skipWordIgnoreCase(KW.USING.name())) {
				return parseConnect(prologue, reference, (p, c, i, a) -> new ConnectUsingTask(p, c, i, a));
			}
			else if (tokenizer.skipWordIgnoreCase(KW.TO.name()) || !atEndOfTask()) {
				return parseConnect(prologue, reference, (p, c, i, a) -> new ConnectToTask(p, c, i, a));
			}
			else {
				return new ConnectDefaultTask(prologue, reference);
			}
		}

		private Task parseConnect(Task.Prologue prologue, ConnectionReference reference, Connector connector)
				throws InputMismatchException, NoSuchElementException, IOException, NamingException {

			boolean inherit = false;
			Expression<String> argument = null;

			if (tokenizer.skipWordIgnoreCase(KW.DEFAULT.name())) {
				inherit = true;
				if (tokenizer.skipWordIgnoreCase(KW.WITH.name())) {
					argument = parseStringExpression();
				}
				else {
					return new ConnectDefaultTask(prologue, reference);
				}
			}
			else {
				argument = parseStringExpression();
			}

			return connector.newTask(prologue, reference, inherit, argument);
		}
	}

	private ConnectionReference parseConnectionReference() throws InputMismatchException, NoSuchElementException, IOException {

		ConnectionReference reference = null;

		String name = tokenizer.nextWordUpperCase();
		if (name.equals(KW.DEFAULT.name())) {
			String typeName = tokenizer.nextWordUpperCase();
			reference = ConnectionReference.createDefault(typeName);
			if (reference == null) {
				throw new InputMismatchException("Expecting " + KW.CONNECTION.name() + " type name, found " + typeName);
			}
		}
		else {
			reference = ConnectionReference.createDefault(name);
			if (reference == null) {
				Connection connection = connections.get(name);
				if (connection == null) {
					throw new NoSuchElementException("Name not declared as a " + KW.CONNECTION.name() + ": " + name);
				}
				reference = ConnectionReference.create(connection);
			}
		}

		return reference;
	}

	@FunctionalInterface
	private static interface Connector {
		Task newTask(Task.Prologue prologue, ConnectionReference reference, boolean inherit, Expression<String> argument);
	}

	class RequestTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue)
				throws InputMismatchException, NoSuchElementException, IOException, NamingException {

			Expression<String> url = parseStringExpression();

			Expression<Integer> connectTimeout = parseTimeout(KW.CONNECT.name());
			Expression<Integer> socketTimeout = parseTimeout(KW.SOCKET.name());

			List<RequestTask.Header> headers = null;
			if (tokenizer.skipWordIgnoreCase(KW.HEADER.name())) {
				headers = parseRequestHeaders();
			}

			String request = tokenizer.nextWordUpperCase();

			RequestTaskTrailingParser parser = requestTaskParsers.get(request);

			if (parser == null) {
				throw new InputMismatchException("Expecting " + KW.REQUEST.name() + " type, found " + request);
			}

			return parser.parse(prologue, url, connectTimeout, socketTimeout, headers);
		}
	}

	private Expression<Integer> parseTimeout(String name) throws IOException {

		Expression<Integer> result = null;

		BacktrackingTokenizerMark mark = tokenizer.mark();
		if (tokenizer.skipWordIgnoreCase(name) && tokenizer.skipWordIgnoreCase(KW.TIMEOUT.name())) {
			result = parseIntegerExpression();
		}
		else {
			tokenizer.reset(mark);
		}

		return result;
	}

	private List<RequestTask.Header> parseRequestHeaders() throws IOException {

		List<RequestTask.Header> result = new LinkedList<RequestTask.Header>();
		do {
			Expression<String> name = parseStringExpression();
			boolean isNullable = tokenizer.skipWordIgnoreCase(KW.NULL.name());
			Expression<String> value = parseStringExpression();

			result.add(new RequestTask.Header(name, isNullable, value));
		} while (tokenizer.hasNextDelimiter(","));

		return result;
	}

	class GetRequestTaskParser extends RequestTaskTrailingParser {
		@Override
		protected Task newRequestTask(
				Prologue prologue,
				Expression<String> url,
				Expression<Integer> connectTimeout,
				Expression<Integer> socketTimeout,
				List<Header> headers,
				Expression<String> requestTemplate,
				List<SourceWithAliases> sourcesWithAliases,
				Expression<String> responseTemplate,
				List<TargetWithKeepers> targetsWithKeepers) {
			return new RequestGetTask(prologue, url, connectTimeout, socketTimeout, headers, sourcesWithAliases, responseTemplate, targetsWithKeepers);
		}
	}

	class DeleteRequestTaskParser extends RequestTaskTrailingParser {
		@Override
		protected Task newRequestTask(
				Prologue prologue,
				Expression<String> url,
				Expression<Integer> connectTimeout,
				Expression<Integer> socketTimeout,
				List<Header> headers,
				Expression<String> requestTemplate,
				List<SourceWithAliases> sourcesWithAliases,
				Expression<String> responseTemplate,
				List<TargetWithKeepers> targetsWithKeepers) {
			return new RequestDeleteTask(prologue, url, connectTimeout, socketTimeout, headers, sourcesWithAliases, responseTemplate, targetsWithKeepers);
		}
	}

	class PutRequestTaskParser extends RequestWithBodyTaskTrialingParser {
		@Override
		protected Task newRequestTask(
				Prologue prologue,
				Expression<String> url,
				Expression<Integer> connectTimeout,
				Expression<Integer> socketTimeout,
				List<Header> headers,
				Expression<String> requestTemplate,
				List<SourceWithAliases> sourcesWithAliases,
				Expression<String> responseTemplate,
				List<TargetWithKeepers> targetsWithKeepers) {
			return new RequestPutTask(prologue, url, connectTimeout, socketTimeout, headers, requestTemplate, sourcesWithAliases, responseTemplate, targetsWithKeepers);
		}
	}

	class PostRequestTaskParser extends RequestWithBodyTaskTrialingParser {
		@Override
		protected Task newRequestTask(
				Prologue prologue,
				Expression<String> url,
				Expression<Integer> connectTimeout,
				Expression<Integer> socketTimeout,
				List<Header> headers,
				Expression<String> requestTemplate,
				List<SourceWithAliases> sourcesWithAliases,
				Expression<String> responseTemplate,
				List<TargetWithKeepers> targetsWithKeepers) {
			return new RequestPostTask(prologue, url, connectTimeout, socketTimeout, headers, requestTemplate, sourcesWithAliases, responseTemplate, targetsWithKeepers);
		}
	}

	abstract class RequestTaskTrailingParser {

		public Task parse(
				Task.Prologue prologue, Expression<String> url,
				Expression<Integer> connectTimeout,
				Expression<Integer> socketTimeout,
				List<RequestTask.Header> headers) throws IOException {

			Expression<String> requestTemplate = parseRequestString();

			List<SourceWithAliases> sourcesWithAliases = new ArrayList<SourceWithAliases>();

			parseFromSourceWithAliases(sourcesWithAliases);

			parseJoinSourcesWithAliases(sourcesWithAliases);

			Expression<String> responseTemplate = null;
			List<TargetWithKeepers> targetsWithKeepers = new ArrayList<TargetWithKeepers>();

			if (tokenizer.skipWordIgnoreCase(KW.RESPONSE.name())) {

				responseTemplate = parseStringExpression();

				parseIntoTarget(targetsWithKeepers);

				parseJoinTargets(targetsWithKeepers);
			}

			return newRequestTask(prologue, url, connectTimeout, socketTimeout, headers, requestTemplate, sourcesWithAliases, responseTemplate, targetsWithKeepers);
		}

		protected Expression<String> parseRequestString() throws IOException { return null; }

		private void parseFromSourceWithAliases(List<SourceWithAliases> sourcesWithAliases) throws IOException {

			if (tokenizer.skipWordIgnoreCase(KW.FROM.name()) && !tokenizer.skipWordIgnoreCase(KW.NOTHING.name())) {
				parseSourceWithAliases(sourcesWithAliases);
			}
			else {
				addSingleEmptyRowSourceWithoutAliases(sourcesWithAliases);
			}
		}

		private void addSingleEmptyRowSourceWithoutAliases(List<SourceWithAliases> sourcesWithAliases) {

			ExpressionBase[] rowWithNoColumns = new ExpressionBase[0];
			List<ExpressionBase[]> values = new ArrayList<ExpressionBase[]>();
			values.add(rowWithNoColumns);
			ValuesSource source = new ValuesSource(values);

			sourcesWithAliases.add(new SourceWithAliases(source, null));
		}

		protected void parseSourceWithAliases(List<SourceWithAliases> sourcesWithAliases) throws IOException {

			Source source = parseSource(KW.REQUEST.name(), null, false, true, null);
			List<Expression<String>> columnNameAliases = parseAliases(KW.AS.name(), false);

			sourcesWithAliases.add(new SourceWithAliases(source, columnNameAliases));
		}

		private List<Expression<String>> parseAliases(String introWord, boolean required) throws IOException {

			if (required) {
				skipRequiredWordIgnoreCase(introWord);
			}
			else if (!tokenizer.skipWordIgnoreCase(introWord)) {
				return null;
			}

			List<Expression<String>> aliases = new ArrayList<Expression<String>>();
			do {
				Expression<String> alias = parseStringExpression();
				aliases.add(alias);
			} while (tokenizer.skipDelimiter(","));

			return aliases;
		}

		protected void parseJoinSourcesWithAliases(List<SourceWithAliases> sourcesWithAliases) throws IOException {}

		private void parseIntoTarget(List<TargetWithKeepers> targetsWithKeepers) throws InputMismatchException, IOException {

			TargetWithKeepers targetWithKeepers = null;

			List<Expression<String>> targetIdentifiers = parseAliases(KW.KEEP.name(), false);
			if (targetIdentifiers != null) {

				DataTarget target = parseDataTarget(KW.REQUEST.name(), KW.INTO.name(), true, false);

				targetWithKeepers = new TargetWithKeepers(target, targetIdentifiers);
			}
			else {
				skipRequiredWordIgnoreCase(KW.INTO.name());
				skipRequiredWordIgnoreCase(KW.NOTHING.name());
			}

			targetsWithKeepers.add(targetWithKeepers);
		}

		private void parseJoinTargets(List<TargetWithKeepers> targetsWithKeepers) throws InputMismatchException, IOException {

			List<Expression<String>> targetIdentifiers;
			while ((targetIdentifiers = parseAliases(KW.KEEP.name(), false)) != null) {

				skipRequiredWordIgnoreCase(KW.JOIN.name());

				DataTarget target = parseDataTarget(KW.REQUEST.name(), null, true, false);

				targetsWithKeepers.add(new TargetWithKeepers(target, targetIdentifiers));
			}
		}

		protected void skipRequiredWordIgnoreCase(String word) throws InputMismatchException, IOException {
			if (!tokenizer.skipWordIgnoreCase(word)) {
				throw new InputMismatchException("Expecting " + word + ", found " + tokenizer.nextToken().getImage());
			}
		}

		protected abstract Task newRequestTask(
				Prologue prologue,
				Expression<String> url,
				Expression<Integer> connectTimeout,
				Expression<Integer> socketTimeout,
				List<Header> headers,
				Expression<String> requestTemplate,
				List<SourceWithAliases> sourcesWithAliases,
				Expression<String> responseTemplate,
				List<TargetWithKeepers> targetsWithKeepers);
	}

	abstract class RequestWithBodyTaskTrialingParser extends RequestTaskTrailingParser {

		@Override
		protected Expression<String> parseRequestString() throws IOException {
			return parseStringExpression();
		}

		@Override
		protected void parseJoinSourcesWithAliases(List<SourceWithAliases> sourcesWithAliases) throws IOException {

			while (tokenizer.skipWordIgnoreCase(KW.JOIN.name())) {
				parseSourceWithAliases(sourcesWithAliases);
			}
		}
	}

	class PromptTaskParser implements TaskParser {

		public Task parse(Task.Prologue prologue)
				throws InputMismatchException, NoSuchElementException, IOException, NamingException {

			Expression<String> prompt = parseStringExpression();

			boolean isPassword = tokenizer.skipWordIgnoreCase(KW.PASSWORD.name());

			tokenizer.skipWordIgnoreCase(KW.INTO.name());

			List<VariableBase> variables = new LinkedList<VariableBase>();
			do {
				variables.add(parseVariableReference());
			} while (tokenizer.skipDelimiter(","));

			return new PromptTask(prologue, prompt, isPassword, variables);
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

	private Source parseSource(
			String taskTypeName,
			String introWord,
			boolean singleRow,
			boolean allowTable,
			ArrayList<VariableType> columnTypes) throws IOException {

		if (introWord != null) {
			tokenizer.skipWordIgnoreCase(introWord);
		}

		if (tokenizer.skipWordIgnoreCase(KW.VALUES.name())) {
			return parseValuesSource(singleRow, columnTypes);
		}
		else if (tokenizer.skipWordIgnoreCase(KW.VARIABLE.name())) {
			return parseTableVariableSource();
		}
		else if (tokenizer.skipWordIgnoreCase(KW.PROPERTIES.name())) {
			return parsePropertiesSource(columnTypes);
		}
		else if (tokenizer.skipWordIgnoreCase(KW.FILES.name())) {
			return parseFileSource(null, false, columnTypes);
		}
		else if (tokenizer.skipWordIgnoreCase(KW.SOURCE.name())) {
			return parseFileSource(KW.FILES.name(), false, columnTypes);
		}
		else if (tokenizer.skipWordIgnoreCase(KW.TARGET.name())) {
			return parseFileSource(KW.FILES.name(), true, columnTypes);
		}
		else {
			Connection connection = parseOptionalConnection(null, null);

			if (connection != null) {
				if (connection instanceof DatabaseConnection) {
					return parseDataSource(taskTypeName, (DatabaseConnection)connection, singleRow, allowTable);
				}
				else if (connection instanceof FtpConnection) {
					return parseFtpSource((FtpConnection)connection, KW.FTP.name(), columnTypes);
				}
				else if (connection instanceof EmailConnection) {
					return parseEmailSource((EmailConnection)connection, KW.EMAIL.name(), columnTypes);
				}
				else {
					throw new InputMismatchException("A connection was specified that cannot be used in this context");
				}
			}
			else if (tokenizer.skipWordIgnoreCase(KW.FTP.name())) {
				return parseFtpSource(null, null, columnTypes);
			}
			else if (tokenizer.skipWordIgnoreCase(KW.EMAIL.name())) {
				return parseEmailSource(null, null, columnTypes);
			}
			return parseDataSource(taskTypeName, null, singleRow, allowTable);
		}
	}

	@SuppressWarnings("unchecked")
	private TableVariableSource parseTableVariableSource() throws InputMismatchException, NoSuchElementException, IOException {

		VariableBase variable = parseVariableReference();
		if (variable.getType() != VariableType.TABLE) {
			throw new InputMismatchException("A variable used as a " + KW.VARIABLE.name() + " data source must be of type " + KW.TABLE.name());
		}

		return new TableVariableSource((Variable<Table>)variable);
	}

	private ValuesSource parseValuesSource(boolean singleRow, ArrayList<VariableType> columnTypes) throws InputMismatchException, NoSuchElementException, IOException {

		List<ExpressionBase[]> values = new LinkedList<ExpressionBase[]>();

		ExpressionBase[] firstRow = parseExpressionList((columnTypes != null) ? columnTypes.size() : 0, columnTypes);
		values.add(firstRow);

		while (tokenizer.skipDelimiter(",")) {
			values.add(parseExpressionList(firstRow.length, columnTypes));
		}

		if (singleRow && (values.size() != 1)) {
			throw new InputMismatchException(KW.VALUES.name() + " clause cannot have more than one row in this context");
		}

		return new ValuesSource(values);
	}

	private ExpressionBase[] parseExpressionList(int columnCount, ArrayList<VariableType> columnTypes)
			throws InputMismatchException, NoSuchElementException, IOException {

		List<ExpressionBase> expressionList = new LinkedList<ExpressionBase>();
		tokenizer.nextDelimiter("(");

		int columnIndex = 0;
		do {
			ExpressionBase expression;
			if ((columnCount != 0) && (columnCount <= columnIndex)) {
				throw new InputMismatchException(KW.VALUES.name() + " clause has too many columns");
			}
			else if (columnTypes == null) {
				expression = parseAnyExpression();
			}
			else {
				expression = parseExpression(columnTypes.get(columnIndex));
			}
			expressionList.add(expression);
			++columnIndex;
		} while (tokenizer.skipDelimiter(","));

		tokenizer.nextDelimiter(")");

		if ((columnCount != 0) && (columnIndex < columnCount)) {
			throw new InputMismatchException(KW.VALUES.name() + " clause has " +
					((columnTypes != null) ? "too few" : "an inconsistent number of") + " columns");
		}

		return expressionList.stream().toArray(ExpressionBase[]::new);
	}

	private PropertiesSource parsePropertiesSource(ArrayList<VariableType> columnTypes)
			throws InputMismatchException, IOException {

		Expression<String> fileName = parseStringExpression();

		List<Expression<String>> propertyNames = new LinkedList<Expression<String>>();
		do {
			propertyNames.add(parseStringExpression());
		} while (tokenizer.skipDelimiter(","));

		return new PropertiesSource(fileName, propertyNames);
	}

	private Source parseFileSource(String introWord, boolean writeNotRead, ArrayList<VariableType> columnTypes) throws IOException {

		if (introWord != null) {
			tokenizer.skipWordIgnoreCase(introWord);
		}

		validateSingleVarcharColumn(KW.FILES.name(), columnTypes);

		Expression<String> fileNamePattern = parseStringExpression();

		return new FilesSource(fileNamePattern, writeNotRead);
	}

	private Source parseFtpSource(FtpConnection connection, String introWord, ArrayList<VariableType> columnTypes) throws IOException {

		if (introWord != null) {
			tokenizer.skipWordIgnoreCase(introWord);
		}

		validateSingleVarcharColumn(KW.FTP.name(), columnTypes);

		Expression<String> fileNamePattern = parseStringExpression();

		return new FtpSource(connection, fileNamePattern);
	}

	private void validateSingleVarcharColumn(String sourceTypeName, ArrayList<VariableType> columnTypes) {

		if ((columnTypes != null) && ((columnTypes.size() != 1) || (columnTypes.get(0) != VariableType.VARCHAR))) {
			throw new InputMismatchException(sourceTypeName + " source returns a single column of VARCHAR type");
		}
	}

	private Source parseEmailSource(EmailConnection connection, String introWord, ArrayList<VariableType> columnTypes) throws IOException {

		if (introWord != null) {
			tokenizer.skipWordIgnoreCase(introWord);
		}

		ArrayList<EmailSource.Field> fields = parseEmailSourceFields(columnTypes);

		tokenizer.skipWordIgnoreCase(KW.WHERE.name());

		EmailSource.Status status = emailSourceStatusParser.parsePhrase();
		if (status == null) {
			status = EmailSource.Status.all;
		}

		Expression<String> folder = null;
		if (tokenizer.skipWordIgnoreCase(KW.FOLDER.name())) {
			folder = parseStringExpression();
		}

		List<Expression<String>> senders = null;
		if (tokenizer.skipWordIgnoreCase(KW.SENDER.name())) {
			senders = new LinkedList<Expression<String>>();
			do {
				senders.add(parseStringExpression());
			} while (tokenizer.skipWordIgnoreCase(KW.OR.name()));
		}

		if ((folder == null) && (senders == null)) {
			throw new InputMismatchException("Either " + KW.FOLDER.name() + " or " + KW.SENDER.name() + " must be specified");
		}

		Expression<LocalDateTime> after = null;
		Expression<LocalDateTime> before = null;
		if (tokenizer.skipWordIgnoreCase(KW.RECEIVED.name())) {
			if (tokenizer.skipWordIgnoreCase(KW.AFTER.name())) {
				after = parseDatetimeExpression();
			}
			if (tokenizer.skipWordIgnoreCase(KW.BEFORE.name())) {
				before = parseDatetimeExpression();
			}
			if ((before == null) && (after == null)) {
				throw new InputMismatchException(KW.RECEIVED.name() + " must be followed by " + KW.BEFORE.name() + " and/or " + KW.AFTER.name());
			}
		}

		List<Expression<String>> subject = null;
		if (tokenizer.skipWordIgnoreCase(KW.SUBJECT.name())) {
			subject = parseStringContains();
		}

		List<Expression<String>> body = null;
		if (tokenizer.skipWordIgnoreCase(KW.BODY.name())) {
			body = parseStringContains();
		}

		List<Expression<String>> attachmentName = null;
		if (tokenizer.skipWordIgnoreCase(KW.ATTACHMENT.name())) {
			if (!tokenizer.skipWordIgnoreCase(KW.NAME.name())) {
				throw new InputMismatchException(KW.ATTACHMENT.name() + " must be followed by " + KW.NAME.name() + " in this context");
			}
			attachmentName = parseStringContains();
		}

		boolean detach = tokenizer.skipWordIgnoreCase(KW.DETACH.name());

		Expression<String> attachmentDirectory = null;
		if (detach && tokenizer.skipWordIgnoreCase(KW.TO.name())) {
			attachmentDirectory = parseStringExpression();
		}

		Boolean markReadNotUnread = null;
		if (tokenizer.skipWordIgnoreCase(KW.MARK.name())) {
			if (tokenizer.skipWordIgnoreCase(KW.READ.name())) {
				markReadNotUnread = true;
			}
			else if (tokenizer.skipWordIgnoreCase(KW.UNREAD.name())) {
				markReadNotUnread = false;
			}
			else {
				throw new InputMismatchException(KW.MARK.name() + " must be followed by " + KW.READ.name() + " or " + KW.UNREAD.name());
			}
		}

		Expression<String> targetFolder = null;
		if (tokenizer.skipWordIgnoreCase(KW.MOVE.name())) {
			tokenizer.skipWordIgnoreCase(KW.TO.name());
			targetFolder = parseStringExpression();
		}

		boolean delete = tokenizer.skipWordIgnoreCase(KW.DELETE.name());

		if ((targetFolder != null) && delete) {
			throw new InputMismatchException(KW.MOVE.name() + " and " + KW.DELETE.name() + " cannot both be specified");
		}

		return new EmailSource(
				connection,
				fields,
				status,
				folder,
				senders,
				after,
				before,
				subject,
				body,
				attachmentName,
				detach,
				attachmentDirectory,
				markReadNotUnread,
				targetFolder,
				delete);
	}

	private ArrayList<EmailSource.Field> parseEmailSourceFields(ArrayList<VariableType> columnTypes) throws IOException {

		tokenizer.skipWordIgnoreCase(KW.SELECT.name());

		ArrayList<EmailSource.Field> fields = new ArrayList<EmailSource.Field>();

		EmailSource.Field field = emailSourceFieldParser.parsePhrase();
		if (field != null) {
			for (int i = 0; ; ++i) {
				if (columnTypes != null) {
					if (columnTypes.size() <= i) {
						throw new InputMismatchException("More fields were specified than variables to hold them");
					}
					if (columnTypes.get(i) != EmailSource.typeOf(field)) {
						throw new InputMismatchException("Wrong variable type was specified for field #" + String.valueOf(i));
					}
				}

				fields.add(field);
				if (!tokenizer.skipDelimiter(",")) {
					break;
				}

				field = emailSourceFieldParser.parsePhrase();
				if (field == null) {
					throw new InputMismatchException("Expecting " + KW.EMAIL.name() + " " + KW.SOURCE.name() + " field name, found " + tokenizer.nextToken().getImage());
				}
			}
		}

		if ((columnTypes != null) && (fields.size() < columnTypes.size())) {
			throw new InputMismatchException("Fewer fields were specified than variables to hold them");
		}

		if (fields.contains(EmailSource.Field.count) && (1 < fields.size())) {
			throw new InputMismatchException(KW.COUNT.name() + " field cannot be combined with any other field");
		}

		return fields;
	}

	private class PhraseParser<T> {
		private String[][] wordsOfPhrases;
		private T[] values;

		public PhraseParser(String[] phrases, T[] values) {
			wordsOfPhrases = new String[phrases.length][];
			for (int i = 0; i < phrases.length; ++i) {
				wordsOfPhrases[i] = phrases[i].split(" ");
			}
			this.values = values;
		}

		public T parsePhrase() throws IOException {

			if (tokenizer.hasNextWord()) {
				BacktrackingTokenizerMark phraseMark = tokenizer.mark();

				for (int i = 0; i < wordsOfPhrases.length; ++i) {
					String[] wordsOfPhrase = wordsOfPhrases[i];
					boolean isMatch = true;
					for (int j = 0; j < wordsOfPhrase.length; ++j) {
						if (!tokenizer.skipWordIgnoreCase(wordsOfPhrase[j])) {
							isMatch = false;
							break;
						}
					}

					if (isMatch) {
						return values[i];
					}
					else {
						tokenizer.reset(phraseMark);
					}
				}
			}

			return null;
		}
	}

	private List<Expression<String>> parseStringContains() throws IOException {

		tokenizer.skipWordIgnoreCase(KW.CONTAINS.name());

		List<Expression<String>> result = new LinkedList<Expression<String>>();
		do {
			result.add(parseStringExpression());
		} while (tokenizer.skipWordIgnoreCase(KW.AND.name()));

		return result;
	}

	private DataSource parseDataSource(String taskTypeName, DatabaseConnection connection, boolean singleRow, boolean allowTable) throws IOException {

		if (tokenizer.skipWordIgnoreCase(KW.STATEMENT.name())) {
			return parseStatementDataSource(connection, singleRow);
		}
		else if (tokenizer.skipWordIgnoreCase(KW.SQL.name())) {
			return parseParameterizedStatementDataSource(connection, singleRow);
		}
		else if (tokenizer.skipWordIgnoreCase(KW.PROCEDURE.name())) {
			return parseProcedureDataSource(connection, singleRow);
		}
		else if (allowTable && tokenizer.skipWordIgnoreCase(KW.TABLE.name())) {
			return parseTableDataSource(connection, singleRow);
		}
		else {
			throw new InputMismatchException("Invalid data source in " + taskTypeName + " " + KW.TASK.name());
		}
	}

	private StatementDataSource parseStatementDataSource(DatabaseConnection connection, boolean singleRow) throws IOException {

		Expression<String> statement = parseStringExpression();

		return new StatementDataSource(connection, statement, singleRow);
	}

	private ParameterizedStatementDataSource parseParameterizedStatementDataSource(DatabaseConnection connection, boolean singleRow) throws IOException {

		List<ExpressionBase> expressions = new ArrayList<ExpressionBase>();
		StringBuilder statement = new StringBuilder();
		parseParameterizedStatement(expressions, statement);

		return new ParameterizedStatementDataSource(connection, expressions, statement.toString(), singleRow);
	}

	private TableDataSource parseTableDataSource(DatabaseConnection connection, boolean singleRow) throws IOException {

		Expression<String> table = parseStringExpression();

		return new TableDataSource(connection, table, singleRow);
	}

	private ProcedureDataSource parseProcedureDataSource(DatabaseConnection connection, boolean singleRow) throws IOException {

		Expression<String> procedure = parseStringExpression();

		List<DirectionalParam> params = new LinkedList<DirectionalParam>();
		if (hasNextArgument()) {
			do {
				params.add(parseArgument());
			} while (tokenizer.skipDelimiter(","));
		}

		VariableBase resultParam = null;
		if (tokenizer.skipWordIgnoreCase(KW.RETURNING.name())) {
			resultParam = parseVariableReference();
		}

		return new ProcedureDataSource(connection, resultParam, procedure, params, singleRow);
	}

	private boolean hasNextArgument() throws IOException {

		if (tokenizer.skipWordIgnoreCase(KW.WITH.name())) {
			return true;
		}
		else if (tokenizer.hasNextWordIgnoreCase(KW.RETURNING.name())) {
			return false;
		}
		else {
			return !atEndOfProcedure();
		}
	}

	private DirectionalParam parseArgument() throws IOException {

		ExpressionBase expression = parseAnyExpression();
		ParamDirection direction = parseDirection();

		if (
				(direction.equals(ParamDirection.OUT) || direction.equals(ParamDirection.INOUT)) &&
				!(expression instanceof Reference)) {

			throw new InputMismatchException(KW.OUT.name() + " or " + KW.INOUT.name() + " argument must be a variable");
		}

		return new DirectionalParam(expression, direction);
	}

	private ParamDirection parseDirection() throws IOException {

		if (tokenizer.skipWordIgnoreCase(KW.IN.name())) {
			return ParamDirection.IN;
		}
		else if (tokenizer.skipWordIgnoreCase(KW.OUT.name()) || tokenizer.skipWordIgnoreCase(KW.OUTPUT.name())) {
			return ParamDirection.OUT;
		}
		else if (tokenizer.skipWordIgnoreCase(KW.INOUT.name())) {
			return ParamDirection.INOUT;
		}
		else {
			return ParamDirection.IN;
		}
	}

	private DataTarget parseDataTarget(String taskTypeName, String introWord, boolean allowTable, boolean haveHeaders) throws IOException {

		DatabaseConnection connection = parseDatabaseConnection(introWord);

		Expression<Integer> batchSize = null;
		if (tokenizer.skipWordIgnoreCase(KW.BATCH.name())) {
			if (!tokenizer.skipWordIgnoreCase(KW.SIZE.name())) {
				throw new InputMismatchException("Expecting " + KW.BATCH.name() + " to be followed by " + KW.SIZE.name());
			}
			batchSize = parseIntegerExpression();
		}

		if (tokenizer.skipWordIgnoreCase(KW.STATEMENT.name())) {
			return parseStatementDataTarget(connection, batchSize);
		}
		else if (tokenizer.skipWordIgnoreCase(KW.SQL.name())) {
			return parseTokenizedStatementDataTarget(connection, batchSize);
		}
		else if (allowTable && tokenizer.skipWordIgnoreCase(KW.TABLE.name())) {
			return parseTableDataTarget(connection, batchSize, haveHeaders);
		}
		else {
			throw new InputMismatchException("Invalid data target in " + taskTypeName + " " + KW.TASK.name());
		}
	}

	private StatementDataTarget parseStatementDataTarget(DatabaseConnection connection, Expression<Integer> batchSize) throws IOException {

		Expression<String> statement = parseStringExpression();

		return new StatementDataTarget(connection, batchSize, statement);
	}

	private TokenizedStatementDataTarget parseTokenizedStatementDataTarget(DatabaseConnection connection, Expression<Integer> batchSize) throws IOException {

		StringBuilder statement = new StringBuilder();
		parseTokenizedStatement(statement);

		return new TokenizedStatementDataTarget(connection, batchSize, statement.toString());
	}

	private TableDataTarget parseTableDataTarget(DatabaseConnection connection, Expression<Integer> batchSize, boolean haveHeaders) throws IOException {

		if (!haveHeaders) {
			throw new RuntimeException(KW.READ.name() + " " + KW.INTO.name() + " " + KW.TABLE.name() + " requires column headers");
		}

		Expression<String> table = parseStringExpression();

		Expression<String> prefix = null;
		if (tokenizer.skipWordIgnoreCase(KW.PREFIX.name())) {
			tokenizer.skipWordIgnoreCase(KW.WITH.name());
			prefix = parseStringExpression();
		}

		return new TableDataTarget(connection, batchSize, table, prefix);
	}

	private VariableBase parseVariableReference() throws InputMismatchException, NoSuchElementException, IOException {

		String name = tokenizer.nextWordUpperCase();
		VariableBase variable = variables.get(name);
		if (variable == null) {
			throw new NoSuchElementException("Variable name not declared: " + name);
		}
		return variable;
	}

	private FileHandler parseFileHandler(boolean writeNotRead) throws IOException {

		String typeName = tokenizer.nextWordUpperCase();
		return FileHandler.get(typeName, writeNotRead);
	}

	private PageIdentifierExpression parsePageIdentifier(FileHandler handler) throws IOException {
		return parsePageIdentifier(handler, true);
	}

	private PageIdentifierExpression parsePageIdentifier(FileHandler handler, boolean isSheetNameRequired) throws IOException {

		Expression<String> filePath = parseStringExpression();
		if ((handler == null) || !handler.getHasSheets()) {
			return new FileIdentifierExpression(handler, filePath);
		}
		else {
			Expression<String> sheetName;
			if (isSheetNameRequired || !tokenizer.skipWordIgnoreCase(KW.SHEET.name())) {
				sheetName = parseStringExpression();
				tokenizer.skipWordIgnoreCase(KW.SHEET.name());
			}
			else {
				sheetName = new StringConstant("");
			}
			return new SheetIdentifierExpression(handler, filePath, sheetName);
		}
	}

	private PageOptions parseFileOptions(boolean writeNotRead, FileHandler handler) throws IOException {

		PageOptions options = null;
		PageOptions.Parser parser = handler.getOptionsParser(writeNotRead);
		if (parser != null) {
			options = parser.parse(this);
		}
		return options;
	}

	private TargetHeaderExpressions parseTargetHeaders(String wordAfterHeaders) throws IOException {

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

		return new TargetHeaderExpressions(!noHeaders, headersFromMetadata, captions);
	}

	private SourceHeaderExpressions parseSourceHeaders(String wordAfterHeaders) throws IOException {

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

		return new SourceHeaderExpressions(!noHeaders, validateHeaders, headersToMetadata, captions);
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

		while (!atEndOfSQL()) {
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
			throw new NoSuchElementException("Name not declared as a " + KW.CONNECTION.name() + ": " + name);
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
					if ((className != null) && (connection.getClass().getName() != className)) {
						throw new InputMismatchException("Connection is not " + typeName + " type");
					}
				}
			}
		}
		return connection;
	}

	private boolean parseIfExists() throws IOException {

		BacktrackingTokenizerMark mark = tokenizer.mark();
		if (tokenizer.skipWordIgnoreCase(KW.IF.name()) && tokenizer.skipWordIgnoreCase(KW.EXISTS.name())) {
			return true;
		}
		else {
			tokenizer.reset(mark);
			return false;
		}
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

	private boolean atEndOf(KW keyword) throws IOException {

		if (!tokenizer.hasNextWordIgnoreCase(KW.END.name())) {
			return false;
		}

		BacktrackingTokenizerMark mark = tokenizer.mark();
		tokenizer.nextToken();

		boolean result = tokenizer.hasNextWordIgnoreCase(keyword.name());

		tokenizer.reset(mark);
		return result;
	}

	/**
	 * @return true if tokenizer is positioned past the last token of a SQL token list.
	 * This can happen in three ways:
	 * <p>
	 * 1. "END TASK" is found; the tokenizer does not advance past any input; or<br>
	 * 2. "TASK {identifier}" is found; the tokenizer does not advance past any input; or<br>
	 * 3. "END SQL" is found; the tokenizer <b>is</b> advanced past "END SQL".
	 * @throws IOException
	 */
	private boolean atEndOfSQL() throws IOException {

		if (atEndOrStartOfLegacyTask()) {
			return true;
		}

		boolean result = atEndOf(KW.SQL);

		if (result) {
			tokenizer.nextToken();
			tokenizer.nextToken();
		}

		return result;
	}

	private ExpressionBase parseExpression(VariableType type)
			throws InputMismatchException, NoSuchElementException, IOException {
		return parseExpression(type, false);
	}

	private ExpressionBase parseExpression(VariableType type, boolean allowTable)
			throws InputMismatchException, NoSuchElementException, IOException {

		ExpressionBase expression = null;
		if (type == VariableType.INTEGER || type == VariableType.BIT) {
			expression = parseIntegerExpression();
		}
		else if (type == VariableType.VARCHAR) {
			expression = parseStringExpression();
		}
		else if (type == VariableType.DATETIME) {
			expression = parseDatetimeFromCompatibleExpression();
		}
		else if (allowTable && (type == VariableType.TABLE)) {
			expression = parseTableVariableExpression();
		}
		else {
			throw new InputMismatchException("Variable of type " + type.getName() + " is not supported in this context");
		}

		return expression;
	}

	private ExpressionBase parseAnyExpression() {
		return parseAnyExpression(false);
	}

	private ExpressionBase parseAnyExpression(boolean allowTable) {

		BacktrackingTokenizerMark mark = tokenizer.mark();

		try { return parseIntegerExpression(); }
		catch (Exception ex) { tokenizer.reset(mark); }

		try { return parseStringExpression(); }
		catch (Exception ex) { tokenizer.reset(mark); }

		try { return parseDatetimeExpression(); }
		catch (Exception ex) { tokenizer.reset(mark); }

		if (allowTable) {
			try { return parseTableVariableExpression(); }
			catch (Exception ex) { tokenizer.reset(mark); }
		}

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
		catch (Exception ex) { tokenizer.reset(mark);

			throw new InputMismatchException(Optional.ofNullable(ex.getMessage()).orElse(ex.getClass().getName()));
		}
	}

	private ExpressionBase parseFormatableExpression() {

		BacktrackingTokenizerMark mark = tokenizer.mark();

		try { return parseIntegerExpression(); }
		catch (Exception ex) { tokenizer.reset(mark); }

		try { return parseDatetimeExpression(); }
		catch (Exception ex) { tokenizer.reset(mark);

			throw new InputMismatchException(Optional.ofNullable(ex.getMessage()).orElse(ex.getClass().getName()));
		}
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

		ExpressionBase left = null;
		if (tokenizer.hasNextDelimiter("(")) {

			left = parseAnyParenthesizedExpression();
			if (left.getType() == VariableType.BOOLEAN) {
				return (Expression<Boolean>)left;
			}
		}
		else {
			left = parseAnyExpression();
		}

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
		else if ((left.getType() == VariableType.INTEGER) || (left.getType() == VariableType.BIT)) {
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

	private ExpressionBase parseAnyParenthesizedExpression() throws IOException {

		ExpressionBase result;
		try {
			result = parseAnyExpression();
		}
		catch (InputMismatchException ex) {

			tokenizer.nextDelimiter("(");

			result = parseBooleanExpression();

			tokenizer.nextDelimiter(")");
		}
		return result;
	}

	private Expression<Integer> parseIntegerExpression() throws IOException {

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
		else if (tokenizer.skipWordIgnoreCase(KW.CASE.name())) {
			return integerTermParser.parseCase();
		}
		else if (hasNextFunction()) {
			String name = tokenizer.nextWordUpperCase();

			FunctionParser<Integer> functionParser = integerFunctionParsers.get(name);
			if (functionParser != null) {

				tokenizer.nextDelimiter("(");
				Expression<Integer> result = functionParser.parse();
				tokenizer.nextDelimiter(")");

				return result;
			}
			else {
				throw new InputMismatchException("Unrecognized " + KW.INTEGER.name() + " function: " + name);
			}
		}
		else if (hasNextVariable()) {
			return integerTermParser.parseReference();
		}
		else if (tokenizer.skipDelimiter("(")) {
			return integerTermParser.parseParenthesized();
		}
		else {
			throw new InputMismatchException("Invalid " + KW.INTEGER.name() + " expression term: " + tokenizer.nextToken().getImage());
		}
	}

	private class DatePartParser implements FunctionParser<Integer> {

		public Expression<Integer> parse() throws InputMismatchException, NoSuchElementException, IOException {

			ChronoField field =
					tokenizer.skipWordIgnoreCase(KW.YEAR.name()) ? ChronoField.YEAR :
					tokenizer.skipWordIgnoreCase(KW.MONTH.name()) ? ChronoField.MONTH_OF_YEAR :
					tokenizer.skipWordIgnoreCase(KW.DAY.name()) ? ChronoField.DAY_OF_MONTH :
					tokenizer.skipWordIgnoreCase(KW.DAYOFYEAR.name()) ? ChronoField.DAY_OF_YEAR :
					tokenizer.skipWordIgnoreCase(KW.WEEKDAY.name()) ? ChronoField.DAY_OF_WEEK :
					tokenizer.skipWordIgnoreCase(KW.HOUR.name()) ? ChronoField.HOUR_OF_DAY :
					tokenizer.skipWordIgnoreCase(KW.MINUTE.name()) ? ChronoField.MINUTE_OF_HOUR :
					tokenizer.skipWordIgnoreCase(KW.SECOND.name()) ? ChronoField.SECOND_OF_MINUTE :
					null;
			if (field == null) {
				throw new InputMismatchException("Unrecognized date part name for " + KW.DATEPART.name());
			}

			tokenizer.nextDelimiter(",");
			Expression<LocalDateTime> datetime = parseDatetimeExpression();

			return new IntegerFromDatetime(datetime, field);
		}
	}

	private class CharIndexParser implements FunctionParser<Integer> {

		public Expression<Integer> parse() throws InputMismatchException, NoSuchElementException, IOException {

			Expression<String> toFind = parseStringExpression();
			tokenizer.nextDelimiter(",");
			Expression<String> toSearch = parseStringExpression();
			Expression<Integer> startIndex = null;
			if (tokenizer.skipDelimiter(",")) {
				startIndex = parseIntegerExpression();
			}

			return new CharIndex(toFind, toSearch, startIndex);
		}
	}

	private class LengthParser implements FunctionParser<Integer> {

		public Expression<Integer> parse() throws InputMismatchException, NoSuchElementException, IOException {

			Expression<String> string = parseStringExpression();

			return new Length(string);
		}
	}

	public Expression<String> parseStringExpression() throws IOException {

		Expression<String> left = parseStringTerm();
		while (tokenizer.skipDelimiter("+")) {
			Expression<String> right = parseStringTerm();
			left = new StringConcat(left, right);
		}
		return left;
	}

	private Expression<String> parseStringTerm()
			throws InputMismatchException, NoSuchElementException, IOException {

		if (tokenizer.hasNextQuoted()) {
			return new StringConstant(tokenizer.nextQuoted().getBody());
		}
		else if (tokenizer.skipWordIgnoreCase(KW.NULL.name())) {
			return new StringConstant(null);
		}
		else if (tokenizer.skipWordIgnoreCase(KW.CASE.name())) {
			return stringTermParser.parseCase();
		}
		else if (hasNextFunction()) {
			String name = tokenizer.nextWordUpperCase();

			FunctionParser<String> functionParser = stringFunctionParsers.get(name);
			if (functionParser != null) {

				tokenizer.nextDelimiter("(");
				Expression<String> result = functionParser.parse();
				tokenizer.nextDelimiter(")");

				return result;
			}
			else {
				throw new InputMismatchException("Unrecognized " + KW.VARCHAR.name() + " function: " + name);
			}
		}
		else if (hasNextVariable()) {
			return stringTermParser.parseReference();
		}
		else if (tokenizer.skipDelimiter("(")) {
			return stringTermParser.parseParenthesized();
		}
		else {
			throw new InputMismatchException("Invalid " + KW.VARCHAR.name() + " expression term: " + tokenizer.nextToken().getImage());
		}
	}

	private class FormatParser implements FunctionParser<String> {

		@SuppressWarnings("unchecked")
		public Expression<String> parse() throws InputMismatchException, NoSuchElementException, IOException {

			ExpressionBase source = parseFormatableExpression();
			tokenizer.nextDelimiter(",");
			Expression<String> format = parseStringExpression();

			if (source.getType() == VariableType.INTEGER || source.getType() == VariableType.BIT) {
				return new StringFromInteger((Expression<Integer>)source, format);
			}
			else if (source.getType() == VariableType.DATETIME) {
				return new StringFromDatetime((Expression<LocalDateTime>)source, format);
			}
			else {
				throw new InputMismatchException("Internal error - unsupported argument type for " + KW.FORMAT.name());
			}
		}
	}

	private class LeftParser implements FunctionParser<String> {

		public Expression<String> parse() throws InputMismatchException, NoSuchElementException, IOException {

			Expression<String> string = parseStringExpression();
			tokenizer.nextDelimiter(",");
			Expression<Integer> length = parseIntegerExpression();

			return new Left(string, length);
		}
	}

	private class LeftTrimParser implements FunctionParser<String> {

		public Expression<String> parse() throws InputMismatchException, NoSuchElementException, IOException {

			Expression<String> string = parseStringExpression();

			return new LeftTrim(string);
		}
	}

	private class LowerParser implements FunctionParser<String> {

		public Expression<String> parse() throws InputMismatchException, NoSuchElementException, IOException {

			Expression<String> string = parseStringExpression();

			return new Lower(string);
		}
	}

	private class ReplaceParser implements FunctionParser<String> {

		public Expression<String> parse() throws InputMismatchException, NoSuchElementException, IOException {

			Expression<String> string = parseStringExpression();
			tokenizer.nextDelimiter(",");
			Expression<String> pattern = parseStringExpression();
			tokenizer.nextDelimiter(",");
			Expression<String> replacement = parseStringExpression();

			return new Replace(string, pattern, replacement);
		}
	}

	private class ReplicateParser implements FunctionParser<String> {

		public Expression<String> parse() throws InputMismatchException, NoSuchElementException, IOException {

			Expression<String> string = parseStringExpression();
			tokenizer.nextDelimiter(",");
			Expression<Integer> repeats = parseIntegerExpression();

			return new Replicate(string, repeats);
		}
	}

	private class ReverseParser implements FunctionParser<String> {

		public Expression<String> parse() throws InputMismatchException, NoSuchElementException, IOException {

			Expression<String> string = parseStringExpression();

			return new Reverse(string);
		}
	}

	private class RightParser implements FunctionParser<String> {

		public Expression<String> parse() throws InputMismatchException, NoSuchElementException, IOException {

			Expression<String> string = parseStringExpression();
			tokenizer.nextDelimiter(",");
			Expression<Integer> length = parseIntegerExpression();

			return new Right(string, length);
		}
	}

	private class RightTrimParser implements FunctionParser<String> {

		public Expression<String> parse() throws InputMismatchException, NoSuchElementException, IOException {

			Expression<String> string = parseStringExpression();

			return new RightTrim(string);
		}
	}

	private class SpaceParser implements FunctionParser<String> {

		public Expression<String> parse() throws InputMismatchException, NoSuchElementException, IOException {

			Expression<Integer> repeats = parseIntegerExpression();

			return new Space(repeats);
		}
	}

	private class SubstringParser implements FunctionParser<String> {

		public Expression<String> parse() throws InputMismatchException, NoSuchElementException, IOException {

			Expression<String> string = parseStringExpression();
			tokenizer.nextDelimiter(",");
			Expression<Integer> start = parseIntegerExpression();
			tokenizer.nextDelimiter(",");
			Expression<Integer> length = parseIntegerExpression();

			return new Substring(string, start, length);
		}
	}

	private class UpperParser implements FunctionParser<String> {

		public Expression<String> parse() throws InputMismatchException, NoSuchElementException, IOException {

			Expression<String> string = parseStringExpression();

			return new Upper(string);
		}
	}

	private class CharParser implements FunctionParser<String> {

		public Expression<String> parse() throws InputMismatchException, NoSuchElementException, IOException {

			Expression<Integer> ascii = parseIntegerExpression();

			return new Char(ascii);
		}
	}

	private class Base64StringParser implements FunctionParser<String> {

		public Expression<String> parse() throws InputMismatchException, NoSuchElementException, IOException {

			Expression<String> string = parseStringExpression();

			return new Base64String(string);
		}
	}

	private class ErrorMessageParser implements FunctionParser<String> {

		public Expression<String> parse() throws InputMismatchException, NoSuchElementException, IOException {
			return new ErrorMessage(getTaskReference());
		}
	}

	private Expression<LocalDateTime> parseDatetimeExpression()
			throws InputMismatchException, NoSuchElementException, IOException {

		if (tokenizer.skipWordIgnoreCase(KW.NULL.name())) {
			return new DatetimeConstant(null);
		}
		else if (tokenizer.skipWordIgnoreCase(KW.CASE.name())) {
			return datetimeTermParser.parseCase();
		}
		else if (hasNextFunction()) {
			BacktrackingTokenizerMark mark = tokenizer.mark();
			String name = tokenizer.nextWordUpperCase();

			FunctionParser<LocalDateTime> functionParser = datetimeFunctionParsers.get(name);
			if (functionParser != null) {

				tokenizer.nextDelimiter("(");
				Expression<LocalDateTime> result = functionParser.parse();
				tokenizer.nextDelimiter(")");

				return result;
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
				return datetimeTermParser.parseReference();
			}
			else {
				throw new InputMismatchException("Invalid " + KW.DATETIME.name() + " expression term: " + tokenizer.nextToken().getImage());
			}
		}
	}

	private class GetDateParser implements FunctionParser<LocalDateTime> {

		public Expression<LocalDateTime> parse() throws InputMismatchException, NoSuchElementException, IOException {
			return new DatetimeNullary();
		}
	}

	private class DateAddParser implements FunctionParser<LocalDateTime> {

		public Expression<LocalDateTime> parse() throws InputMismatchException, NoSuchElementException, IOException {

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

			tokenizer.nextDelimiter(",");
			Expression<Integer> increment = parseIntegerExpression();
			tokenizer.nextDelimiter(",");
			Expression<LocalDateTime> datetime = parseDatetimeExpression();

			return new DatetimeAdd(datetime, unit, increment);
		}
	}

	private class DateFromPartsParser implements FunctionParser<LocalDateTime> {

		public Expression<LocalDateTime> parse() throws InputMismatchException, NoSuchElementException, IOException {

			Expression<Integer> year = parseIntegerExpression();
			tokenizer.nextDelimiter(",");
			Expression<Integer> month = parseIntegerExpression();
			tokenizer.nextDelimiter(",");
			Expression<Integer> day = parseIntegerExpression();

			return new DateFromParts(year, month, day);
		}
	}

	private class DateTimeFromPartsParser implements FunctionParser<LocalDateTime> {

		public Expression<LocalDateTime> parse() throws InputMismatchException, NoSuchElementException, IOException {

			Expression<Integer> year = parseIntegerExpression();
			tokenizer.nextDelimiter(",");
			Expression<Integer> month = parseIntegerExpression();
			tokenizer.nextDelimiter(",");
			Expression<Integer> day = parseIntegerExpression();
			// Next comma is optional for backward compatibility.
			tokenizer.skipDelimiter(",");
			Expression<Integer> hour = parseIntegerExpression();
			tokenizer.nextDelimiter(",");
			Expression<Integer> minute = parseIntegerExpression();
			tokenizer.nextDelimiter(",");
			Expression<Integer> seconds = parseIntegerExpression();
			tokenizer.nextDelimiter(",");
			Expression<Integer> milliseconds = parseIntegerExpression();

			return new DatetimeFromParts(year, month, day, hour, minute, seconds, milliseconds);
		}
	}

	private Expression<Table> parseTableVariableExpression()
			throws InputMismatchException, NoSuchElementException, IOException {

		if (tokenizer.skipWordIgnoreCase(KW.CASE.name())) {
			return tableTermParser.parseCase();
		}
		else if (hasNextFunction()) {
			String name = tokenizer.nextWordUpperCase();

			FunctionParser<Table> functionParser = tableFunctionParsers.get(name);
			if (functionParser != null) {

				tokenizer.nextDelimiter("(");
				Expression<Table> result = functionParser.parse();
				tokenizer.nextDelimiter(")");

				return result;
			}
			else {
				throw new InputMismatchException("Unrecognized " + KW.TABLE.name() + " function: " + name);
			}
		}
		else if (hasNextVariable()) {
			return tableTermParser.parseReference();
		}
		else {
			throw new InputMismatchException("Invalid " + KW.TABLE.name() + " expression term: " + tokenizer.nextToken().getImage());
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

	interface FunctionParser<Type> {
		Expression<Type> parse() throws InputMismatchException, NoSuchElementException, IOException;
	}

	private class IsNullParser<Type> implements FunctionParser<Type> {

		private TermParser<Type> termParser;

		public IsNullParser(TermParser<Type> termParser) {
			this.termParser = termParser;
		}

		@Override
		public Expression<Type> parse() throws InputMismatchException, NoSuchElementException, IOException {
			return termParser.parseIsNull();
		}
	}

	private class IfParser<Type> implements FunctionParser<Type> {

		private TermParser<Type> termParser;

		public IfParser(TermParser<Type> termParser) {
			this.termParser = termParser;
		}

		@Override
		public Expression<Type> parse() throws InputMismatchException, NoSuchElementException, IOException {
			return termParser.parseIf();
		}
	}

	private class ChooseParser<Type> implements FunctionParser<Type> {

		private TermParser<Type> termParser;

		public ChooseParser(TermParser<Type> termParser) {
			this.termParser = termParser;
		}

		@Override
		public Expression<Type> parse() throws InputMismatchException, NoSuchElementException, IOException {
			return termParser.parseChoose();
		}
	}

	private class CallParser<Type> implements FunctionParser<Type> {

		private TermParser<Type> termParser;

		public CallParser(TermParser<Type> termParser) {
			this.termParser = termParser;
		}

		@Override
		public Expression<Type> parse() throws InputMismatchException, NoSuchElementException, IOException {
			return termParser.parseCall();
		}
	}

	private abstract class TermParser<Type> {

		private VariableType type;

		public TermParser(VariableType type) {
			this.type = type;
		}

		public abstract Expression<Type> parseExpression() throws IOException;

		public Expression<Type> parseIsNull() throws IOException {

			Expression<Type> left = parseExpression();
			tokenizer.nextDelimiter(",");
			Expression<Type> right = parseExpression();

			return new NullReplace<Type>(left, right);
		}

		public Expression<Type> parseIf()
				throws InputMismatchException, NoSuchElementException, IOException {

			Expression<Boolean> condition = parseBooleanExpression();
			tokenizer.nextDelimiter(",");
			Expression<Type> left = parseExpression();
			tokenizer.nextDelimiter(",");
			Expression<Type> right = parseExpression();

			return new IfExpression<Type>(condition, left, right);
		}

		public Expression<Type> parseCase() throws IOException {

			Expression<Type> result;
			if (tokenizer.skipWordIgnoreCase(KW.WHEN.name())) {
				result = parseSearchedCase();
			}
			else {
				result = parseSimpleCase();
			}

			skipKeywordOrFail(KW.END);

			return result;
		}

		private Expression<Type> parseSearchedCase() throws IOException {

			List<Map.Entry<Expression<Boolean>, Expression<Type>>> whenClauses = new LinkedList<Map.Entry<Expression<Boolean>, Expression<Type>>>();

			do {
				Expression<Boolean> when = parseBooleanExpression();

				skipKeywordOrFail(KW.THEN);

				Expression<Type> result = parseExpression();

				whenClauses.add(new SimpleEntry<Expression<Boolean>, Expression<Type>>(when, result));

			} while (tokenizer.skipWordIgnoreCase(KW.WHEN.name()));


			Expression<Type> elseResult = null;
			if (tokenizer.skipWordIgnoreCase(KW.ELSE.name())) {
				elseResult = parseExpression();
			}

			return new SearchedCase<Type>(whenClauses, elseResult);
		}

		private Expression<Type> parseSimpleCase() throws InputMismatchException, NoSuchElementException, IOException {

			ExpressionBase input = parseAnyExpression();

			skipKeywordOrFail(KW.WHEN);

			List<Map.Entry<ExpressionBase, Expression<Type>>> whenClauses = new LinkedList<Map.Entry<ExpressionBase, Expression<Type>>>();

			do {
				ExpressionBase when = thisTaskSetParser.parseExpression(input.getType());

				skipKeywordOrFail(KW.THEN);

				Expression<Type> result = parseExpression();

				whenClauses.add(new SimpleEntry<ExpressionBase, Expression<Type>>(when, result));

			} while (tokenizer.skipWordIgnoreCase(KW.WHEN.name()));


			Expression<Type> elseResult = null;
			if (tokenizer.skipWordIgnoreCase(KW.ELSE.name())) {
				elseResult = parseExpression();
			}

			return new SimpleCase<Type>(input, whenClauses, elseResult);
		}

		private void skipKeywordOrFail(KW keyword) throws InputMismatchException, IOException {

			if (!tokenizer.skipWordIgnoreCase(keyword.name())) {
				throw new InputMismatchException("Expecting " + KW.WHEN.name() + ", found " + tokenizer.nextToken().getImage());
			}
		}

		public Expression<Type> parseChoose()
				throws InputMismatchException, NoSuchElementException, IOException {

			Expression<Integer> index = parseIntegerExpression();

			tokenizer.nextDelimiter(",");

			List<Expression<Type>> expressions = new ArrayList<Expression<Type>>();
			do {
				Expression<Type> expression = parseExpression();
				expressions.add(expression);
			} while (tokenizer.skipDelimiter(","));

			return new Choose<Type>(index, expressions);
		}

		public Expression<Type> parseCall()
				throws InputMismatchException, NoSuchElementException, IOException {

			VariableType returnType = parseType();
			if (returnType == null) {
				throw new InputMismatchException("A type name must be the first argument to the CALL function");
			}
			if (returnType != type) {
				throw new InputMismatchException("The return type specified in CALL cannot be used in this context");
			}
			tokenizer.nextDelimiter(",");
			Expression<String> className = parseStringExpression();
			tokenizer.nextDelimiter(",");
			Expression<String> methodName = parseStringExpression();

			List<ExpressionBase> expressions = new ArrayList<ExpressionBase>();
			while (tokenizer.skipDelimiter(",")) {
				ExpressionBase expression = parseAnyExpression(true);
				expressions.add(expression);
			}

			return new Call<Type>(type, className, methodName, expressions);
		}

		public Expression<Type> parseReference()
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
			if (!variable.getType().isCompatibleWith(type)) {
				throw new InputMismatchException("Variable has wrong type in " + type.getName() + " expression: " + name);
			}

			return (Variable<Type>)variable;
		}
	}

	/**
	 * Parse a data type
	 *
	 * @return the data type object
	 * @throws InputMismatchException
	 * @throws IOException
	 */
	protected VariableType parseType() throws InputMismatchException, IOException {

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
		else if (type.equals(KW.TABLE.name() )) {
			return VariableType.TABLE;
		}
		else {
			return null;
		}
	}
}
