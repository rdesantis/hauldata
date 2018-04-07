/*
 * Copyright (c) 2016 - 2018, Ronald DeSantis
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

package com.hauldata.dbpa.log;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public abstract class LoggerBase implements Logger {

	private String processId;
	private String taskPrefix;
	private List<Appender> appenders;
	private Level minLevel;
	
	protected LoggerBase(String processId, String taskPrefix, List<Appender> appenders) {
		this.processId = processId;
		this.taskPrefix = taskPrefix;
		this.appenders = appenders;

		minLevel = appenders.stream().map(a -> a.getLevel()).min(Level::compareTo).orElse(Level.info);
	}

	@Override
	public void addAppender(Appender appender) {
		appenders.add(appender);
	}

	private void write(Level level, String taskId, String message) {

		if (level.compareTo(minLevel) >= 0) {
			String resolvedProcessId = (processId == null) ? "" : processId;
			String resolvedTaskId = resolveTaskId(taskId);

			if (message.contains("\"")) {
				message = message.replace("\"", "\"\"");
			}

			for (Appender appender : appenders) {
				if (level.compareTo(appender.getLevel()) >= 0) {
					appender.log(resolvedProcessId, resolvedTaskId, LocalDateTime.now(), level.ordinal(), message);
				}
			}
		}
	}

	@Override
	public void info(String taskId, String message) {
		write(Level.info, taskId, message);
	}

	@Override
	public void warn(String taskId, String message) {
		write(Level.warn, taskId, message);
	}

	@Override
	public void error(String taskId, String message) {
		write(Level.error, taskId, message);
	}

	@Override
	public void message(String taskId, String message) {
		write(Level.message, taskId, message);
	}

	@Override
	public void error(String taskId, Exception exception) {

		String message = exception.getMessage();
		if (message == null) {
			message = exception.getClass().getName();

			Optional<StackTraceElement> element = Arrays.stream(exception.getStackTrace()).filter(e -> e.getClassName().startsWith("com.hauldata.dbpa")).findFirst();
			if (element.isPresent()) {
				message += " at " + element.get().toString();
			}
		}
		error(taskId, message);
	}

	@Override
	public Logger nestTask(String nestedTaskId) {

		return new NestedLogger(processId, resolveTaskId(nestedTaskId), appenders);
	}

	@Override
	public Logger nestProcess(String parentTaskId, String nestedProcessId) {

		String resolvedProcessId =
				((processId != null) ? processId : "") +
				"[" + resolveTaskId(parentTaskId) + "]" +
				"." + nestedProcessId;
		return new NestedLogger(resolvedProcessId, null, appenders);
	}

	private String resolveTaskId(String taskId) {
		return (taskPrefix != null) ? taskPrefix + "." + taskId : taskId;
	}

	protected void closeAppenders() {
		
		for (Appender appender : appenders) {
			appender.close();
		}
	}

	@Override
	public abstract void close();
}
