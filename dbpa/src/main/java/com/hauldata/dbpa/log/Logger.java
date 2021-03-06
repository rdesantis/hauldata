/*
 * Copyright (c) 2016, 2018, Ronald DeSantis
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

public interface Logger {

	enum Level { info, warn, error, message };

	void addAppender(Appender appender);

	void info(String taskId, String message);
	void warn(String taskId, String message);
	void error(String taskId, String message);
	void message(String taskId, String message);

	void error(String taskId, Exception exception);

	Logger nestTask(String nestedTaskId);

	Logger nestProcess(String taskId, String nestedProcessId);

	void close();
}
