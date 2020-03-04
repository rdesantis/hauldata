/*
 * Copyright (c) 2016, 2017, Ronald DeSantis
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
import java.util.Map;

import javax.naming.NamingException;

import com.hauldata.dbpa.task.Task;

/**
 * Set of tasks nested within a process or other set of tasks
 */
public class NestedTaskSet extends TaskSet {

	/**
	 * Instantiate a NestedTaskSet object by parsing at the parent parser hand-off
	 */
	public static NestedTaskSet parse(TaskSetParser parser, Task parentTask, String structureName) throws IOException, NamingException {

		Map<String, Task> tasks = parser.parseTasks(parentTask, structureName);

		return new NestedTaskSet(tasks);
	}

	private NestedTaskSet(Map<String, Task> tasks) {
		super(tasks);
	}

	/**
	 * Run the tasks in the task set.
	 * @param context is the context in which the task runs
	 * @throws Exception
	 */
	public void run(Context context) throws Exception {

		runTasks(context);
	}
}
