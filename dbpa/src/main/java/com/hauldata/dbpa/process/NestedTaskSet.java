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
import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.naming.NamingException;

import com.hauldata.dbpa.task.Task;
import com.hauldata.dbpa.task.Task.Result;

/**
 * Set of tasks nested within a process or other set of tasks
 */
public class NestedTaskSet extends TaskSet {

	/**
	 * Instantiate a NestedTaskSet object by parsing at the parent parser hand-off
	 */
	public static NestedTaskSet parse(TaskSetParser parentParser, Task parentTask) throws IOException, NamingException {

		NestedTaskSetParser parser = new NestedTaskSetParser(parentParser, parentTask);
		
		return parser.parse();
	}

	/**
	 * Constructor with package visibility for use by NestedTaskSetParser
	 */
	NestedTaskSet(Map<String, Task> tasks) {
		super(tasks);
	}
	
	/**
	 * Run the tasks in the task set.
	 * Upon completion, the task set is altered in such a way that it cannot be run again.
	 * @param context is the context in which the task runs
	 * @throws Exception
	 * @see NestedTaskSet#runForRerun(Context)
	 */
	public void run(Context context) throws Exception {

		runTasks(context);
	}

	/**
	 * Run the tasks in the task set.
	 * Upon completion, the task set is restored to a state in which it can be run again.
	 * @param context is the context in which the task runs
	 * @throws Exception
	 * @see NestedTaskSet#run(Context)
	 */
	public void runForRerun(Context context) throws Exception {

		List<Map<Task, Result>> savedPredecessors = savePredecessors();

		runTasks(context);

		restorePredecessors(savedPredecessors);
	}

	private List<Map<Task, Result>> savePredecessors() {

		List<Map<Task, Result>> result = new LinkedList<Map<Task, Result>>();

		for (Task task : tasks.values()) {
			result.add(new HashMap<Task, Task.Result>(task.getPredecessors()));
		}

		return result; 
	}

	private void restorePredecessors(List<Map<Task, Result>> savedPredecessors) {

		ListIterator<Map<Task, Result>> predecessorsIterator = savedPredecessors.listIterator();

		for (Task task : tasks.values()) {
			task.setPredecessors(predecessorsIterator.next());
		}
	}
}

/**
 * Nested task set parser
 */
class NestedTaskSetParser extends TaskSetParser {

	private Task parentTask;

	public NestedTaskSetParser(TaskSetParser parent, Task parentTask) {
		super(parent);
		this.parentTask = parentTask;
	}

	/**
	 * Parse a nested set of tasks.
	 * 
	 * @throws IOException if physical read of the script fails
	 * @throws NoSuchElementException 
	 * @throws InputMismatchException 
	 * @throws NamingException 
	 * @throws RuntimeException if any syntax errors are encountered in the script
	 */
	public NestedTaskSet parse()
			throws IOException, InputMismatchException, NoSuchElementException, NamingException {

		parseTasks();
		return new NestedTaskSet(tasks);
	}

	@Override
	protected Task getParentTask() {
		return parentTask;
	}
}
