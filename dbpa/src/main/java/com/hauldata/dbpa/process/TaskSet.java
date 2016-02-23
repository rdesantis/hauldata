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

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;

import com.hauldata.dbpa.task.Task;

/**
 * Executable task set base class
 */
abstract class TaskSet {

	protected Map<String, Task> tasks;

	protected TaskSet(Map<String, Task> tasks) {
		this.tasks = tasks;
	}

	protected void runTasks(Context context) throws InterruptedException {

		// Queue up the tasks that have no predecessors for execution.

		TaskExecutor executor = context.executor;
		List<Task> waiting = new LinkedList<Task>();

		for (Task task : tasks.values()) {
			if (task.getPredecessors().isEmpty()) {
			    executor.submit(task, context);
			}
			else {
				waiting.add(task);
			}
		}

		if (executor.allCompleted()) {
			throw new NoSuchElementException("Internal error - no task without predecessors");
		}

		try {
			// While any tasks are waiting on predecessors, process tasks as they complete.
	
			Task.Result lastResult = Task.Result.waiting;
	
			while (!waiting.isEmpty()) {
				Task task = executor.getCompleted();
	
				lastResult = task.getResult();
	
				List<Task> successors = task.getSuccessors();
				for (Task successor : successors) {
					if (waiting.contains(successor)) {
	
						if (successor.canRunAfterRemovePredecessor(task)) {
							waiting.remove(successor);
	
							executor.submit(successor, context);
						}
						else if (successor.getResult() == Task.Result.orphaned) {
							// This predecessor did not complete in the status required by this successor
							// and this successor has become orphaned, as may have some or all of its successors.
							// Remove all orphaned tasks from the waiting queue.
	
							for (ListIterator<Task> waitingIterator = waiting.listIterator(); waitingIterator.hasNext(); ) {
								Task waiter = waitingIterator.next();
								if (waiter.getResult() == Task.Result.orphaned) {
									context.log.write(waiter.getName(), "Task orphaned");
									waitingIterator.remove();
								}
							}
						}
					}
				}
			}
	
			// Tasks with no successors may still be running; wait for them to finish and check their results.
	
			while (!executor.allCompleted()) {
				Task task = executor.getCompleted();
	
				lastResult = task.getResult();
			}
	
			if (lastResult != Task.Result.success) {
				throw new RuntimeException("Last task failed");
			}
		}
		catch (InterruptedException iex) {
			// This thread was interrupted.  Cancel all tasks. 

			context.log.write("process", "Process interrupted");

			executor.terminateAll();

			while (!executor.allCompleted()) {
				Task task = executor.getCompleted();

				if (task.getResult() == Task.Result.waiting) {
					context.log.write(task.getName(), "Task cancelled");
				}
			}
			
			for (Task waiter : waiting) {
				context.log.write(waiter.getName(), "Task orphaned");
			}
			
			throw iex;
		}
	}
}
