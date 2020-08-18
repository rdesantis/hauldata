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
import com.hauldata.dbpa.task.Task.Result;

/**
 * Executable task set base class
 */
public abstract class TaskSet {

	public static final String failedMessage = "Last task failed";
	public static final String orphanedMessage = "Task orphaned";
	public static final String cancelledMessage = "Task cancelled";
	public static final String interruptedMessage = "Process interrupted";

	protected Map<String, Task> tasks;
	private Task failedTask;
	private Object returnValue;

	protected TaskSet(Map<String, Task> tasks) {
		this.tasks = tasks;
	}

	public Map<String, Task> getTasks() {
		return tasks;
	}

	public Task getFailedTask() {
		return failedTask;
	}

	public Object getReturnValue() {
		return returnValue;
	}

	protected void runTasks(Context context) throws InterruptedException {

		failedTask = null;
		returnValue = null;

		// Queue up the tasks that have no predecessors for execution.

		TaskExecutor executor = context.executor;
		List<Task> waiting = new LinkedList<Task>();

		for (Task task : tasks.values()) {
			task.initialize();

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

			AbnormalTermination termination = new AbnormalTermination();

			while (!waiting.isEmpty()) {
				Task task = executor.getCompleted();
				boolean isTerminalTask = true;

				List<Task> successors = task.getSuccessors();
				for (Task successor : successors) {
					if (waiting.contains(successor)) {

						if (successor.canRunAfterRemovePredecessor(task)) {

							waiting.remove(successor);

							executor.submit(successor, context);

							isTerminalTask = false;
						}
						else if (successor.getResult() == Task.Result.orphaned) {
							// This predecessor did not complete in the status required by this successor
							// and this successor has become orphaned, as may have some or all of its successors.
							// Remove all orphaned tasks from the waiting queue.

							for (ListIterator<Task> waitingIterator = waiting.listIterator(); waitingIterator.hasNext(); ) {
								Task waiter = waitingIterator.next();
								if (waiter.getResult() == Task.Result.orphaned) {
									context.logger.warn(waiter.getName(), orphanedMessage);
									waitingIterator.remove();
								}
							}
						}
						else {
							isTerminalTask = false;
						}
					}
				}

				if (isTerminalTask) {
					termination.check(task);
				}
			}

			// Tasks with no successors may still be running; wait for them to finish and check their results.

			while (!executor.allCompleted()) {
				Task task = executor.getCompleted();

				termination.check(task);
			}

			if (termination.failed) {
				throw new RuntimeException(failedMessage);
			}
			else if (termination.stopped) {
				throw new Task.StoppedException();
			}
			else if (termination.breaking) {
				throw new Task.BreakingException();
			}
		}
		catch (InterruptedException iex) {
			// This thread was interrupted.  Cancel all tasks.

			context.logger.error("process", interruptedMessage);

			executor.terminateAll();

			while (!executor.allCompleted()) {
				Task task = executor.getCompleted();

				if (task.getResult() == Task.Result.waiting) {
					context.logger.error(task.getName(), cancelledMessage);
				}
			}

			for (Task waiter : waiting) {
				context.logger.warn(waiter.getName(), orphanedMessage);
			}

			throw iex;
		}
	}

	private class AbnormalTermination {
		public boolean failed = false;
		public boolean stopped = false;
		public boolean breaking = false;

		public void check(Task task) {
			if (task.getResult() == Result.failure) {
				failed = true;
				failedTask = task;
			}
			else if (task.getResult() == Result.stopped) {
				stopped = true;
				returnValue = task.getReturnValue();
			}
			else if (task.getResult() == Result.breaking) {
				breaking = true;
			}
		}
	}
}
