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

package com.hauldata.dbpa.task;

import java.util.List;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;

public abstract class Task {

	public static final String startMessage = "Starting task";
	public static final String succeedMessage = "Task succeeded";
	public static final String skipMessage = "Skipping task";
	public static final String terminateMessage = "Task terminated";
	public static final String failMessage = "Task failed";
	public static final String stopMessage = "Task thread stopped";
	public static final String breakingMessage = "Task breaking out of loop";

	public enum Result { waiting, running, success, failure, completed, terminated, orphaned, stopped, breaking };

	private String name;
	private Expression<String> qualifier;
	private Map<Task, Result> predecessors;
	private Expression.Combination combination;
	private Expression<Boolean> condition;
	private Task parent;
	private List<Task> successors;

	private Map<Task, Result> remainingPredecessors;
	private Exception exception;
	private Result result;
	private Object returnValue;

	/**
	 * Prologue to each task constructor
	 * @param name is the task name
	 * @param predecessors is a set of predecessor tasks mapped to their required completion states
	 * @param combination is the mode of combining predecessor task completion states to determine whether this task is ready to run, i.e., AND or OR
	 * @param condition is a boolean expression that must evaluate to true to enable this task's body to execute, or null if there is no such condition
	 */
	public static class Prologue {
		public String name;
		public Expression<String> qualifier;
		public Map<Task, Task.Result> predecessors;
		public Expression.Combination combination;
		public Expression<Boolean> condition;
		public Task parent;

		public Prologue(
				String name,
				Expression<String> qualifier,
				Map<Task, Result> predecessors,
				Expression.Combination combination,
				Expression<Boolean> condition,
				Task parent) {
			this.name = name;
			this.qualifier = qualifier;
			this.predecessors = predecessors;
			this.combination = combination;
			this.condition = condition;
			this.parent = parent;
		}
	}

	public static class Reference {
		public Task task;
	}

	public static class StoppedException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		private Object returnValue;

		public StoppedException() {
			returnValue = null;
		}

		public StoppedException(Object returnValue) {
			this.returnValue = returnValue;
		}

		public Object getReturnValue() {
			return returnValue;
		}
	}

	public static class BreakingException extends RuntimeException {
		private static final long serialVersionUID = 2L;
	}

	/**
	 * Base class constructor
	 */
	protected Task(Prologue prologue) {
		this.name = prologue.name;
		this.qualifier = prologue.qualifier;
		this.predecessors = prologue.predecessors;
		this.combination = prologue.combination;
		this.condition = prologue.condition;
		this.parent = prologue.parent;

		this.successors = new LinkedList<Task>();
		this.remainingPredecessors = new HashMap<Task, Task.Result>();
		this.result = null;
		this.returnValue = null;
	}

	// Getters and setters.

	/**
	 * @return the unadorned task name
	 */
	public String getTaskName() {
		return name;
	}

	public void setTaskNameFromIndex(int taskIndex, int lineNumber) {
		name = String.valueOf(taskIndex) + "@" + String.valueOf(lineNumber);
	}

	/**
	 * @return the adorned task name including the evaluated qualifier
	 */
	public String getName() {
		String qualifiedName = name;
		if (qualifier != null) {
			String evaluatedQualifier = qualifier.evaluate();
			qualifiedName = name + ":" + ((evaluatedQualifier != null) ? evaluatedQualifier : "");
		}
		return qualifiedName;
	}

	public Task getParent() {
		return parent;
	}

	// Predecessors

	public void setPredecessors(Map<Task, Result> predecessors) {
		this.predecessors = predecessors;
	}

	public void putRemainingPredecessor(Task predecessor, Result result) {
		synchronized (remainingPredecessors) {
			remainingPredecessors.put(predecessor, result);
		}
	}

	/**
	 * @return the set of predecessor tasks that must complete with their required result states
	 * before this one can start
	 */
	public Map<Task, Result> getPredecessors() {
		return predecessors;
	}

	public Expression.Combination getCombination() {
		return combination;
	}

	// Successors

	public void addSuccessor(Task task) {
		successors.add(task);
	}

	public List<Task> getSuccessors() {
		return successors;
	}

	/**
	 * Run the task.
	 *
	 * It is assumed that the predecessor completion requirements are met.
	 * If the task enable condition evaluates to false, the task completes immediately
	 * with success result.
	 *
	 * @param context is the context is which the task will run.
	 */
	public void run(Context context) {

		String name = this.name;
		try {
			name = getName();

			if (condition == null || condition.evaluate()) {

				boolean isLogTask = this instanceof LoggingTask;
				if (!isLogTask) {
					context.logger.info(name, startMessage);
				}

				result = Result.running;

				execute(context);

				if (!isLogTask) {
					context.logger.info(name, succeedMessage);
				}
			}
			else {
				context.logger.info(name, skipMessage);
			}
			result = Result.success;
		}
		catch (InterruptedException ex) {
			context.logger.error(name, terminateMessage);

			setAbnormalResult(ex, Result.terminated);
		}
		catch (StoppedException ex) {
			context.logger.warn(name, stopMessage);

			result = Result.stopped;
			returnValue = ex.getReturnValue();
		}
		catch (BreakingException ex) {
			context.logger.warn(name, breakingMessage);

			result = Result.breaking;
		}
		catch (Exception ex) {
			context.logger.error(name, ex);
			context.logger.error(name, failMessage);

			setAbnormalResult(ex, Result.failure);
		}
	}

	/**
	 * Set the initial state of the task.
	 * <p>
	 * The initial task state must account for the following scenarios:
	 * - A task may initially go into either the submitted queue or the waiting queue.
	 * - A task may be executed multiple times if it is contained in a looping structure.
	 * - A WAITFOR ASYNC task may have additional predecessors added after it has been initialized.
	 */
	public void initialize() {

		synchronized (remainingPredecessors) {
			remainingPredecessors.putAll(predecessors);
		}
		exception = null;
		result = Result.waiting;
	}

	/**
	 * Set the final status of a task that was not able to terminate normally.
	 */
	public void setAbnormalResult(Exception exception, Result result) {
		this.exception = exception;
		this.result = result;
	}

	/**
	 * Execute the task body.
	 *
	 * Implementations must throw an exception to indicate task failure. Caller will catch the exception and mark the task with failure status.
	 * The exception message will be written to the log.  If the exception does not have a message, the exception class name will be written.
	 * Generally implementations only need to catch and re-throw exceptions where they can add helpful troubleshooting information
	 * that is not provided in the original exception message.  For InterruptedException, the caller provides a standard message; there is no
	 * need to catch and re-throw this.
	 *
	 * @param context is the context is which the task will run.
	 */
	protected abstract void execute(Context context) throws Exception;

	/**
	 * @return the exception that terminated the task, or null if the task completed successfully
	 * or the task was orphaned
	 */
	public Exception getException() {
		return exception;
	}

	/**
	 * @return the completion result of the task.  Values are:<br>
	 *
	 * waiting - the task has not yet run;<br>
	 * success - the task ran to successful completion;<br>
	 * failure - the task failed while running;<br>
	 * terminated - the task was terminated by an interrupt;<br>
	 * orphaned - the task cannot run because predecessor(s) completed in state(s)
	 * that don't meet this task's requirements
	 */
	public Result getResult() {
		return result;
	}

	/**
	 * @return the return value of the task.  Only the RETURN task sets
	 * a return value.  For all others, this function returns null.
	 */
	public Object getReturnValue() {
		return returnValue;
	}

	/**
	 * Remove a completed predecessor and test if this task can run.
	 *
	 * If the predecessor is orphaned, this task may not ever be able to run,
	 * nor may its successors.
	 *
	 * On return, this task will have updated its result to orphaned
	 * if it can never run.  It will also have updated its successors
	 * if any of them can never run.
	 *
	 * @param task is the predecessor to remove
	 * @return true if this task can run after removing the predecessor
	 */
	public boolean canRunAfterRemovePredecessor(Task task) {

		Result requiredResult = remainingPredecessors.get(task);
		remainingPredecessors.remove(task);

		boolean canRun = false;
		if ((task.getResult() == requiredResult) || ((requiredResult == Result.completed) && task.completedNormally())) {
			// This predecessor completed in the status required by this successor.

			if (remainingPredecessors.isEmpty() || (combination == Expression.Combination.or)) {
				canRun = true;
			}
		}
		else {
			// This predecessor did not complete in the status required by this successor.

			if (remainingPredecessors.isEmpty() || (combination == Expression.Combination.and) || task.completedTerminally()) {
				// The successor can never run; it is orphaned.  Its successors may be too.

				result = Result.orphaned;
				for (Task successor : successors) {
					if (successor.getResult() == Result.waiting) {
						successor.canRunAfterRemovePredecessor(this);
					}
				}
			}
		}

		return canRun;
	}

	/**
	 * @return true if the task ended with status success or failure, false otherwise
	 */
	public boolean completedNormally() {
		return (result == Result.success) || (result == Result.failure);
	}

	/**
	 * @return true if the task ended with a status that forces its successors never to run, false otherwise
	 */
	public boolean completedTerminally() {
		return (result == Result.stopped);
	}
}
