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
import java.util.LinkedList;
import java.util.Map;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;

public abstract class Task {

	public static final String startMessage = "Starting task";
	public static final String succeedMessage = "Task succeeded";
	public static final String skipMessage = "Skipping task; run conditions not met";
	public static final String terminateMessage = "Task terminated";
	public static final String failMessage = "Task failed";

	public enum Result { waiting, running, success, failure, completed, terminated, orphaned };

	private String name;
	private Map<Task, Result> predecessors;
	private Expression.Combination combination;
	private Expression<Boolean> condition;
	private Task parent;
	private List<Task> successors;
	private Result result;

	/**
	 * Prologue to each task constructor
	 * @param name is the task name
	 * @param predecessors is a set of predecessor tasks mapped to their required completion states
	 * @param combination is the mode of combining predecessor task completion states to determine whether this task is ready to run, i.e., AND or OR
	 * @param condition is a boolean expression that must evaluate to true to enable this task's body to execute, or null if there is no such condition
	 */
	public static class Prologue {
		public String name;
		public Map<Task, Task.Result> predecessors;
		public Expression.Combination combination;
		public Expression<Boolean> condition;
		public Task parent;

		public Prologue(
				String name,
				Map<Task, Result> predecessors,
				Expression.Combination combination,
				Expression<Boolean> condition,
				Task parent) {
			this.name = name;
			this.predecessors = predecessors;
			this.combination = combination;
			this.condition = condition;
			this.parent = parent;
		}
	}

	/**
	 * Base class constructor
	 */
	protected Task(Prologue prologue) {
		this.name = prologue.name;
		this.predecessors = prologue.predecessors;
		this.combination = prologue.combination;
		this.condition = prologue.condition;
		this.parent = prologue.parent;

		this.successors = new LinkedList<Task>();
		this.result = Result.waiting;
	}

	// Getters and setters.

	public String getName() {
		return name;
	}

	public String getPath() {
		StringBuilder path = new StringBuilder();
		path.append(getName());

		Task node = this;
		while ((node = node.getParent()) != null) {
			path.insert(0, ".");
			path.insert(0, node.getName());
		}

		return path.toString();
	}

	public Task getParent() {
		return parent;
	}

	public void setNameFromIndex(int taskIndex) {
		this.name = String.valueOf(taskIndex);
	}

	// Predecessors

	public void setPredecessors(Map<Task, Result> predecessors) {
		this.predecessors = predecessors;
	}

	public void putPredecessor(Task predecessor, Result result) {
		predecessors.put(predecessor, result);
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

		try {
			if (condition == null || condition.evaluate()) {

				boolean isLogTask = this instanceof LogTask;
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

			result = Result.terminated;
		}
		catch (Exception ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();

			context.logger.error(name, message);
			context.logger.error(name, failMessage);

			result = Result.failure;
		}
	}

	/**
	 * Set the final task status.
	 */
	public void setFinalResult(Result result) {
		this.result = result;
	}

	/**
	 * Execute the task body.
	 *
	 * Subclasses must throw an exception to indicate task failure. Base class will catch the exception and mark the task with failure status.
	 * @param context is the context is which the task will run.
	 */
	protected abstract void execute(Context context) throws Exception;

	/**
	 * @return the completion result of the task.  Values are:
	 *
	 * waiting - the task has not yet run;
	 * success - the task ran to successful completion;
	 * failure - the task failed while running;
	 * terminated - the task was terminated by an interrupt
	 * orphaned - the task cannot run because predecessor(s) completed in state(s)
	 * that don't meet this task's requirements
	 */
	public Result getResult() {
		return result;
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

		Result requiredResult = predecessors.get(task);
		predecessors.remove(task);

		boolean canRun = false;
		if ((task.getResult() == requiredResult) || ((requiredResult == Result.completed) && (task.getResult() != Result.orphaned))) {
			// This predecessor completed in the status required by this successor.

			if (predecessors.isEmpty() || (combination == Expression.Combination.or)) {
				canRun = true;
			}
		}
		else {
			// This predecessor did not complete in the status required by this successor.

			if (predecessors.isEmpty() || (combination == Expression.Combination.and)) {
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
}
