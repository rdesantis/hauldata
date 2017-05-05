/*
 * Copyright (c) 2017, Ronald DeSantis
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

package com.hauldata.dbpa.expression;


import java.util.HashSet;
import java.util.Set;

import com.hauldata.dbpa.task.Task;
import com.hauldata.dbpa.task.Task.Result;
import com.hauldata.dbpa.variable.VariableType;

public class ErrorMessage extends Expression<String> {

	private Task.Reference taskReference;

	public ErrorMessage(Task.Reference taskReference) {
		super(VariableType.VARCHAR);
		this.taskReference = taskReference;
	}

	@Override
	public String evaluate() {

		Exception exception = getPredecessorException(taskReference.task);
		return (exception != null) ? exception.getMessage() : null;
	}

	/**
	 * Find an exception that caused the failure of a direct or indirect predecessor of the indicated task.
	 *
	 * @param task is the task whose direct and indirect predecessors are checked
	 * @return the exception of the failed predecessor or null if no direct or indirect predecessor failed
	 */
	private Exception getPredecessorException(Task task) {
		
		// Do a breadth-first search for failed direct predecessors.

		Set<Task> previousPredecessors = task.getPredecessors().keySet();
		while (!previousPredecessors.isEmpty()) {

			Set<Task> predecessors = previousPredecessors;
			previousPredecessors = new HashSet<Task>();

			for (Task predecessor : predecessors) {
				if (predecessor.getResult() == Result.failure) {
					return predecessor.getException();
				}
				previousPredecessors.addAll(predecessor.getPredecessors().keySet());
			}
		}

		// No failed direct predecessors.  Check predecessors of the parent task.

		Task parent = task.getParent();
		return (parent != null) ? getPredecessorException(parent) : null;
	}
}
