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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.hauldata.dbpa.task.Task;
import com.hauldata.dbpa.task.Task.Result;

/**
 * Concurrent task executor
 */
public class TaskExecutor {

	// See http://stackoverflow.com/questions/3096842/wait-for-one-of-several-threads

	private ExecutorService es;
	private ExecutorCompletionService<Task> ecs;
	private Map<Task, Future<Task>> submissions;

	private class CallableTask implements Callable<Task> {

		@Override
		public Task call() throws Exception {
			task.run(context);
			return task;
		}

		public CallableTask(Task task, Context context) {
			this.task = task;
			this.context = context;
		}

		private Task task;
		private Context context;
	}

	public TaskExecutor() {
		es = null;
		ecs = null;
		submissions = null;
	}

	private ExecutorCompletionService<Task> getEcs() {
		if (ecs == null) {
			es = Executors.newCachedThreadPool();
			ecs = new ExecutorCompletionService<Task>(es);
			submissions = new HashMap<Task, Future<Task>>();
		}
		return ecs;
	}

	/**
	 * Submit a task for execution
	 */
	public void submit(Task task, Context context) {

		Future<Task> futureTask = getEcs().submit(new CallableTask(task, context));

		submissions.put(task, futureTask);
	}

	/**
	 * Get a task that has completed; blocks until a task completes.
	 * @return the completed task or null if no tasks remain
	 * @throws InterruptedException if this thread was interrupted while waiting
	 */
	public Task getCompleted() throws InterruptedException {

		if (allCompleted()) {
			return null;
		}

		Future<Task> completedTask = ecs.take();

		Task task = null;
		Result result = null;

		try {
			task = completedTask.get();
		}
		catch (CancellationException cex) {
			result = Result.terminated;
		}
		catch (ExecutionException eex) {
			result = Result.failure;
		}

		if (task == null) {

			// Task was cancelled before completion, possibly before it was started,
			// or failed by throwing an exception, therefore result could not be retrieved
			// from the completion queue.  Find the map entry for this result and update its status.

			for (Entry<Task, Future<Task>> entry : submissions.entrySet()) {
				if (entry.getValue() == completedTask) {

					task = entry.getKey();
					task.setFinalResult(result);
					break;
				}
			}
		}

		submissions.remove(task);

		return task;
	}

	/**
	 * @return true if no incomplete tasks remain submitted
	 */
	public boolean allCompleted() {
		return (submissions == null) || submissions.isEmpty();
	}

	/**
	 * Terminate all submitted tasks.
	 */
	public void terminateAll() {

		if (!allCompleted()) {
			for (Future<Task> futureTask : submissions.values()) {
				futureTask.cancel(true);
			}
		}
	}

	/**
	 * Close the executor
	 *
	 * To assure good behavior, getCompleted() should be called
	 * until all completed tasks have been retrieved before
	 * closing the executor.
	 *
	 * If any tasks have not completed, this function will wait a very
	 * short time for them to complete.
	 * 
	 * @return true if all tasks and the executor terminate in a timely fashion. 
	 * @throws InterruptedException
	 */
	public boolean close() throws InterruptedException {
		if (es == null) {
			return true;
		}

		es.shutdown();
		return es.awaitTermination(1, TimeUnit.MINUTES);
	}
}
