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

package com.hauldata.dbpa.control;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.hauldata.dbpa.control.ProcessRun.Status;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.process.DbProcess;

/**
 * Concurrent process executor
 */
public class ProcessExecutor {

	// See http://stackoverflow.com/questions/3096842/wait-for-one-of-several-threads

	private ExecutorService es;
	private ExecutorCompletionService<ProcessRun> ecs;

	private Map<ProcessRun, Future<ProcessRun>> runs;

	private class CallableProcessRun implements Callable<ProcessRun> {

		@Override
		public ProcessRun call() throws Exception {
			
			try {
				process.run(args, context);
				result.runSucceeded();
			}
			catch (InterruptedException iex) {
				result.runTerminated();
			}
			catch (Exception ex) {
				result.runFailed();
			}

			try { context.close(); } catch (Exception ex) {}

			return result;
		}

		public CallableProcessRun(
				DbProcess process,
				String[] args,
				Context context,
				ProcessRun result) {

			this.process = process;
			this.args = args;
			this.context = context;
			this.result = result;
		}

		private DbProcess process;
		private String[] args;
		private Context context;
		private ProcessRun result;
	}

	public ProcessExecutor() {

		es = Executors.newCachedThreadPool();
		ecs = new ExecutorCompletionService<ProcessRun>(es);
		
		// Note: Intentionally not using ConcurrentHashMap.  See comment in submit().
		runs = new HashMap<ProcessRun, Future<ProcessRun>>();
	}

	/**
	 * Submit a process for execution
	 */
	public void submit(
			ProcessRun run,
			DbProcess process,
			String[] args,
			Context context) {

		run.runInProgress();

		// There is a race condition where getCompleted() could pull the
		// completed process from the completion queue and attempt to remove
		// it from the runs map before submit() has added it to the map.
		// Using ConcurrentHashMap will not solve this problem.
		// Therefore, use regular HashMap but synchronize explicitly.

		synchronized (runs) {

			Future<ProcessRun> futureRun = ecs.submit(new CallableProcessRun(process, args, context, run));

			runs.put(run, futureRun);
		}
	}

	/**
	 * Get a process run that has completed; blocks until a process completes.
	 * 
	 * @throws InterruptedException if the thread calling this function was interrupted
	 */
	public ProcessRun getCompleted() throws InterruptedException {

		Future<ProcessRun> completedRun = ecs.take();

		ProcessRun result = null;
		Status exceptionStatus = null;

		synchronized (runs) {

			try {
				result = completedRun.get();
			}
			catch (CancellationException cex) {
				exceptionStatus = Status.runTerminated;
			}
			catch (ExecutionException eex) {
				exceptionStatus = Status.runFailed;
			}

			if (result == null) {

				// Process was cancelled before completion, possibly before it was started,
				// or failed by throwing an exception, therefore result could not be retrieved
				// from the completion queue.  Find the map entry for this result and update its status.

				for (Entry<ProcessRun, Future<ProcessRun>> entry : runs.entrySet()) {
					if (entry.getValue() == completedRun) {
	
						result = entry.getKey();
						result.setEndStatusNow(exceptionStatus);
						break;
					}
				}
			}

			runs.remove(result);
		}

		return result;
	}

	/**
	 * @return true if no incomplete processes remain submitted
	 */
	public boolean allCompleted() {
		return runs.isEmpty();
	}

	/**
	 * Stop a running process
	 */
	public boolean stop(ProcessRun run) {

		synchronized (runs) {

			Future<ProcessRun> futureRun = runs.get(run);

			if (futureRun == null) {
				throw new NoSuchElementException("Run is not in progress");
			}
	
			return futureRun.cancel(true);
		}
	}

	/**
	 * Stop all running processes
	 */
	public void stopAll() {

		synchronized (runs) {
			for (Future<ProcessRun> futureRun : runs.values()) {
				futureRun.cancel(true);
			}
		}
	}

	/**
	 * Close the executor
	 *
	 * To assure good behavior, getCompleted() should be called
	 * until all completed processes have been retrieved before
	 * closing the executor.
	 *
	 * If any processes have not completed, this function will wait a very
	 * short time for them to complete.
	 * 
	 * @return true if all processes and the executor terminate in a timely fashion. 
	 * @throws InterruptedException
	 */
	public boolean close() throws InterruptedException {

		es.shutdown();
		return es.awaitTermination(1, TimeUnit.MINUTES);
	}
}
