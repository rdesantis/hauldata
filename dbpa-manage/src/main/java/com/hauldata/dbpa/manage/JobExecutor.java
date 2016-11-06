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

package com.hauldata.dbpa.manage;

import java.util.HashMap;
import java.util.List;
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
import java.util.stream.Collectors;

import com.hauldata.dbpa.manage.api.JobRun;
import com.hauldata.dbpa.manage.api.JobRun.Status;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.process.DbProcess;

/**
 * Concurrent job executor
 */
public class JobExecutor {

	// See http://stackoverflow.com/questions/3096842/wait-for-one-of-several-threads

	private ExecutorService es;
	private ExecutorCompletionService<JobRun> ecs;

	private Map<JobRun, Future<JobRun>> runs;
	private Map<Integer, JobRun> runsById;

	private class CallableJobRun implements Callable<JobRun> {

		private DbProcess process;
		private String[] args;
		private Context context;
		private JobRun result;

		@Override
		public JobRun call() throws Exception {

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

		public CallableJobRun(
				DbProcess process,
				String[] args,
				Context context,
				JobRun result) {

			this.process = process;
			this.args = args;
			this.context = context;
			this.result = result;
		}
	}

	public JobExecutor() {

		es = Executors.newCachedThreadPool();
		ecs = new ExecutorCompletionService<JobRun>(es);

		// Note: Intentionally not using ConcurrentHashMap.  See comment in submit().

		runs = new HashMap<JobRun, Future<JobRun>>();
		runsById = new HashMap<Integer, JobRun>();
	}

	/**
	 * Submit a job for execution
	 */
	public void submit(
			JobRun run,
			DbProcess process,
			String[] args,
			Context context) {

		run.runInProgress();

		// There is a race condition where getCompleted() could pull the
		// completed job from the completion queue and attempt to remove
		// it from the runs map before submit() has added it to the map.
		// Using ConcurrentHashMap will not solve this problem.
		// Therefore, use regular HashMap but synchronize explicitly.

		synchronized (runs) {

			Future<JobRun> futureRun = ecs.submit(new CallableJobRun(process, args, context, run));

			runs.put(run, futureRun);

			runsById.put(run.getRunId(), run);
		}
	}

	/**
	 * Get the list of running jobs.
	 * @return the list of submitted jobs that are in progress.
	 * It is guaranteed that the status of each is JobRun.Status.runInProgress.
	 */
	public List<JobRun> getRunning() {
		synchronized (runs) {
			return runs.keySet().stream().filter(e -> (e.getStatus() == JobRun.Status.runInProgress)).collect(Collectors.toList());
		}
	}

	/**
	 * Get a running job by ID.
	 * @return the job with the indicated ID if it is still being processed by the executor, otherwise null.
	 */
	public JobRun getRunning(int runId) {
		synchronized (runs) {
			return runsById.get(runId);
		}
	}

	/**
	 * Get a job run that has completed; blocks until a job completes.
	 * 
	 * @throws InterruptedException if the thread calling this function was interrupted
	 */
	public JobRun getCompleted() throws InterruptedException {

		Future<JobRun> completedRun = ecs.take();

		JobRun result = null;
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

				// Job was cancelled before completion, possibly before it was started,
				// or failed by throwing an exception, therefore result could not be retrieved
				// from the completion queue.  Find the map entry for this result and update its status.

				for (Entry<JobRun, Future<JobRun>> entry : runs.entrySet()) {
					if (entry.getValue() == completedRun) {
	
						result = entry.getKey();
						result.setEndStatusNow(exceptionStatus);
						break;
					}
				}
			}

			runsById.remove(result.getRunId());
			runs.remove(result);
		}

		return result;
	}

	/**
	 * @return true if no incomplete jobs remain submitted
	 */
	public boolean allCompleted() {
		return runs.isEmpty();
	}

	/**
	 * Stop a running job
	 */
	public boolean stop(JobRun run) {

		synchronized (runs) {

			Future<JobRun> futureRun = runs.get(run);

			if (futureRun == null) {
				throw new NoSuchElementException("Run is not in progress");
			}
	
			return futureRun.cancel(true);
		}
	}

	/**
	 * Stop all running jobs
	 */
	public void stopAll() {

		synchronized (runs) {
			for (Future<JobRun> futureRun : runs.values()) {
				futureRun.cancel(true);
			}
		}
	}

	/**
	 * Close the executor
	 *
	 * To assure good behavior, getCompleted() should be called
	 * until all completed jobs have been retrieved before
	 * closing the executor.
	 *
	 * If any jobs have not completed, this function will wait a very
	 * short time for them to complete.
	 * 
	 * @return true if all jobs and the executor terminate in a timely fashion. 
	 * @throws InterruptedException
	 */
	public boolean close() throws InterruptedException {

		es.shutdown();
		return es.awaitTermination(1, TimeUnit.MINUTES);
	}
}
