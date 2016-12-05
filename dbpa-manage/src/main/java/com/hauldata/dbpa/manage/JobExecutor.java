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
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.hauldata.dbpa.manage_control.api.JobRun;
import com.hauldata.dbpa.manage_control.api.JobStatus;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.process.DbProcess;

/**
 * Concurrent job executor
 */
public class JobExecutor {

	// See http://stackoverflow.com/questions/3096842/wait-for-one-of-several-threads

	private ExecutorService es;
	private ExecutorCompletionService<JobRun> ecs;

	private static class SubmittedRun {

		public JobRun run;
		public Future<JobRun> future;

		public SubmittedRun(
				JobRun run,
				Future<JobRun> future) {
			this.run = run;
			this.future = future;
		}
	}

	private Map<Integer, SubmittedRun> submissions;

	private static class CallableJobRun implements Callable<JobRun> {

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

		submissions = new HashMap<Integer, SubmittedRun>();
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

		synchronized (submissions) {

			Future<JobRun> futureRun = ecs.submit(new CallableJobRun(process, args, context, run));

			submissions.put(run.getRunId(), new SubmittedRun(run, futureRun));
		}
	}

	/**
	 * Get the list of running jobs.
	 * <p>
	 * It is only guaranteed that the status of each job was JobRun.Status.runInProgress when this
	 * function was called.  Each job object in the list continues to be actively updated even after
	 * the function returns, so the status of a job may have changed when the caller examines it.
	 *
	 * @return the list of submitted jobs that are in progress
	 */
	public List<JobRun> getRunning() {
		synchronized (submissions) {
			return submissions.values().stream().map(s -> s.run).filter(r -> (r.getState().getStatus() == JobStatus.runInProgress)).collect(Collectors.toList());
		}
	}

	/**
	 * Get a running job.
	 *
	 * @param runId is the ID of the job run.
	 * @return the job run with the indicated ID if it is still being processed by the executor.
	 * @throws NoSuchElementException if the job is no longer being processed by the executor.
	 */
	public JobRun getRunning(int runId) {

		synchronized (submissions) {

			SubmittedRun submission = submissions.get(runId);

			if (submission == null) {
				throw new NoSuchElementException("Run is not in progress");
			}

			return submission.run;
		}
	}

	/**
	 * Get a job run that has completed; blocks until a job completes.
	 *
	 * @throws InterruptedException if the thread calling this function was interrupted
	 */
	public JobRun getCompleted() throws InterruptedException {

		Future<JobRun> completedFuture = ecs.take();

		JobRun completedRun = null;
		JobStatus exceptionStatus = null;

		synchronized (submissions) {

			try {
				completedRun = completedFuture.get();
			}
			catch (CancellationException cex) {
				exceptionStatus = JobStatus.runTerminated;
			}
			catch (ExecutionException eex) {
				exceptionStatus = JobStatus.runFailed;
			}

			if (completedRun == null) {

				// Job was cancelled before completion, possibly before it was started,
				// or failed by throwing an exception, therefore result could not be retrieved
				// from the completion queue.  Find the run for this future and update its status.

				Optional<SubmittedRun> possibleSubmission = submissions.values().stream().filter(s -> (s.future == completedFuture)).findFirst();

				if (possibleSubmission.isPresent()) {
					completedRun = possibleSubmission.get().run;
					completedRun.setEndStatusNow(exceptionStatus);
				}
			}

			submissions.remove(completedRun.getRunId());
		}

		return completedRun;
	}

	/**
	 * @return true if no incomplete jobs remain submitted
	 */
	public boolean allCompleted() {
		return submissions.isEmpty();
	}

	/**
	 * Stop a running job
	 *
	 * @param runId is the ID of the job run.
	 * @return true if the job could be stopped, false otherwise.
	 * @throws NoSuchElementException if the job is no longer being processed by the executor.
	 */
	public boolean stop(int runId) throws NoSuchElementException {

		synchronized (submissions) {

			SubmittedRun submission = submissions.get(runId);

			if (submission == null) {
				throw new NoSuchElementException("Run is not in progress");
			}

			return submission.future.cancel(true);
		}
	}

	/**
	 * Stop all running jobs
	 */
	public void stopAll() {

		synchronized (submissions) {
			for (SubmittedRun submission : submissions.values()) {
				submission.future.cancel(true);
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
