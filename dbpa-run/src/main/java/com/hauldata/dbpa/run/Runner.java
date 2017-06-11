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

package com.hauldata.dbpa.run;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Path;

import javax.naming.NamingException;

import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.process.ContextProperties;
import com.hauldata.dbpa.process.DbProcess;
import com.hauldata.util.schedule.ScheduleSet;
import com.hauldata.util.tokenizer.BacktrackingTokenizer;

public abstract class Runner {

	public static final String scheduleFileExt = "sch";

	public static Runner get(
			String processID,
			ContextProperties contextProps,
			String[] args,
			RunOptions options) throws IOException, NamingException {

		if (options.isCheckOnly() ) {
			return new TestRunner(processID, contextProps, args, options.isScheduled(), options.getScheduleName());
		}
		else if (options.isScheduled()) {
			return new ScheduledRunner(processID, contextProps, args, options.getScheduleName());
		}
		else {
			return new OneTimeRunner(processID, contextProps, args);
		}
	}

	protected Runner() {}

	public abstract void run() throws Exception;

	public abstract void close(int status);

	protected ScheduleSet getSchedule(Context context, String scheduleName) throws IOException {

		Path path = context.getSchedulePath(scheduleName + "." + scheduleFileExt);
		Reader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path.toString())));
		BacktrackingTokenizer tokenizer = new BacktrackingTokenizer(reader);
		return ScheduleSet.parse(tokenizer);
	}
}

abstract class ExecuteRunner extends Runner {

	protected String[] args;
	protected Context context =  null;
	protected DbProcess process = null;
	HookThread hookThread = null;

	protected ExecuteRunner(
			String processID,
			ContextProperties contextProps,
			String[] args) throws IOException, NamingException {

		if (processID == null) {
			throw new RuntimeException("No script name was specified");
		}

		this.args = args;

		try {
			context = contextProps.createContext(processID);

			process = context.loader.load(processID);

			hookThread = new HookThread();
		}
		catch (Exception ex) {
			try { if (context != null) context.close(); } catch (Exception exx) {}
			throw ex;
		}
	}

	@Override
	public void close(int status) {

		if ((hookThread != null) && !hookThread.isAlive()) {
			hookThread.unhook();
			hookThread = null;
		}

		try { if (context != null) context.close(); } catch (Exception ex) {}

		if (hookThread == null) {
			System.exit(status);
		}
	}
}

class HookThread extends Thread {

	private Thread processThread;

	public HookThread() {
		processThread = Thread.currentThread();
		Runtime.getRuntime().addShutdownHook(this);
	}

	@Override
	public void run() {
		processThread.interrupt();
		try {
			processThread.join();
		}
		catch (InterruptedException ex) {}
	}

	public void unhook() {
		Runtime.getRuntime().removeShutdownHook(this);
	}
}

class OneTimeRunner extends ExecuteRunner {

	protected OneTimeRunner(
			String processID,
			ContextProperties contextProps,
			String[] args) throws IOException, NamingException {
		super(processID, contextProps, args);
	}

	@Override
	public void run() throws Exception {
		process.run(args, context);
	}
}

class ScheduledRunner extends ExecuteRunner {

	private String processID;
	private ScheduleSet schedule;

	protected ScheduledRunner(
			String processID,
			ContextProperties contextProps,
			String[] args,
			String scheduleName) throws IOException, NamingException {
		super(processID, contextProps, args);
		this.processID = processID;

		schedule = getSchedule(context, scheduleName);
	}

	@Override
	public void run() throws Exception {

		if (schedule.isImmediate()) {
			process.run(args, context);
		}

		while (schedule.sleepUntilNext()) {

			process = context.loader.load(processID);

			process.run(args, context);
		}
	}
}

class TestRunner extends Runner {
	public TestRunner(
			String processID,
			ContextProperties contextProps,
			String[] args,
			boolean isScheduled,
			String scheduleName) throws IOException, NamingException {

		Context context = null;
		try {
			context = contextProps.createContext(processID);

			if (processID != null) {
				DbProcess process = context.loader.load(processID);
				process.validate(args);
			}

			if (isScheduled) {
				getSchedule(context, scheduleName);
			}
		}
		finally {
			try { if (context != null) context.close(); } catch (Exception ex) {}
		}
	}

	@Override
	public void run() {}

	@Override
	public void close(int status) {
		System.exit(status);
	}
}
