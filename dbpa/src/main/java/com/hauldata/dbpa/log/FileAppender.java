/*
 * Copyright (c) 2016, 2017, Ronald DeSantis
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

package com.hauldata.dbpa.log;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.hauldata.dbpa.log.Logger.Level;
import com.hauldata.util.schedule.ScheduleSet;

public class FileAppender extends AppenderBase {

	private String fileName;
	private BufferedWriter writer;

	private ScheduleSet rollover;
	private Thread rolloverThread; 

	public FileAppender(String fileName, String rolloverSchedule, Level level) throws IOException {
		super(level);
		this.fileName = fileName;

		openFile(LocalDateTime.now());
		setupRollover(rolloverSchedule);
	}

	private void openFile(LocalDateTime now) throws IOException {

		// Replace each incidence of %d{pattern} in fileName with current date/time formatted to the pattern.

		final String regex = "%d\\{(.+?)\\}";
		final Pattern pattern = Pattern.compile(regex);

		StringBuffer stampedName = new StringBuffer();

		Matcher matcher = pattern.matcher(fileName);
		while (matcher.find()) {
			String stamp = now.format(DateTimeFormatter.ofPattern(matcher.group(1)));
			matcher.appendReplacement(stampedName, stamp);
		}
		matcher.appendTail(stampedName);

		// Open the log file, appending to existing if file name is not stamped or stamp has not changed.

		writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(stampedName.toString(), true)));
	}

	private void closeFile() {
		try {
			writer.close();
		}
		catch (Exception ex) {
		}
	}

	@Override
	public void log(String processId, String taskId, LocalDateTime datetime, int level, String message) {
		try {
			synchronized (writer) {
				writer.write(processId + "," + taskId + "," + datetime.toString() + "," + String.valueOf(level) + ",\"" + message + "\"" + String.format("%n"));
				writer.flush();
			}
		}
		catch (Exception ex) {
			// No good choice but to ignore the exception.  Don't want to crash the process just because logging is broken.
		}
	}

	private void setupRollover(String rolloverSchedule) throws IOException {

		rollover = null;
		rolloverThread = null; 

		if (rolloverSchedule == null || rolloverSchedule.isEmpty() || rolloverSchedule.equalsIgnoreCase("no") || rolloverSchedule.equalsIgnoreCase("none")) {
			return;
		}

		rollover = ScheduleSet.parse(rolloverSchedule);
		rolloverThread = new Thread(new RolloverThread()); 

		rolloverThread.start();
	}

	private class RolloverThread implements Runnable {

		@Override
		public void run() {
			try {
				while (!Thread.currentThread().isInterrupted()) {
					LocalDateTime rolloverTime = rollover.nextFrom(LocalDateTime.now());

					rollover.sleepUntilNext();

					synchronized (writer) {
						closeFile();
						openFile(rolloverTime);
					}
				}
			}
			catch (Exception ex) {
				// Expect InterruptedException when thread is terminated by cleanupRollover().
				// Terminate for any other exception as well.
			}
		}
	}

	private void cleanupRollover() {
		if (rolloverThread != null) {
			rolloverThread.interrupt();
		}
	}
	
	@Override
	public void close() {
		cleanupRollover();
		closeFile();
	}
}
