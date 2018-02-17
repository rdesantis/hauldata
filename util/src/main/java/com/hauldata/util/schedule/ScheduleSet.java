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

package com.hauldata.util.schedule;

import java.io.IOException;
import java.io.StringReader;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import com.hauldata.util.tokenizer.BacktrackingTokenizer;
import com.hauldata.util.tokenizer.BacktrackingTokenizerMark;

public class ScheduleSet implements ScheduleBase {

	private List<Schedule> schedules;
	private boolean immediate;

	/**
	 * Construct an empty schedule set.
	 * isImmediate() returns false until setImmediate(true) is called.
	 * The set is initially empty.  Use add() to add schedules to the set.
	 */
	public ScheduleSet() {
		schedules = new LinkedList<Schedule>();
		immediate = false;
	}

	/**
	 * @return true if the schedule set specifies immediate execution.
	 * It may also specify subsequent scheduled executions.
	 */
	public boolean isImmediate() {
		return immediate;
	}

	public void setImmediate(boolean immediate) {
		this.immediate = immediate;
	}

	public void add(Schedule schedule) {
		schedules.add(schedule);
	}
	
	@Override
	public LocalDateTime nextFrom(LocalDateTime earliest) {

		LocalDateTime soonestNext = null;
		for (Schedule schedule : schedules) {
			LocalDateTime next = schedule.nextFrom(earliest);
			if ((next != null) && ((soonestNext == null) || next.isBefore(soonestNext))) {
				soonestNext = next;
			}
		}
		return soonestNext;
	}

	@Override
	public ZonedDateTime nextFrom(ZonedDateTime earliest) {

		ZonedDateTime soonestNext = null;
		for (Schedule schedule : schedules) {
			ZonedDateTime next = schedule.nextFrom(earliest);
			if ((next != null) && ((soonestNext == null) || next.isBefore(soonestNext))) {
				soonestNext = next;
			}
		}
		return soonestNext;
	}

	/**
	 * @return the number of milliseconds until the next scheduled event
	 * or a negative number if no more events are scheduled.
	 */
	public long untilNext() {

		ZonedDateTime now = ZonedDateTime.now();
		ZonedDateTime wakeTime = nextFrom(now);
		if (wakeTime == null) {
			return -1;
		}
		return now.until(wakeTime, ChronoUnit.MILLIS);
	}

	/**
	 * Sleep until the next scheduled event.
	 * 
	 * @return false if no more events are scheduled, otherwise sleeps
	 * until at least one clock tick AFTER the date and time of the next scheduled event
	 * then returns true.  Waiting for an extra clock tick guarantees that the same
	 * scheduled event does not run more than once.  But this also means that if
	 * events are scheduled within a few clock ticks of each other, later events can be
	 * missed.
	 */
	public boolean sleepUntilNext() throws InterruptedException {

		ZonedDateTime wakeTime = nextFrom(ZonedDateTime.now());
		if (wakeTime == null) {
			return false;
		}

		// Sleep interval is not precise, so loop sleeping until clock
		// time advances past the wake time to guarantee the same schedule
		// does not run twice.

		Instant wakeInstant = wakeTime.toInstant();
		while (!Instant.now().isAfter(wakeInstant)) {

			long millis = Instant.now().until(wakeInstant, ChronoUnit.MILLIS);
			if (0 < millis) {
				Thread.sleep(millis);
			}
		}

		return true;
	}

	/**
	 * Instantiate a schedule set from a text string such as "EVERY DAY AT '10:30 AM' FROM '1/1/2016'".
	 * Schedules within the set are separated by commas.  The special schedule "TODAY NOW" causes
	 * isImmediate() to return true.
	 * 
	 * @param source is the text to parse
	 * @return the schedule set
	 * @throws RuntimeException if the text cannot be parsed
	 */
	public static ScheduleSet parse(String source) {

		ScheduleSet result;

		BacktrackingTokenizer tokenizer = new BacktrackingTokenizer(new StringReader(source));
		try {
			result = parse(tokenizer);
			if (tokenizer.hasNext()) {
				throw new RuntimeException("Unexpected tokens at the end of the schedule string");
			}
		}
		catch (IOException ex) {
			// This method doesn't do physical I/O so can't throw I/O exception.
			// If we get here somehow, re-throw it.
			throw new RuntimeException(Optional.ofNullable(ex.getMessage()).orElse(ex.getClass().getName()));
		}
		finally {
			try { tokenizer.close(); } catch (Exception ex) {}
		}

		return result;
	}

	/**
	 * Instantiate a schedule set by reading text from a tokenizer.  On return, the tokenizer
	 * is positioned at the next token past the schedule set definition.
	 * 
	 * @param tokenizer provides the text to parse
	 * @return the schedule set
	 * @throws RuntimeException if the text cannot be parsed
	 * @throws IOException
	 * @see ScheduleSet#parse(String)
	 */
	public static ScheduleSet parse(BacktrackingTokenizer tokenizer) throws IOException {

		ScheduleSet result = new ScheduleSet();

		do {
			if (skipIsImmediate(tokenizer)) {
				result.setImmediate(true);
			}
			else {
				result.add(Schedule.parse(tokenizer));
			}
		} while (tokenizer.skipDelimiter(","));
		
		return result;
	}

	private static boolean skipIsImmediate(BacktrackingTokenizer tokenizer) throws IOException {

		BacktrackingTokenizerMark mark = tokenizer.mark();

		if (tokenizer.skipWordIgnoreCase("TODAY") && tokenizer.skipWordIgnoreCase("NOW")) {
			return true;
		}

		tokenizer.reset(mark);

		return false;
	}
}
