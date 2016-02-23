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

package com.hauldata.util.schedule;

import java.io.IOException;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashSet;
import java.util.InputMismatchException;
import java.util.NoSuchElementException;
import java.util.Set;

import com.hauldata.util.schedule.DateSchedule.LogicalDay;
import com.hauldata.util.tokenizer.BacktrackingTokenizer;
import com.hauldata.util.tokenizer.BacktrackingTokenizerMark;
import com.hauldata.util.tokenizer.Quoted;

public class Schedule implements ScheduleBase {

	private DateSchedule dateSchedule;
	private TimeSchedule timeSchedule;

	public Schedule(DateSchedule dateSchedule, TimeSchedule timeSchedule) {
		this.dateSchedule = dateSchedule;
		this.timeSchedule = timeSchedule;
	}

	@Override
	public LocalDateTime nextFrom(LocalDateTime earliest) {

		LocalDate earliestDate = earliest.toLocalDate();
		LocalTime resultTime = timeSchedule.nextFrom(earliest.toLocalTime());

		if (resultTime == null) {
			earliestDate = earliestDate.plusDays(1);
			resultTime = timeSchedule.nextFrom(LocalTime.MIN);
		}

		LocalDate resultDate = dateSchedule.nextFrom(earliestDate);
		if (resultDate == null) {
			return null;
		}

		return LocalDateTime.of(resultDate, resultTime);
	}

	/**
	 * Instantiate a schedule by reading text from a tokenizer.  On return, the tokenizer
	 * is positioned at the next token past the schedule definition.
	 * 
	 * @param tokenizer provides the text to parse
	 * @return the schedule
	 * @throws RuntimeException if the text cannot be parsed
	 * @throws IOException
	 */
	public static Schedule parse(BacktrackingTokenizer tokenizer) throws IOException {
		return (new ScheduleParser(tokenizer)).parse();
	}
}

class ScheduleParser {

	private static final String DATETIME = "DATETIME";
	private static final String DATE = "DATE";
	private static final String TODAY = "TODAY";
	private static final String AT = "AT";
	private static final String EVERY = "EVERY";
	private static final String WEEKDAY = "WEEKDAY";
	private static final String DAY = "DAY";
	private static final String WEEK = "WEEK";
	private static final String MONTH = "MONTH";
	private static final String HOUR = "HOUR";
	private static final String MINUTE = "MINUTE";
	private static final String SECOND = "SECOND";
	private static final String ON = "ON";
	private static final String FIRST = "FIRST";
	private static final String THIRD = "THIRD";
	private static final String FOURTH = "FOURTH";
	private static final String LAST = "LAST";
	private static final String FROM = "FROM";
	private static final String UNTIL = "UNTIL";

	BacktrackingTokenizer tokenizer;

	private static final Set<DayOfWeek> weekdays = new HashSet<DayOfWeek>(Arrays.asList((new DayOfWeek[]
			{DayOfWeek.MONDAY, DayOfWeek.TUESDAY, DayOfWeek.WEDNESDAY, DayOfWeek.THURSDAY, DayOfWeek.FRIDAY})));

	public ScheduleParser(BacktrackingTokenizer tokenizer) {
		this.tokenizer = tokenizer;
	}
	
	public Schedule parse() throws IOException {

		if (tokenizer.skipWordIgnoreCase(DATETIME)) {
			return parseDateTimeSchedule();
		}
		else if (tokenizer.skipWordIgnoreCase(DATE) || tokenizer.hasNextQuoted()) {
			return parseDateSchedule();
		}
		else if (tokenizer.skipWordIgnoreCase(TODAY)) {
			return parseDateSchedule(LocalDate.now());
		}
		else if (tokenizer.skipWordIgnoreCase(WEEKDAY + "S")) {
			return parseWeeklySchedule(1, weekdays);
		}
		else if (hasNextDayOfWeekIgnoreCase()) {
			return parseWeeklySchedule(1);
		}
		else if (tokenizer.skipWordIgnoreCase(EVERY)) {

			if (tokenizer.skipWordIgnoreCase(WEEKDAY)) {
				return parseWeeklySchedule(1, weekdays);
			}
			else if (hasNextDayOfWeekIgnoreCase()) {
				return parseWeeklySchedule(1);
			}

			UnitFrequency unitFrequency = parseUnitFrequency(false);
			if (unitFrequency.unit == ChronoUnit.DAYS) {
				return parseDailySchedule(unitFrequency.frequency);
			}
			else if (unitFrequency.unit == ChronoUnit.WEEKS) {
				return parseWeeklySchedule(unitFrequency.frequency);
			}
			else if (unitFrequency.unit == ChronoUnit.MONTHS) {
				return parseMonthlySchedule(unitFrequency.frequency);
			}
			else {
				throw new InputMismatchException("Unsupported calendar unit for date schedule");
			}
		}
		else {
			throw new InputMismatchException("Invalid date schedule syntax");
		}
	}

	private Schedule parseDateTimeSchedule() throws IOException {

		LocalDateTime dateTime = nextQuotedDateTime();
		return new Schedule(
				DateSchedule.onetime(dateTime.toLocalDate()),
				TimeSchedule.onetime(dateTime.toLocalTime()));
	}
	
	private Schedule parseDateSchedule() throws IOException {

		LocalDate date = nextQuotedDate();
		return parseDateSchedule(date);
	}
	
	private Schedule parseDateSchedule(LocalDate date) throws IOException {

		TimeSchedule timeSchedule = parseTimeSchedule();
		return new Schedule(
				DateSchedule.onetime(date),
				timeSchedule);
	}
	
	private Schedule parseDailySchedule(int frequency) throws IOException {

		DateRange range = parseDateRange();
		TimeSchedule timeSchedule = parseTimeSchedule();
		return new Schedule(
				DateSchedule.recurring(ChronoUnit.DAYS, frequency, range.startDate, range.endDate),
				timeSchedule);
	}

	private Schedule parseWeeklySchedule(int frequency) throws IOException {

		tokenizer.skipWordIgnoreCase(ON);
		Set<DayOfWeek> days = parseDayOfWeekSet();
		return parseWeeklySchedule(frequency, days);
	}

	private Schedule parseWeeklySchedule(int frequency, Set<DayOfWeek> days) throws IOException {

		DateRange range = parseDateRange();
		TimeSchedule timeSchedule = parseTimeSchedule();
		return new Schedule(
				DateSchedule.daysOfWeek(frequency, days, range.startDate, range.endDate),
				timeSchedule);
	}

	private Schedule parseMonthlySchedule(int frequency) throws IOException {

		tokenizer.skipWordIgnoreCase(ON);
		if (tokenizer.skipWordIgnoreCase(DAY)) {
			return parseOrdinalDayOfMonthSchedule(frequency);
		}
		else {
			return parseLogicalDayOfMonthSchedule(frequency);
		}
	}

	private Schedule parseOrdinalDayOfMonthSchedule(int frequency) throws IOException {

		int ordinal = tokenizer.nextInt();
		return parseOrdinalDayOfMonthSchedule(frequency, ordinal);
	}

	private Schedule parseOrdinalDayOfMonthSchedule(int frequency, int ordinal) throws IOException {

		DateRange range = parseDateRange();
		TimeSchedule timeSchedule = parseTimeSchedule();
		return new Schedule(
				DateSchedule.ordinalDayOfMonth(frequency, ordinal, range.startDate, range.endDate),
				timeSchedule);
	}

	private Schedule parseLogicalDayOfMonthSchedule(int frequency) throws IOException {

		int ordinal = parseOrdinal();
		LogicalDay logicalDay = parseLogicalDay();

		if ((0 < ordinal) && (logicalDay == LogicalDay.DAY)) {
			return parseOrdinalDayOfMonthSchedule(frequency, ordinal);
		}

		DateRange range = parseDateRange();
		TimeSchedule timeSchedule = parseTimeSchedule();
		return new Schedule(
				DateSchedule.logicalDayOfMonth(frequency, ordinal, logicalDay, range.startDate, range.endDate),
				timeSchedule);
	}

	private class DateRange {
		public LocalDate startDate;
		public LocalDate endDate;
		
		DateRange(LocalDate fromDate, LocalDate toDate) {
			this.startDate = fromDate;
			this.endDate = toDate;
		}
	}

	private DateRange parseDateRange() throws IOException {

		LocalDate startDate = LocalDate.now();
		LocalDate endDate = null;
		if (tokenizer.skipWordIgnoreCase(FROM)) {
			startDate = nextQuotedDate();
		}
		if (tokenizer.skipWordIgnoreCase(UNTIL)) {
			endDate = nextQuotedDate();
		}
		return new DateRange(startDate, endDate);
	}

	private TimeSchedule parseTimeSchedule() throws InputMismatchException, NoSuchElementException, IOException {
		
		if (tokenizer.skipWordIgnoreCase(AT)) {
			return TimeSchedule.onetime(nextQuotedTime());
		}
		else if (tokenizer.skipWordIgnoreCase(EVERY)) {
			UnitFrequency unitFrequency = parseUnitFrequency(true);
			TimeRange range = parseTimeRange();

			return TimeSchedule.recurring(unitFrequency.unit, unitFrequency.frequency, range.startTime, range.endTime);
		}
		else {
			throw new InputMismatchException("Invalid time schedule syntax");
		}
	}

	private class TimeRange {
		public LocalTime startTime;
		public LocalTime endTime;
		
		TimeRange(LocalTime fromTime, LocalTime toTime) {
			this.startTime = fromTime;
			this.endTime = toTime;
		}
	}

	private TimeRange parseTimeRange() throws IOException {

		LocalTime startTime = LocalTime.MIN;
		LocalTime endTime = LocalTime.MAX;
		if (tokenizer.skipWordIgnoreCase(FROM)) {
			startTime = nextQuotedTime();
		}
		if (tokenizer.skipWordIgnoreCase(UNTIL)) {
			endTime = nextQuotedTime();
		}
		return new TimeRange(startTime, endTime);
	}

	private class UnitFrequency {
		public ChronoUnit unit;
		public int frequency;
		
		UnitFrequency(ChronoUnit unit, int frequency) {
			this.unit = unit;
			this.frequency = frequency; 
		}
	}

	private UnitFrequency parseUnitFrequency(boolean wantTime) throws InputMismatchException, NoSuchElementException, IOException {

		int frequency = 1;
		String suffix = "";

		if (tokenizer.hasNextInt()) {
			frequency = tokenizer.nextInt();
			if (frequency > 1) {
				suffix = "S";
			}
		}

		ChronoUnit unit = null;
		boolean isTime = false;

		if (tokenizer.skipWordIgnoreCase(DAY + suffix)) {
			unit = ChronoUnit.DAYS;
			isTime = false;
		}
		else if (tokenizer.skipWordIgnoreCase(WEEK + suffix)) {
			unit = ChronoUnit.WEEKS;
			isTime = false;
		}
		else if (tokenizer.skipWordIgnoreCase(MONTH + suffix)) {
			unit = ChronoUnit.MONTHS;
			isTime = false;
		}
		else if (tokenizer.skipWordIgnoreCase(HOUR + suffix)) {
			unit = ChronoUnit.HOURS;
			isTime = true;
		}
		else if (tokenizer.skipWordIgnoreCase(MINUTE + suffix)) {
			unit = ChronoUnit.MINUTES;
			isTime = true;
		}
		else if (tokenizer.skipWordIgnoreCase(SECOND + suffix)) {
			unit = ChronoUnit.SECONDS;
			isTime = true;
		}
		else {
			throw new InputMismatchException("Unrecognized date or time unit or singular / plural mismatch");
		}

		if (isTime != wantTime) {
			throw new InputMismatchException("Date or time unit not valid in this context");
		}

		return new UnitFrequency(unit, frequency);
	}

	private Set<DayOfWeek> parseDayOfWeekSet() throws IOException {

		Set<DayOfWeek> result = new HashSet<DayOfWeek>();

		do {
			result.add(DayOfWeek.valueOf(tokenizer.nextWordUpperCase()));
		} while (tokenizer.hasNextDelimiter(","));

		return result;
	}

	private int parseOrdinal() throws IOException {

		int ordinal;
		if (tokenizer.skipWordIgnoreCase(FIRST)) {
			ordinal = 1;
		}
		else if (tokenizer.skipWordIgnoreCase(SECOND)) {
			ordinal = 2;
		}
		else if (tokenizer.skipWordIgnoreCase(THIRD)) {
			ordinal = 3;
		}
		else if (tokenizer.skipWordIgnoreCase(FOURTH)) {
			ordinal = 4;
		}
		else if (tokenizer.skipWordIgnoreCase(LAST)) {
			ordinal = 0;
		}
		else {
			throw new InputMismatchException("Invalid day of month ordinal");
		}
		return ordinal;
	}

	private LogicalDay parseLogicalDay() throws InputMismatchException, NoSuchElementException, IOException {

		LogicalDay day = null;
		if (tokenizer.skipWordIgnoreCase(DAY)) {
			day = LogicalDay.DAY;
		}
		else if (tokenizer.skipWordIgnoreCase(WEEKDAY)) {
			day = LogicalDay.WEEKDAY;
		}
		else {
			DayOfWeek dayOfWeek = DayOfWeek.valueOf(tokenizer.nextWordUpperCase());
			day = LogicalDay.of(dayOfWeek);
		}
		return day;
	}

    /**
     * Returns true if the next token in this tokenizer's input is a valid
     * name of a day of the week ignoring case.
     * The tokenizer does not advance past any input.
     * 
     * @return	true if and only if this tokenizer's next token is a valid
     *			name of a day of the week
     * @throws	IOException if passed by the underlying
     *			<code>java.io.Reader</code> object of this tokenizer.
     */
	private boolean hasNextDayOfWeekIgnoreCase() throws IOException {

		boolean result = true;

		BacktrackingTokenizerMark mark = tokenizer.mark();
		try {
			DayOfWeek.valueOf(tokenizer.nextWordUpperCase());
		}
		catch (IllegalArgumentException ex) {
			result = false;
		}
		finally {
			tokenizer.reset(mark);
		}

		return result;
	}
	
	/**
     * Scans the next token of the input as quoted text containing a date.
     *
     * @return	a <code>LocalDate</code> containing the date parsed
     *			from the input.
     * @throws	InputMismatchException if the next token
     *			is not quoted text or cannot be parsed as a date
     * @throws	NoSuchElementException if input is exhausted
     * @throws	IOException if passed by the underlying
     *			<code>java.io.Reader</code> object of this ScheduleParser.
     */
	private LocalDate nextQuotedDate() throws IOException, InputMismatchException, NoSuchElementException {

		Quoted quotedDate = tokenizer.nextQuoted();

		final DateTimeFormatter[] formatters = {
				DateTimeFormatter.ofPattern("M/d/yyyy"),
				DateTimeFormatter.ofPattern("yyyy-M-d"),
				};

		LocalDate result = null;
		for (DateTimeFormatter formatter : formatters) {
			try {
				result = LocalDate.parse(quotedDate.getBody(), formatter);
				break;
			}
			catch (DateTimeParseException ex) {}
		}
		if (result == null) {
			throw new InputMismatchException("Invalid date format: " + quotedDate.toString());
		}
		return result;
	}

	/**
     * Scans the next token of the input as quoted text containing a time of day.
     *
     * @return	a <code>LocalTime</code> containing the time parsed
     *			from the input.
     * @throws	InputMismatchException if the next token
     *			is not quoted text or cannot be parsed as a time of day
     * @throws	NoSuchElementException if input is exhausted
     * @throws	IOException if passed by the underlying
     *			<code>java.io.Reader</code> object of this ScheduleParser.
     */
	private LocalTime nextQuotedTime() throws IOException, InputMismatchException, NoSuchElementException {

		Quoted quotedTime = tokenizer.nextQuoted();

		final DateTimeFormatter[] formatters = {
				DateTimeFormatter.ofPattern("h:m[:s] a"),
				DateTimeFormatter.ofPattern("H:m[:s]")
				};

		LocalTime result = null;
		for (DateTimeFormatter formatter : formatters) {
			try {
				result = LocalTime.parse(quotedTime.getBody(), formatter);
				break;
			}
			catch (DateTimeParseException ex) {}
		}
		if (result == null) {
			throw new InputMismatchException("Invalid time format: " + quotedTime.toString());
		}
		return result;
	}

	/**
     * Scans the next token of the input as quoted text containing a date and time of day.
     *
     * @return	a <code>LocalDateTime</code> containing the date and time parsed
     *			from the input.
     * @throws	InputMismatchException if the next token
     *			is not quoted text or cannot be parsed as a date and time of day
     * @throws	NoSuchElementException if input is exhausted
     * @throws	IOException if passed by the underlying
     *			<code>java.io.Reader</code> object of this ScheduleParser.
     */
	private LocalDateTime nextQuotedDateTime() throws IOException, InputMismatchException, NoSuchElementException {

		Quoted quotedDateTime = tokenizer.nextQuoted();

		final DateTimeFormatter[] formatters = {
				DateTimeFormatter.ofPattern("M/d/yyyy h:m[:s] a"),
				DateTimeFormatter.ofPattern("M/d/yyyy H:m[:s]"),
				DateTimeFormatter.ofPattern("yyyy-M-d H:m[:s]"),
				DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.SSS]")
				};

		LocalDateTime result = null;
		for (DateTimeFormatter formatter : formatters) {
			try {
				result = LocalDateTime.parse(quotedDateTime.getBody(), formatter);
				break;
			}
			catch (DateTimeParseException ex) {}
		}
		if (result == null) {
			throw new InputMismatchException("Invalid time format: " + quotedDateTime.toString());
		}
		return result;
	}
}
