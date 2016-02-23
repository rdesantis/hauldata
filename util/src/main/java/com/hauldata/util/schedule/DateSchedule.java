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

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Set;

/**
 * Schedule of event(s) that occur on a calendar schedule without regard to time of day
 */
public abstract class DateSchedule {

	/**
	 * Instantiate a schedule for an event that occurs on one date
	 * 
	 * @param time is the time of the event
	 * @return the schedule
	 */
	public static DateSchedule onetime(LocalDate date) {
		return new OnetimeDateSchedule(date);
	}

	/**
	 * Instantiate a schedule for an event that recurs on multiple evenly-spaced dates
	 * 
	 * @param unit is the time unit of the recurrence, e.g., day, week, month
	 * @param frequency is the number of time units from one event to the next
	 * @param startDate is the date of the first event
	 * @param endDate is the latest date allowed for an event or null for an open-ended schedule
	 * @return the schedule
	 */
	public static DateSchedule recurring(
			ChronoUnit unit,
			int frequency,
			LocalDate startDate,
			LocalDate endDate) {
		return new RecurringDateSchedule(unit, frequency, startDate, endDate);
	}

	/**
	 * Instantiate a schedule for an event that recurs on certain days of the week
	 * 
	 * @param frequency is the number of weeks from one set of days to the next,
	 * e.g., 1 if the schedule recurs every week, 2 for every other week, etc.
	 * @param days is the days of the week on which the event occurs
	 * @param startDate is the date of the first event
	 * @param endDate is the latest date allowed for an event or null for an open-ended schedule
	 * @return the schedule
	 */
	public static DateSchedule daysOfWeek(
			int frequency,
			Set<DayOfWeek> days,
			LocalDate startDate,
			LocalDate endDate) {
		return new DaysOfWeekSchedule(frequency, days, startDate, endDate);
	}

	/**
	 * Instantiate a schedule for an event that recurs on a certain day number of the month
	 * 
	 * @param frequency is the number of months from one event to the next,
	 * e.g., 1 if the schedule recurs every month, 2 for every other month, etc.
	 * @param ordinal is the numeric day of the month on which the event occurs,
	 * e.g., 1 for the 1st, 2 for the 2nd, etc. 
	 * @param startDate is the date of the first event
	 * @param endDate is the latest date allowed for an event or null for an open-ended schedule
	 * @return the schedule
	 */
	public static DateSchedule ordinalDayOfMonth(
			int frequency,
			int ordinal,
			LocalDate startDate,
			LocalDate endDate) {
		return new OrdinalDayOfMonthSchedule(frequency, ordinal, startDate, endDate);
	}

	/**
	 * Instantiate a schedule for an event that recurs on a certain logical day of the month
	 * 
	 * @param frequency is the number of months from one event to the next,
	 * e.g., 1 if the schedule recurs every month, 2 for every other month, etc.
	 * @param ordinal indicates the instance of the logical day within the month,
	 * e.g., 1 for the 1st, 2 for the 2nd, etc., with 0 indicating the last instance in the month
	 * @param day is the logical day of the month
	 * @param startDate is the date of the first event
	 * @param endDate is the latest date allowed for an event or null for an open-ended schedule
	 * @return the schedule
	 */
	public static DateSchedule logicalDayOfMonth(
			int frequency,
			int ordinal,
			LogicalDay day,
			LocalDate startDate,
			LocalDate endDate) {
		return
				(day.equals(LogicalDay.DAY) && (0 < ordinal)) ?
				new OrdinalDayOfMonthSchedule(frequency, ordinal, startDate, endDate) :
				new LogicalDayOfMonthSchedule(frequency, ordinal, day, startDate, endDate);
	}

	/**
	 * Logical days within the month.
	 * DO NOT REORDER THESE.  MONDAY through SUNDAY must have ordinal values 1 through 7
	 * following the ISO-8601 standard in order to work in conjunction with java.time.DayOfWeek.
	 */
	enum LogicalDay {
		DAY, MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY, WEEKDAY, WEEKENDDAY;

		private static LogicalDay[] logicalDayValues = LogicalDay.values();

		public static LogicalDay of(DayOfWeek day) {
			return logicalDayValues[day.getValue()];
		}
	}

	/**
	 * Return the date of the next scheduled event on or after
	 * the indicated date.
	 * 
	 * @param earliestDate is the earliest date allowed for the next event
	 * @return the date of the next event or null if no more events are
	 * scheduled on or after the date
	 */
	public abstract LocalDate nextFrom(LocalDate earliestDate);
}

class OnetimeDateSchedule extends DateSchedule {

	private LocalDate date;

	public OnetimeDateSchedule(LocalDate date) {

		this.date = date;
	}

	@Override
	public LocalDate nextFrom(LocalDate earliestDate) {
		return !earliestDate.isAfter(date) ? date : null;
	}
}

abstract class ChronoDateSchedule extends DateSchedule {
	
	protected ChronoUnit unit;
	protected int frequency;
	protected LocalDate startDate;
	protected LocalDate endDate;

	protected ChronoDateSchedule(
			ChronoUnit unit,
			int frequency,
			LocalDate startDate,
			LocalDate endDate) {

		if (frequency < 0) {
			throw new RuntimeException("Frequency must be positive");
		}

		if ((endDate != null) && endDate.isBefore(startDate)) {
			throw new RuntimeException("End date cannot be earlier than start date");
		}

		this.unit = unit;
		this.frequency = frequency;
		this.startDate = startDate;
		this.endDate = endDate;
	}

	protected abstract LocalDate nextFrom(LocalDate nearbyDate, LocalDate earliestDate);

	@Override
	public LocalDate nextFrom(LocalDate earliestDate) {

		if (earliestDate.isBefore(startDate)) {
			earliestDate = startDate;
		}

		if ((endDate != null) && earliestDate.isAfter(endDate)) {
			return null;
		}

		int unitsUntil = (int)startDate.until(earliestDate, unit);
		int cyclesUntil = unitsUntil / frequency;

		LocalDate result = nextFrom(startDate.plus(cyclesUntil * frequency, unit), earliestDate);

		if ((endDate != null) && result.isAfter(endDate)) {
			return null;
		}

		return result;
	}
}

class RecurringDateSchedule extends ChronoDateSchedule {
	
	public RecurringDateSchedule(
			ChronoUnit unit,
			int frequency,
			LocalDate startDate,
			LocalDate endDate) {

		super(unit, frequency, startDate, endDate);
	}

	@Override
	protected LocalDate nextFrom(LocalDate nearbyDate, LocalDate earliestDate) {

		if (earliestDate.isAfter(nearbyDate)) {
			nearbyDate = nearbyDate.plus(frequency, unit);
		}

		return nearbyDate;
	}
}

class DaysOfWeekSchedule extends ChronoDateSchedule {

	private Set<DayOfWeek> days;

	public DaysOfWeekSchedule(
			int frequency,
			Set<DayOfWeek> days,
			LocalDate startDate,
			LocalDate endDate) {

		super(ChronoUnit.WEEKS, frequency, startDate, endDate);
		
		if (days.isEmpty()) {
			throw new RuntimeException("At least one day of the week must be selected");
		}
		
		this.days = days;
	}

	@Override
	protected LocalDate nextFrom(LocalDate nearbyDate, LocalDate earliestDate) {

		while (!days.contains(nearbyDate.getDayOfWeek()) || earliestDate.isAfter(nearbyDate)) {
			nearbyDate = nearbyDate.plusDays(1);
		}

		return nearbyDate;
	}
}

class OrdinalDayOfMonthSchedule extends ChronoDateSchedule {
	
	private int ordinal;

	public OrdinalDayOfMonthSchedule(
			int frequency,
			int ordinal,
			LocalDate startDate,
			LocalDate endDate) {

		super(ChronoUnit.MONTHS, frequency, startDate, endDate);

		if ((ordinal < 1) || (28 < ordinal)) {
			throw new RuntimeException("Day of month must be between 1 and 28");
		}
		
		this.ordinal = ordinal;
	}

	@Override
	protected LocalDate nextFrom(LocalDate nearbyDate, LocalDate earliestDate) {

		nearbyDate =
				nearbyDate.getDayOfMonth() < ordinal ? nearbyDate.withDayOfMonth(ordinal) :
				nearbyDate.getDayOfMonth() == ordinal ? nearbyDate :
				nearbyDate.plusMonths(1).withDayOfMonth(ordinal);

		if (earliestDate.isAfter(nearbyDate)) {
			nearbyDate = nearbyDate.plus(frequency, unit);
		}

		return nearbyDate;
	}
}

class LogicalDayOfMonthSchedule extends ChronoDateSchedule {

	private int ordinal;
	private LogicalDay day;

	public LogicalDayOfMonthSchedule(
			int frequency,
			int ordinal,
			LogicalDay day,
			LocalDate startDate,
			LocalDate endDate) {

		super(ChronoUnit.MONTHS, frequency, startDate, endDate);

		if ((ordinal < 0) || (4 < ordinal)) {
			throw new RuntimeException("Instance in month must be between 0 and 4");
		}

		if (startDate.getDayOfMonth() != 1) {
			throw new RuntimeException("For logical day of month schedule, start date must be first of a month");
			// See the note in shiftToOrdinalDayOfWeekOfMonth() before removing this restriction!
		}

		this.ordinal = ordinal;
		this.day = day;
	}

	@Override
	protected LocalDate nextFrom(LocalDate nearbyDate, LocalDate earliestDate) {

		if (day.equals(LogicalDay.DAY)) {
			if (ordinal == 0) {
				// Last day of month

				nearbyDate = shiftToLastDayOfMonth(nearbyDate);

				if (earliestDate.isAfter(nearbyDate)) {
					nearbyDate = nearbyDate.plus(frequency, unit);
					nearbyDate = shiftToLastDayOfMonth(nearbyDate);
				}

				return nearbyDate;
			}
			else {
				// This code should never be reached.
				// DateSchedule.logicalDayOfMonth() redirects this case to OrdinalDayOfMonthSchedule.
				throw new RuntimeException("For logical DAY of month schedule, only LAST instance is supported");
			}
		}
		else if ((LogicalDay.MONDAY.compareTo(day) <= 0) && (0 <= day.compareTo(LogicalDay.SUNDAY))) {
			if (ordinal == 0) {
				// Last instance of day of week in month

				nearbyDate = shiftToLastDayOfWeekOfMonth(nearbyDate);

				if (earliestDate.isAfter(nearbyDate)) {
					nearbyDate = nearbyDate.plus(frequency, unit);
					nearbyDate = shiftToLastDayOfWeekOfMonth(nearbyDate);
				}

				return nearbyDate;
			}
			else {
				// First through fourth instance of day of week in month
				
				nearbyDate = shiftToOrdinalDayOfWeekOfMonth(nearbyDate);

				if (earliestDate.isAfter(nearbyDate)) {
					nearbyDate = nearbyDate.plus(frequency, unit);
					nearbyDate = shiftToOrdinalDayOfWeekOfMonth(nearbyDate);
				}

				return nearbyDate;
			}
		}
		else {
			throw new RuntimeException("Logical WEEKDAY and WEEKENDDAY of month are not implemented");
		}
	}
	
	private LocalDate shiftToLastDayOfMonth(LocalDate date) {
		return date.withDayOfMonth(date.lengthOfMonth());
	}
	
	private LocalDate shiftToLastDayOfWeekOfMonth(LocalDate date) {

		// NOTE: The note in shiftToOrdinalDayOfWeekOfMonth() applies here too.

		date = shiftToLastDayOfMonth(date);

		int lastDayOfWeekInMonth = date.getDayOfWeek().getValue();

		int offsetFromRequiredDayOfWeek = lastDayOfWeekInMonth - day.ordinal();
		if (offsetFromRequiredDayOfWeek < 0) {
			offsetFromRequiredDayOfWeek += 7;
		}
		return date.minusDays(offsetFromRequiredDayOfWeek);
	}

	private LocalDate shiftToOrdinalDayOfWeekOfMonth(LocalDate date) {

		// NOTE: For an arbitrary date, this function may return a date either forward or backward
		// in time relative to the argument date.  As used in the enclosing class, the shift
		// must never be backward in time.  That is guaranteed by assuring in the class
		// constructor that the argument date is always the first of the month.  If that restriction is
		// removed in the constructor, this function must be modified to handle it.
		// This note also applies to shiftToLastDayOfWeekOfMonth().

		date = date.withDayOfMonth(1);

		int firstDayOfWeekInMonth = date.getDayOfWeek().getValue();

		int offsetToRequiredDayOfWeek = day.ordinal() - firstDayOfWeekInMonth;
		if (offsetToRequiredDayOfWeek < 0) {
			offsetToRequiredDayOfWeek += 7;
		}
		return date.plusDays(offsetToRequiredDayOfWeek).plusWeeks(ordinal - 1);
	}
}
