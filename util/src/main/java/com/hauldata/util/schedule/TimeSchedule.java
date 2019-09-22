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

import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

/**
 * Schedule of event(s) that occur within a single day
 */
public abstract class TimeSchedule {

	/**
	 * Instantiate a schedule for an event that never occurs
	 */
	public static TimeSchedule never() {
		return new NeverTimeSchedule();
	}

	/**
	 * Instantiate a schedule for an event that occurs once a day
	 *
	 * @param time is the time of the event
	 * @return the schedule
	 */
	public static TimeSchedule onetime(LocalTime time) {
		return new OnetimeTimeSchedule(time);
	}

	/**
	 * Instantiate a schedule for an event that recurs multiple times per day
	 *
	 * @param unit is the time unit of the recurrence, e.g., hour, minute, second
	 * @param frequency is the number of time units from one event to the next
	 * @param startTime is the time of the first event
	 * @param endTime is the latest time allowed for an event
	 * @return the schedule
	 */
	public static TimeSchedule recurring(
			ChronoUnit unit,
			int frequency,
			LocalTime startTime,
			LocalTime endTime) {
		return new RecurringTimeSchedule(unit, frequency, startTime, endTime);
	}

	/**
	 * Return the time of the next scheduled event on or after
	 * the indicated time.
	 * <p>
	 * Because the mapping from a LocalTime value to an Instant may be
	 * ambiguous on a day when Daylight Saving Time starts or ends,
	 * this method cannot be used to reliably determine the elapsed time
	 * until the next scheduled event will occur.  Use the method
	 * nextFrom(ZonedDateTime) if the method result is to be used for
	 * that purpose.
	 *
	 * @param earliestTime is the earliest time allowed for the next event
	 * @return the time of the next event or null if no more events are
	 * scheduled in the day
	 */
	public abstract LocalTime nextFrom(LocalTime earliestTime);

	/**
	 * Return the datetime of the next scheduled event on or after
	 * the indicated datetime but on the same day.
	 *
	 * @param earliestDatetime is the earliest datetime allowed for the next event
	 * @return the datetime of the next event or null if no more events are
	 * scheduled in the day.  If the return value is not null, it will always
	 * be on the same date as earliestDateTime.
	 */
	public abstract ZonedDateTime nextFrom(ZonedDateTime earliestDatetime);
}

class NeverTimeSchedule extends TimeSchedule {

	public NeverTimeSchedule() {}

	@Override
	public LocalTime nextFrom(LocalTime earliestTime) {
		return null;
	}

	@Override
	public ZonedDateTime nextFrom(ZonedDateTime earliestDatetime) {
		return null;
	}
}

class OnetimeTimeSchedule extends TimeSchedule {

	private LocalTime time;

	public OnetimeTimeSchedule(LocalTime time) {

		this.time = time;
	}

	@Override
	public LocalTime nextFrom(LocalTime earliestTime) {
		return !earliestTime.isAfter(time) ? time : null;
	}

	@Override
	public ZonedDateTime nextFrom(ZonedDateTime earliestDatetime) {
		ZonedDateTime datetime = ZonedDateTime.of(earliestDatetime.toLocalDate(), time, earliestDatetime.getZone());
		return !earliestDatetime.isAfter(datetime) ? datetime : null;
	}
}

class RecurringTimeSchedule extends TimeSchedule {

	ChronoUnit unit;
	int frequency;
	LocalTime startTime;
	LocalTime endTime;

	public RecurringTimeSchedule(
			ChronoUnit unit,
			int frequency,
			LocalTime startTime,
			LocalTime endTime) {

		if (endTime.isBefore(startTime)) {
			throw new RuntimeException("End time cannot be earlier than start time");
		}

		this.unit = unit;
		this.frequency = frequency;
		this.startTime = startTime;
		this.endTime = endTime;
	}

	@Override
	public LocalTime nextFrom(LocalTime earliestTime) {

		if (!earliestTime.isAfter(startTime)) {
			return startTime;
		}

		if (earliestTime.isAfter(endTime)) {
			return null;
		}

		int unitsUntil = (int)startTime.until(earliestTime, unit);
		int cyclesUntil = unitsUntil / frequency;

		LocalTime result = startTime.plus(cyclesUntil * frequency, unit);

		if (!earliestTime.isAfter(result)) {
			return result;
		}

		result = result.plus(frequency, unit);

		if (result.isAfter(endTime)) {
			return null;
		}

		return result;
	}

	@Override
	public ZonedDateTime nextFrom(ZonedDateTime earliestDatetime) {

		ZonedDateTime startDatetime = ZonedDateTime.of(earliestDatetime.toLocalDate(), this.startTime, earliestDatetime.getZone());
		ZonedDateTime endDatetime = ZonedDateTime.of(earliestDatetime.toLocalDate(), this.endTime, earliestDatetime.getZone());

		if (!earliestDatetime.isAfter(startDatetime)) {
			return startDatetime;
		}

		if (earliestDatetime.isAfter(endDatetime)) {
			return null;
		}

		int unitsUntil = (int)startDatetime.until(earliestDatetime, unit);
		int cyclesUntil = unitsUntil / frequency;

		ZonedDateTime result = startDatetime.plus(cyclesUntil * frequency, unit);

		if (!earliestDatetime.isAfter(result)) {
			return result;
		}

		result = result.plus(frequency, unit);

		if (result.isAfter(endDatetime)) {
			return null;
		}

		return result;
	}
}
