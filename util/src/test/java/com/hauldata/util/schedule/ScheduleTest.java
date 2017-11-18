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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;

public class ScheduleTest extends TestCase {

	static LocalTime midnight = LocalTime.MIDNIGHT;
	static LocalTime nineAm = LocalTime.of(9, 0);
	static LocalTime tenAm = LocalTime.of(10, 0);
	static LocalTime tenThirtyAm = LocalTime.of(10, 30);
	static LocalTime noon = LocalTime.of(12, 0);
	static LocalTime twelveThirtyPm = LocalTime.of(12, 30);
	static LocalTime twoPm = LocalTime.of(14, 0);
	static LocalTime threeFifteenPm = LocalTime.of(15, 15);
	static LocalTime fourPm = LocalTime.of(16, 0);
	static LocalTime fourThirtyPm = LocalTime.of(16, 30);
	static LocalTime fiveThirtyPm = LocalTime.of(17, 30);
	static LocalTime eightThirtyPm = LocalTime.of(20, 30);
	static LocalTime ninePm = LocalTime.of(21, 0);

	static LocalDate sunday1108 = LocalDate.of(2015, 11, 8);
	static LocalDate monday1109 = LocalDate.of(2015, 11, 9);
	static LocalDate tuesday1110 = LocalDate.of(2015, 11, 10);
	static LocalDate friday1113 = LocalDate.of(2015, 11, 13);
	static LocalDate sunday1115 = LocalDate.of(2015, 11, 15);
	static LocalDate monday1116 = LocalDate.of(2015, 11, 16);
	static LocalDate tuesday1201 = LocalDate.of(2015, 12, 01);

    public ScheduleTest(String name) {
        super(name);
    }

	public void testTime() {

		TimeSchedule atNoon = TimeSchedule.onetime(noon);

		assertEquals(atNoon.nextFrom(tenAm), noon);
		assertEquals(atNoon.nextFrom(noon), noon);
		assertNull(atNoon.nextFrom(twoPm));

		TimeSchedule everyTwoHoursFromNineAm = TimeSchedule.recurring(ChronoUnit.HOURS, 2, nineAm, threeFifteenPm);

		assertEquals(everyTwoHoursFromNineAm.nextFrom(tenAm), LocalTime.of(11, 0));
		assertEquals(everyTwoHoursFromNineAm.nextFrom(noon), LocalTime.of(13, 0));
		assertEquals(everyTwoHoursFromNineAm.nextFrom(twoPm), LocalTime.of(15, 0));
		assertNull(everyTwoHoursFromNineAm.nextFrom(fourPm));
	}

	public void testDate() {

		DateSchedule onMonday = DateSchedule.onetime(monday1109);

		assertEquals(onMonday.nextFrom(sunday1108), monday1109);
		assertEquals(onMonday.nextFrom(monday1109), monday1109);
		assertNull(onMonday.nextFrom(tuesday1110));

		DateSchedule everyThirdDay = DateSchedule.recurring(ChronoUnit.DAYS, 3, monday1109, sunday1115);

		assertEquals(everyThirdDay.nextFrom(sunday1108), monday1109);
		assertEquals(everyThirdDay.nextFrom(monday1109), monday1109);
		assertEquals(everyThirdDay.nextFrom(tuesday1110), LocalDate.of(2015, 11, 12));
		assertEquals(everyThirdDay.nextFrom(sunday1115), sunday1115);
		assertNull(everyThirdDay.nextFrom(monday1116));
	}

	public void testDateTime() {

		Schedule monToFri1030amTo0830pm = new Schedule(
				DateSchedule.recurring(ChronoUnit.DAYS, 1, monday1109, friday1113),
				TimeSchedule.recurring(ChronoUnit.HOURS, 2, tenThirtyAm, eightThirtyPm));

		assertEquals(monToFri1030amTo0830pm.nextFrom(LocalDateTime.of(sunday1108, ninePm)), LocalDateTime.of(monday1109, tenThirtyAm));
		assertEquals(monToFri1030amTo0830pm.nextFrom(LocalDateTime.of(monday1109, noon)), LocalDateTime.of(monday1109, twelveThirtyPm));
		assertEquals(monToFri1030amTo0830pm.nextFrom(LocalDateTime.of(monday1109, ninePm)), LocalDateTime.of(tuesday1110, tenThirtyAm));
	}

	public void testMulti() {

		Schedule mondaysAtFourPm = new Schedule(
				DateSchedule.recurring(ChronoUnit.WEEKS, 1, monday1109, monday1116),
				TimeSchedule.onetime(fourPm));

		Schedule fridayEveryTwoHours = new Schedule(
				DateSchedule.onetime(friday1113),
				TimeSchedule.recurring(ChronoUnit.HOURS, 2, tenThirtyAm, eightThirtyPm));

		ScheduleSet schedules = new ScheduleSet();
		schedules.add(mondaysAtFourPm);
		schedules.add(fridayEveryTwoHours);

		assertEquals(schedules.nextFrom(LocalDateTime.of(sunday1108, ninePm)), LocalDateTime.of(monday1109, fourPm));
		assertEquals(schedules.nextFrom(LocalDateTime.of(monday1109, noon)), LocalDateTime.of(monday1109, fourPm));
		assertEquals(schedules.nextFrom(LocalDateTime.of(monday1109, ninePm)), LocalDateTime.of(friday1113, tenThirtyAm));
		assertEquals(schedules.nextFrom(LocalDateTime.of(friday1113, threeFifteenPm)), LocalDateTime.of(friday1113, fourThirtyPm));
		assertEquals(schedules.nextFrom(LocalDateTime.of(friday1113, ninePm)), LocalDateTime.of(monday1116, fourPm));
		assertNull(schedules.nextFrom(LocalDateTime.of(monday1116, ninePm)));
	}

	private ScheduleSet testParse(String scheduleText) throws AssertionFailedError {
		try {
			return ScheduleSet.parse(scheduleText);
		}
		catch (Exception e) {
			throw new AssertionFailedError("Failed parsing: " + e.getMessage());
		}
	}

	public void testParse() {

		ScheduleSet schedules;

		schedules = testParse("Every day from '11/9/2015' until '11/13/2015' every hour from '10:30 AM' until '8:30 PM'");

		assertEquals(schedules.nextFrom(LocalDateTime.of(sunday1108, ninePm)), LocalDateTime.of(monday1109, tenThirtyAm));
		assertEquals(schedules.nextFrom(LocalDateTime.of(monday1109, noon)), LocalDateTime.of(monday1109, twelveThirtyPm));
		assertEquals(schedules.nextFrom(LocalDateTime.of(monday1109, ninePm)), LocalDateTime.of(tuesday1110, tenThirtyAm));

		schedules = testParse("Every Monday from '11/9/2015' until '11/16/2015' at '4:00 PM', '11/13/2015' every 2 hours from '10:30 AM' until '8:30 PM'");

		assertEquals(schedules.nextFrom(LocalDateTime.of(sunday1108, ninePm)), LocalDateTime.of(monday1109, fourPm));
		assertEquals(schedules.nextFrom(LocalDateTime.of(monday1109, noon)), LocalDateTime.of(monday1109, fourPm));
		assertEquals(schedules.nextFrom(LocalDateTime.of(monday1109, ninePm)), LocalDateTime.of(friday1113, tenThirtyAm));
		assertEquals(schedules.nextFrom(LocalDateTime.of(friday1113, threeFifteenPm)), LocalDateTime.of(friday1113, fourThirtyPm));
		assertEquals(schedules.nextFrom(LocalDateTime.of(friday1113, ninePm)), LocalDateTime.of(monday1116, fourPm));
		assertNull(schedules.nextFrom(LocalDateTime.of(monday1116, ninePm)));

		LocalDate today = LocalDate.now();

		schedules = testParse("Daily");

		assertEquals(schedules.nextFrom(LocalDateTime.of(today, ninePm)), LocalDateTime.of(today.plus(1, ChronoUnit.DAYS), midnight));

		schedules = testParse("Weekly from '11/9/2015'");

		assertEquals(schedules.nextFrom(LocalDateTime.of(monday1109, noon)), LocalDateTime.of(sunday1115, midnight));

		schedules = testParse("Monthly from '11/9/2015'");

		assertEquals(schedules.nextFrom(LocalDateTime.of(monday1109, threeFifteenPm)), LocalDateTime.of(tuesday1201, midnight));

		schedules = testParse("Hourly");

		assertEquals(schedules.nextFrom(LocalDateTime.of(today, threeFifteenPm)), LocalDateTime.of(today, fourPm));

		schedules = testParse("Daily every 10 seconds");

		assertEquals(schedules.nextFrom(LocalDateTime.of(today, LocalTime.of(15, 15, 6))), LocalDateTime.of(today, LocalTime.of(15, 15, 10)));

		testParse("Today every second from 1 second from now until 5 seconds from now");

		testParse("Today at 1 seconds from now");

		testParse("Weekdays at '10:00 AM'");

		testParse("Every Monday, Wednesday, Friday at '12:59 PM'");

		testParse("Monthly");

		testParse("Every 3 months on first Friday at '6:00'");

		schedules = testParse("Weekdays from '10/29/2017' at '10:00 AM'");

		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2017, 10, 29), noon)), LocalDateTime.of(LocalDate.of(2017, 10, 30), tenAm));
		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2017, 10, 30), tenAm)), LocalDateTime.of(LocalDate.of(2017, 10, 30), tenAm));
		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2017, 10, 30), noon)), LocalDateTime.of(LocalDate.of(2017, 10, 31), tenAm));
		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2017, 11, 3), noon)), LocalDateTime.of(LocalDate.of(2017, 11, 6), tenAm));
		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2020, 8, 1), tenAm)), LocalDateTime.of(LocalDate.of(2020, 8, 3), tenAm));

		schedules = testParse("Every Tuesday, Thursday from '12/29/2014' at '10:00 AM'");

		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2014, 12, 29), noon)), LocalDateTime.of(LocalDate.of(2014, 12, 30), tenAm));
		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2014, 12, 30), tenAm)), LocalDateTime.of(LocalDate.of(2014, 12, 30), tenAm));
		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2014, 12, 30), noon)), LocalDateTime.of(LocalDate.of(2015, 1, 1), tenAm));
		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2015, 1, 1), noon)), LocalDateTime.of(LocalDate.of(2015, 1, 6), tenAm));
		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2020, 8, 1), tenAm)), LocalDateTime.of(LocalDate.of(2020, 8, 4), tenAm));

		schedules = testParse("Monthly on last day from '10/27/2017'");

		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2017, 10, 27), noon)), LocalDateTime.of(LocalDate.of(2017, 10, 31), midnight));
		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2017, 11, 1), midnight)), LocalDateTime.of(LocalDate.of(2017, 11, 30), midnight));

		schedules = testParse("Every 2 months on last day from '2/29/2016' at '12:00 PM'");

		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2016, 2, 29), noon)), LocalDateTime.of(LocalDate.of(2016, 2, 29), noon));
		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2016, 3, 28), noon)), LocalDateTime.of(LocalDate.of(2016, 4, 30), noon));
		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2016, 4, 29), noon)), LocalDateTime.of(LocalDate.of(2016, 4, 30), noon));
		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2016, 4, 30), noon)), LocalDateTime.of(LocalDate.of(2016, 4, 30), noon));
		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2017, 2, 27), noon)), LocalDateTime.of(LocalDate.of(2017, 2, 28), noon));
		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2017, 2, 28), noon)), LocalDateTime.of(LocalDate.of(2017, 2, 28), noon));

		schedules = testParse("Monthly on third Thursday from '10/18/2017'");

		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2017, 10, 17), noon)), LocalDateTime.of(LocalDate.of(2017, 10, 19), midnight));
		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2017, 10, 19), noon)), LocalDateTime.of(LocalDate.of(2017, 11, 16), midnight));

		schedules = testParse("Monthly on third Thursday from '10/20/2017'");

		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2017, 10, 17), noon)), LocalDateTime.of(LocalDate.of(2017, 11, 16), midnight));
		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2017, 11, 17), noon)), LocalDateTime.of(LocalDate.of(2017, 12, 21), midnight));

		schedules = testParse("Every 2 weeks on Friday from '11/10/2017' at '5:30 PM'");

		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2017, 11, 10), fourPm)), LocalDateTime.of(LocalDate.of(2017, 11, 10), fiveThirtyPm));
		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2017, 11, 10), ninePm)), LocalDateTime.of(LocalDate.of(2017, 11, 24), fiveThirtyPm));
		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2017, 11, 24), ninePm)), LocalDateTime.of(LocalDate.of(2017, 12, 8), fiveThirtyPm));

		// Memorial Day

		schedules = testParse("Every 12 months on last Monday from '5/1/2017' at '12:00 AM'");

		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2017, 1, 1), noon)), LocalDateTime.of(LocalDate.of(2017, 5, 29), midnight));
		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2017, 5, 29), midnight)), LocalDateTime.of(LocalDate.of(2017, 5, 29), midnight));
		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2018, 5, 1), noon)), LocalDateTime.of(LocalDate.of(2018, 5, 28), midnight));
		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2017, 5, 29), noon)), LocalDateTime.of(LocalDate.of(2018, 5, 28), midnight));

		schedules = testParse("Every 12 months on last Monday from '5/29/2017' at '12:00 PM'");

		assertEquals(schedules.nextFrom(LocalDateTime.of(LocalDate.of(2017, 5, 28), midnight)), LocalDateTime.of(LocalDate.of(2017, 5, 29), noon));
	}

	public void testSleep() {

		LocalDateTime startDateTime = LocalDateTime.now().plusSeconds(1);	// Add some padding for CPU latency

		LocalTime startTime = startDateTime.toLocalTime();
		int frequency = 2;
		int cycles = 3;
		LocalTime endTime = startTime.plusSeconds(frequency * (cycles - 1));

		ScheduleSet everyTwoSeconds = new ScheduleSet();

		everyTwoSeconds.add(new Schedule(
				DateSchedule.onetime(startDateTime.toLocalDate()),
				TimeSchedule.recurring(ChronoUnit.SECONDS, frequency, startTime, endTime)));

		final int fuzzMillis = 150;
		final float fuzzSeconds = (float)fuzzMillis / 1000f;

		LocalTime previousTime = startTime.minusSeconds(frequency);
		int count = 0;
		try {
			while (everyTwoSeconds.sleepUntilNext()) {

				LocalTime now = LocalTime.now();
				float duration = (float)ChronoUnit.MILLIS.between(previousTime, now) / 1000f;
				assertEquals((float)frequency, duration, fuzzSeconds);

				previousTime = now;
				++count;
			}
		}
		catch (Exception ex) {
			throw new AssertionFailedError("sleepUntilNext() exception: " + ex.getLocalizedMessage());
		}

		float discrepancy = (float)ChronoUnit.MILLIS.between(endTime, LocalTime.now()) / 1000f;
		assertEquals(0f, discrepancy, fuzzSeconds);

		assertEquals(cycles, count);
	}
}
