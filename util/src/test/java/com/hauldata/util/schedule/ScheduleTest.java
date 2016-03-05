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

import com.hauldata.util.schedule.DateSchedule;
import com.hauldata.util.schedule.Schedule;
import com.hauldata.util.schedule.ScheduleSet;
import com.hauldata.util.schedule.TimeSchedule;

public class ScheduleTest {

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
	static LocalTime eightThirtyPm = LocalTime.of(20, 30);
	static LocalTime ninePm = LocalTime.of(21, 0);

	static LocalDate sunday1108 = LocalDate.of(2015, 11, 8);
	static LocalDate monday1109 = LocalDate.of(2015, 11, 9);
	static LocalDate tuesday1110 = LocalDate.of(2015, 11, 10);
	static LocalDate friday1113 = LocalDate.of(2015, 11, 13);
	static LocalDate sunday1115 = LocalDate.of(2015, 11, 15);
	static LocalDate monday1116 = LocalDate.of(2015, 11, 16);
	static LocalDate tuesday1201 = LocalDate.of(2015, 12, 01);

	public static void main(String[] args) {

		testTime();
		testDate();
		testDateTime();
		testMulti();
		testParse();
		testSleep();
	}

	static void testTime() {

		TimeSchedule atNoon = TimeSchedule.onetime(noon);
		
		assert(atNoon.nextFrom(tenAm).equals(noon));
		assert(atNoon.nextFrom(noon).equals(noon));
		assert(atNoon.nextFrom(twoPm) == null);

		TimeSchedule everyTwoHoursFromNineAm = TimeSchedule.recurring(ChronoUnit.HOURS, 2, nineAm, threeFifteenPm);

		assert(everyTwoHoursFromNineAm.nextFrom(tenAm).equals(LocalTime.of(11, 0)));
		assert(everyTwoHoursFromNineAm.nextFrom(noon).equals(LocalTime.of(13, 0)));
		assert(everyTwoHoursFromNineAm.nextFrom(twoPm).equals(LocalTime.of(15, 0)));
		assert(everyTwoHoursFromNineAm.nextFrom(fourPm) == null);

		System.out.println("Time tests pass");
	}

	static void testDate() {


		DateSchedule onMonday = DateSchedule.onetime(monday1109);

//		System.out.println(onMonday.nextFrom(sunday1108));

		assert(onMonday.nextFrom(sunday1108).equals(monday1109));
		assert(onMonday.nextFrom(monday1109).equals(monday1109));
		assert(onMonday.nextFrom(tuesday1110) == null);

		DateSchedule everyThirdDay = DateSchedule.recurring(ChronoUnit.DAYS, 3, monday1109, sunday1115);

		assert(everyThirdDay.nextFrom(sunday1108).equals(monday1109));
		assert(everyThirdDay.nextFrom(monday1109).equals(monday1109));
		assert(everyThirdDay.nextFrom(tuesday1110).equals(LocalDate.of(2015, 11, 12)));
		assert(everyThirdDay.nextFrom(sunday1115).equals(sunday1115));
		assert(everyThirdDay.nextFrom(monday1116) == null);

		System.out.println("Date tests pass");
	}

	static void testDateTime() {
		
		Schedule monToFri1030amTo0830pm = new Schedule(
				DateSchedule.recurring(ChronoUnit.DAYS, 1, monday1109, friday1113),
				TimeSchedule.recurring(ChronoUnit.HOURS, 2, tenThirtyAm, eightThirtyPm));

		assert(monToFri1030amTo0830pm.nextFrom(LocalDateTime.of(sunday1108, ninePm)).equals(LocalDateTime.of(monday1109, tenThirtyAm)));
		assert(monToFri1030amTo0830pm.nextFrom(LocalDateTime.of(monday1109, noon)).equals(LocalDateTime.of(monday1109, twelveThirtyPm)));
		assert(monToFri1030amTo0830pm.nextFrom(LocalDateTime.of(monday1109, ninePm)).equals(LocalDateTime.of(tuesday1110, tenThirtyAm)));

		System.out.println("DateTime tests pass");
	}

	static void testMulti() {
		
		Schedule mondaysAtFourPm = new Schedule(
				DateSchedule.recurring(ChronoUnit.WEEKS, 1, monday1109, monday1116),
				TimeSchedule.onetime(fourPm));

		Schedule fridayEveryTwoHours = new Schedule(
				DateSchedule.onetime(friday1113),
				TimeSchedule.recurring(ChronoUnit.HOURS, 2, tenThirtyAm, eightThirtyPm));

		ScheduleSet schedules = new ScheduleSet();
		schedules.add(mondaysAtFourPm);
		schedules.add(fridayEveryTwoHours);

		assert(schedules.nextFrom(LocalDateTime.of(sunday1108, ninePm)).equals(LocalDateTime.of(monday1109, fourPm)));
		assert(schedules.nextFrom(LocalDateTime.of(monday1109, noon)).equals(LocalDateTime.of(monday1109, fourPm)));
		assert(schedules.nextFrom(LocalDateTime.of(monday1109, ninePm)).equals(LocalDateTime.of(friday1113, tenThirtyAm)));
		assert(schedules.nextFrom(LocalDateTime.of(friday1113, threeFifteenPm)).equals(LocalDateTime.of(friday1113, fourThirtyPm)));
		assert(schedules.nextFrom(LocalDateTime.of(friday1113, ninePm)).equals(LocalDateTime.of(monday1116, fourPm)));
		assert(schedules.nextFrom(LocalDateTime.of(monday1116, ninePm)) == null);

		System.out.println("Multi tests pass");
	}

	static void testParse() {
		
		ScheduleSet schedules;

		try {
			schedules = ScheduleSet.parse("Every day from '11/9/2015' until '11/13/2015' every hour from '10:30 AM' until '8:30 PM'");
		}
		catch (Exception e) {
			throw new RuntimeException();
		}

		assert(schedules.nextFrom(LocalDateTime.of(sunday1108, ninePm)).equals(LocalDateTime.of(monday1109, tenThirtyAm)));
		assert(schedules.nextFrom(LocalDateTime.of(monday1109, noon)).equals(LocalDateTime.of(monday1109, twelveThirtyPm)));
		assert(schedules.nextFrom(LocalDateTime.of(monday1109, ninePm)).equals(LocalDateTime.of(tuesday1110, tenThirtyAm)));

		try {
			schedules = ScheduleSet.parse("Every Monday from '11/9/2015' until '11/16/2015' at '4:00 PM', '11/13/2015' every 2 hours from '10:30 AM' until '8:30 PM'");
		}
		catch (Exception e) {
			throw new RuntimeException();
		}

		assert(schedules.nextFrom(LocalDateTime.of(sunday1108, ninePm)).equals(LocalDateTime.of(monday1109, fourPm)));
		assert(schedules.nextFrom(LocalDateTime.of(monday1109, noon)).equals(LocalDateTime.of(monday1109, fourPm)));
		assert(schedules.nextFrom(LocalDateTime.of(monday1109, ninePm)).equals(LocalDateTime.of(friday1113, tenThirtyAm)));
		assert(schedules.nextFrom(LocalDateTime.of(friday1113, threeFifteenPm)).equals(LocalDateTime.of(friday1113, fourThirtyPm)));
		assert(schedules.nextFrom(LocalDateTime.of(friday1113, ninePm)).equals(LocalDateTime.of(monday1116, fourPm)));
		assert(schedules.nextFrom(LocalDateTime.of(monday1116, ninePm)) == null);

		try {
			schedules = ScheduleSet.parse("Daily");
		}
		catch (Exception e) {
			throw new RuntimeException();
		}

		assert(schedules.nextFrom(LocalDateTime.of(sunday1108, ninePm)).equals(LocalDateTime.of(monday1109, midnight)));

		try {
			schedules = ScheduleSet.parse("Weekly");
		}
		catch (Exception e) {
			throw new RuntimeException();
		}

		assert(schedules.nextFrom(LocalDateTime.of(monday1109, noon)).equals(LocalDateTime.of(sunday1115, midnight)));

		try {
			schedules = ScheduleSet.parse("Monthly");
		}
		catch (Exception e) {
			throw new RuntimeException();
		}

		assert(schedules.nextFrom(LocalDateTime.of(monday1109, threeFifteenPm)).equals(LocalDateTime.of(tuesday1201, midnight)));

		try {
			schedules = ScheduleSet.parse("Hourly");
		}
		catch (Exception e) {
			throw new RuntimeException();
		}

		assert(schedules.nextFrom(LocalDateTime.of(monday1109, threeFifteenPm)).equals(LocalDateTime.of(monday1109, fourPm)));

		try {
			schedules = ScheduleSet.parse("Daily every 10 seconds");
		}
		catch (Exception e) {
			throw new RuntimeException();
		}

		assert(schedules.nextFrom(LocalDateTime.of(monday1109, LocalTime.of(15, 15, 6))).equals(LocalDateTime.of(monday1109, LocalTime.of(15, 15, 10))));

		System.out.println("Parse tests pass");
	}

	static void testSleep() {

		LocalTime startTime = LocalTime.now().plusSeconds(1);	// Add some padding for CPU latency
		int frequency = 2;
		int cycles = 3;
		LocalTime endTime = startTime.plusSeconds(frequency * (cycles - 1));

		ScheduleSet everyTwoSeconds = new ScheduleSet();

		everyTwoSeconds.add(new Schedule(
				DateSchedule.onetime(LocalDate.now()),
				TimeSchedule.recurring(ChronoUnit.SECONDS, frequency, startTime, endTime)));

		System.out.println("now        " + LocalTime.now());
		System.out.println("start time " + startTime);
		try {
			int count = 0;
			while (everyTwoSeconds.sleepUntilNext()) {
				System.out.println(String.valueOf(++count) + " start at " + LocalTime.now());
			}
		}
		catch (Exception ex) {
			System.out.println(ex.getLocalizedMessage());
		}
		System.out.println("end time   " + endTime);
		System.out.println("now        " + LocalTime.now());
		
		System.out.println("Sleep tests pass");
	}
}
