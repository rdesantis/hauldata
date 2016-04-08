package com.hauldata.dbpa.task;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;

import com.hauldata.dbpa.expression.Expression;

public class WaitforTimeTask extends WaitforTask {

	public WaitforTimeTask(
			Prologue prologue,
			Expression<String> time) {
		super(prologue, time);
	}

	@Override
	protected long sleepMillis(String time) {

		final DateTimeFormatter[] formatters = {
				DateTimeFormatter.ofPattern("h:m[:s] a"),
				DateTimeFormatter.ofPattern("H:m[:s]"),
				};

		LocalTime endTime = null;
		for (DateTimeFormatter formatter : formatters) {
			try {
				endTime = LocalTime.parse(time, formatter);
				break;
			}
			catch (DateTimeParseException ex) {
			}
		}
		if (endTime == null) {
			throw new RuntimeException("Invalid time expression: " + time);
		}

		long millis = LocalTime.now().until(endTime, ChronoUnit.MILLIS);
		if (millis < 0) {
			LocalDateTime endTimeTomorrow = LocalDateTime.of(LocalDate.now().plus(1, ChronoUnit.DAYS), endTime);
			millis = LocalDateTime.now().until(endTimeTomorrow, ChronoUnit.MILLIS);
		}

		return millis;
	}
}
