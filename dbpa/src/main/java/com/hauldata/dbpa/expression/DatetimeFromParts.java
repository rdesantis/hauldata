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

package com.hauldata.dbpa.expression;

import java.time.LocalDateTime;

import com.hauldata.dbpa.variable.VariableType;

public class DatetimeFromParts extends Expression<LocalDateTime> {

	public DatetimeFromParts(
			Expression<Integer> year, Expression<Integer> month, Expression<Integer> day,
			Expression<Integer> hour, Expression<Integer> minute, Expression<Integer> seconds, Expression<Integer> milliseconds) {
		super(VariableType.DATETIME);
		this.year = year;
		this.month = month;
		this.day = day;
		this.hour = hour;
		this.minute = minute;
		this.seconds = seconds;
		this.milliseconds = milliseconds;
	}

	@Override
	public LocalDateTime evaluate() {
		Integer yearValue = year.evaluate();
		Integer monthValue = month.evaluate();
		Integer dayValue = day.evaluate();
		Integer hourValue = hour.evaluate();
		Integer minuteValue = minute.evaluate();
		Integer secondsValue = seconds.evaluate();
		Integer millisecondsValue = milliseconds.evaluate();
		
		if (
				yearValue == null || monthValue == null || dayValue == null ||
				hourValue == null || minuteValue == null || secondsValue == null || millisecondsValue == null) {
			return null;
		}

		return LocalDateTime.of(
				yearValue, monthValue, dayValue,
				hourValue, minuteValue, secondsValue, millisecondsValue * 1000000);
	}

	private Expression<Integer> year;
	private Expression<Integer> month;
	private Expression<Integer> day;
	private Expression<Integer> hour;
	private Expression<Integer> minute;
	private Expression<Integer> seconds;
	private Expression<Integer> milliseconds;
}

