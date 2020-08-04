/*
 * Copyright (c) 2020, Ronald DeSantis
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

package com.hauldata.dbpa.expression.call;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class CallableSample {

	public static Integer bitTrue() {
		return 1;
	}

	public static Boolean booleanTrue() {
		return true;
	}

	public static Integer five() {
		return 5;
	}

	public static Integer add(Integer a, Integer b) {
		return a + b;
	}

	public static String fred() {
		return "fred";
	}

	public static String left(String string, Integer length) {
		return string.substring(0, length);
	}

	public static LocalDateTime now() {
		return LocalDateTime.now();
	}

	public static LocalDateTime plusDays(LocalDateTime when, Integer days) {
		return when.plusDays(days);
	}

	public static List<List<Object>> stuff() {
		List<List<Object>> rows = new ArrayList<List<Object>>(); 

		List<Object> firstRow = new ArrayList<Object>();
		firstRow.add("hello");
		firstRow.add((Integer)123);
		rows.add(firstRow);

		List<Object> secondRow = new ArrayList<Object>();
		secondRow.add("world");
		secondRow.add((Integer)456);
		rows.add(secondRow);

		return rows;
	}

	public static Integer sum(List<List<Object>> rows) {
		Integer result = 0; 

		for (List<Object> row : rows) {
			for (Object column : row) {
				if (column instanceof Integer) {
					result += (Integer)column;
				}
			}
		}

		return result;
	}
}
