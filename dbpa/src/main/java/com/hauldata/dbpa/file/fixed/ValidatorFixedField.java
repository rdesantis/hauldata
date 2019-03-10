/*
 * Copyright (c) 2018, 2019, Ronald DeSantis
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

package com.hauldata.dbpa.file.fixed;

public class ValidatorFixedField extends ColumnFixedField {

	public static class Validator implements Actor {

		private String expectedValue;

		public Validator(String expectedValue) {
			this.expectedValue = expectedValue;
		}

		@Override
		public void invokeWith(int lineNumber, String value) {
			if (!isExpecting(value)) {
				throw new RuntimeException("Expecting '" + expectedValue + "'; found '" + value + "' at line " + String.valueOf(lineNumber));
			}
		}

		public boolean isExpecting(String value) {
			return value.equals(expectedValue);
		}
	}

	public ValidatorFixedField(int startColumn, int endColumn, String expectedValue) {
		super(startColumn, endColumn, new Validator(expectedValue));
	}

	public boolean isExpectedIn(String record) {
		try {
			return ((Validator)actor).isExpecting(getField(record));
		}
		catch (IndexOutOfBoundsException ex) {
			return false;
		}
	}
}
