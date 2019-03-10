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

public abstract class ColumnFixedField extends FixedField {

	private int startColumn;
	private int endColumn;

	protected ColumnFixedField(int startColumn, int endColumn, Actor actor) {
		super(actor);
		this.startColumn = startColumn;
		this.endColumn = endColumn;
	}

	@Override
	public void actOn(int lineNumber, String record) {
		try {
			actor.invokeWith(lineNumber, getField(record));
		}
		catch (IndexOutOfBoundsException ex) {
			throw new RuntimeException("The COLUMNS specified for a field span beyond the record length of " + String.valueOf(record.length()) + " at line " + String.valueOf(lineNumber));
		}
	}

	protected String getField(String record) {
		return record.substring(startColumn - 1, endColumn);
	}
}
