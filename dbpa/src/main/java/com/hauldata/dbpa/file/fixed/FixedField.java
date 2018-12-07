/*
 * Copyright (c) 2018, Ronald DeSantis
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

public abstract class FixedField {

	public static interface Actor {
		void invokeWith(String value);
	}

	private int startColumn;
	private int endColumn;
	protected Actor actor;

	protected FixedField(int startColumn, int endColumn, Actor actor) {
		this.startColumn = startColumn;
		this.endColumn = endColumn;
		this.actor = actor;
	}

	public void actOn(String record) {
		try {
			actor.invokeWith(getField(record));
		}
		catch (IndexOutOfBoundsException ex) {
			throw new RuntimeException("The COLUMNS specified for a field span beyond the record length of " + String.valueOf(record.length()));
		}
	}

	protected String getField(String record) {
		return record.substring(startColumn - 1, endColumn);
	}
}
