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

package com.hauldata.dbpa.variable;

import java.time.LocalDateTime;

/**
 * Variable type
 *
 * This class cannot be directly instantiated.  Instances are only available through the static data members
 * that correspond to each supported variable type: INTEGER, VARCHAR, DATETIME, and BOOLEAN.
 */
public abstract class VariableType {

	private String name;

	private VariableType(String name) {
		this.name = name;
	}

	public boolean isCompatibleWith(VariableType type) {
		return (type == this);
	}

	public static final VariableType VARCHAR = new VariableType("VARCHAR") { public Object getValueChecked(Object value) { return (String)value; } };
	public static final VariableType DATETIME = new VariableType("DATETIME") { public Object getValueChecked(Object value) { return (LocalDateTime)value; } };
	public static final VariableType BOOLEAN = new VariableType("BOOLEAN") { public Object getValueChecked(Object value) { return (Boolean)value; } };
	public static final VariableType VALUES = new VariableType("VALUES") { public Object getValueChecked(Object value) { return (Values)value; } };

	private static abstract class IntegerType extends VariableType {

		private IntegerType(String name) { super(name); }

		@Override
		public boolean isCompatibleWith(VariableType type) {
			return (type == INTEGER) || (type == BIT);
		}

		protected Integer getInteger(Object value) {
			return
					(value == null) ? null :
					value instanceof Integer ? (Integer)value :
					(Integer)((Number)value).intValue();
		}
	}

	public static final VariableType INTEGER = new IntegerType("INTEGER") { public Object getValueChecked(Object value) { return getInteger(value); } };

	public static final VariableType BIT = new IntegerType("BIT") {
		public Object getValueChecked(Object value) {
			Integer result = getInteger(value);
			return
					(result == null) ? null :
					(result == 0) ? result :
					(Integer)1;
			}
	};

	/**
	 * @return the type name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Converts an object to a value that can be stored in a variable of the type.
	 * Throws an exception if the value cannot be stored in the type.
	 * 
	 * @param value is the value to be cast
	 * @return the value cast to the type
	 */
	public abstract Object getValueChecked(Object value);
}
