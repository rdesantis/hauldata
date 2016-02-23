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
	private Object example;

	private VariableType(String name, Object example) {
		this.name = name;
		this.example = example;
	}

	public static VariableType INTEGER = new VariableType("INTEGER", (Integer)0) { public Object getValueChecked(Object value) { return (Integer)value; } };
	public static VariableType VARCHAR = new VariableType("VARCHAR", "") { public Object getValueChecked(Object value) { return (String)value; } };
	public static VariableType DATETIME = new VariableType("DATETIME", LocalDateTime.MIN) { public Object getValueChecked(Object value) { return (LocalDateTime)value; } };
	public static VariableType BOOLEAN = new VariableType("BOOLEAN", (Boolean)false)  { public Object getValueChecked(Object value) { return (Boolean)value; } };

	/**
	 * @return the type name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @return an arbitrary example of a value of the type
	 */
	public Object getExample() {
		return example;
	}

	/**
	 * Casts an object to a value that can be stored in a variable of the type.
	 * Throws an exception if the value cannot be stored in the type.
	 * 
	 * @param value is the value to be cast
	 * @return the value cast to the type
	 */
	public abstract Object getValueChecked(Object value);

	/**
	 * Return the variable type that can be used to store a value
	 * @param example is an example of the value type to be stored
	 * @return the VariableType that can used to store the value or null if there is no such type 
	 */
	public static VariableType of(Object example) {
		return
				example instanceof Integer ? INTEGER :
				example instanceof String ? VARCHAR :
				example instanceof LocalDateTime ? DATETIME :
				null;
	}
}
