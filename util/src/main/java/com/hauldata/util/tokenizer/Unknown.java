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

package com.hauldata.util.tokenizer;

public class Unknown extends StringToken {

	public Unknown(boolean leadingWhitespace, String value) {
		super(leadingWhitespace, value);
	}

	public static final Unknown hasWhitespace = new Unknown(true, null);
	public static final Unknown noWhitespace = new Unknown(false, null);

	public static Unknown withWhitespace(boolean leadingWhitespace) {
		return leadingWhitespace ? hasWhitespace : noWhitespace;
	}

	@Override
	public boolean equals(Object obj) {
		return (obj != null) && (obj instanceof Unknown) && ((Unknown)obj).getValue().equals(getValue()); 
	}
}
