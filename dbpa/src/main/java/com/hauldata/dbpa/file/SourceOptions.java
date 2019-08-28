/*
 * Copyright (c) 2019, Ronald DeSantis
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

package com.hauldata.dbpa.file;

import java.util.HashMap;
import java.util.Map;

import com.hauldata.dbpa.expression.Expression;

public class SourceOptions implements PageOptions {

	public static final SourceOptions DEFAULT = new SourceOptions();

	private Expression<Integer> batchSize = null;

	public Integer getBatchSize() { return (batchSize != null) ? batchSize.evaluate() : null; }

	public static class Parser extends PageOptionsParser {

		static Map<String, Modifier> modifiers;

		static {
			modifiers = new HashMap<String, Modifier>();
			modifiers.put("BATCH SIZE", (parser, options) -> {((SourceOptions)options).batchSize = parser.parseIntegerExpression();});
		}

		public Parser() {
			super(modifiers);
		}

		protected Parser(Map<String, PageOptions.Modifier> subModifiers) {
			super(combine(modifiers, subModifiers));
		}

		@Override
		protected PageOptions makeDefaultOptions() {
			return new SourceOptions();
		}
	}
}
