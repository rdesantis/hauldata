/*
 * Copyright (c) 2017, Ronald DeSantis
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

package com.hauldata.dbpa.file.html;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.file.PageOptions;
import com.hauldata.dbpa.file.PageOptionsParser;
import com.hauldata.dbpa.file.PageOptions.Modifier;

public class HtmlOptions implements PageOptions {

	private Expression<String> tableStyle = null;
	private Expression<String> headStyle = null;
	private Expression<String> bodyStyle = null;
	private Expression<String> headCellStyle = null;
	private Expression<String> bodyCellStyle = null;

	private String evaluatedHeadCellStyle = null;
	private String evaluatedBodyCellStyle = null;

	public String getTableStyle() { return evaluate(tableStyle); }
	public String getHeadStyle() { return evaluate(headStyle); }
	public String getBodyStyle() { return evaluate(bodyStyle); }
	public String getHeadCellStyle() { return Optional.ofNullable(evaluatedHeadCellStyle).orElse((evaluatedHeadCellStyle = evaluate(headCellStyle))); }
	public String getBodyCellStyle() { return Optional.ofNullable(evaluatedBodyCellStyle).orElse((evaluatedBodyCellStyle = evaluate(bodyCellStyle))); }

	private static String evaluate(Expression<String> expression) {
		return (expression != null) ? expression.evaluate() : null;
	}

	public static class Parser extends PageOptionsParser {

		static Map<String, Modifier> modifiers;

		static {
			modifiers = new HashMap<String, Modifier>();
			modifiers.put("TABLE STYLE", (parser, options) -> {((HtmlOptions)options).tableStyle = parser.parseStringExpression();});
			modifiers.put("HEAD STYLE", (parser, options) -> {((HtmlOptions)options).headStyle = parser.parseStringExpression();});
			modifiers.put("BODY STYLE", (parser, options) -> {((HtmlOptions)options).bodyStyle = parser.parseStringExpression();});
			modifiers.put("HEAD CELL STYLE", (parser, options) -> {((HtmlOptions)options).headCellStyle = parser.parseStringExpression();});
			modifiers.put("BODY CELL STYLE", (parser, options) -> {((HtmlOptions)options).bodyCellStyle = parser.parseStringExpression();});
			modifiers.put("CELL STYLE", (parser, options) ->
					{((HtmlOptions)options).bodyCellStyle = ((HtmlOptions)options).headCellStyle = parser.parseStringExpression();});
		}

		public Parser() {
			super(modifiers);
		}

		protected Parser(Map<String, Modifier> subModifiers) {
			super(combine(modifiers, subModifiers));
		}

		@Override
		protected PageOptions makeDefaultOptions() {
			return new HtmlOptions();
		}
	}
}