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

package com.hauldata.dbpa.file;

import java.util.HashMap;
import java.util.Map;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.variable.Variable;

public class Html implements PageNode {

	public static class TargetOptions implements FileOptions {

		private Expression<String> tableStyle = null;
		private Expression<String> headStyle = null;
		private Expression<String> bodyStyle = null;
		private Expression<String> cellStyle = null;
		
		public String getTableStyle() { return evaluate(tableStyle); }
		public String getHeadStyle() { return evaluate(headStyle); }
		public String getBodyStyle() { return evaluate(bodyStyle); }
		public String getCellStyle() { return evaluate(cellStyle); }

		private static String evaluate(Expression<String> expression) {
			return (expression != null) ? expression.evaluate() : null;
		}

		public static class Parser extends FileOptionsParser {

			static Map<String, Modifier> modifiers;

			static {
				modifiers = new HashMap<String, Modifier>();
				modifiers.put("TABLE STYLE", (parser, options) -> {((TargetOptions)options).tableStyle = parser.parseStringExpression();});
				modifiers.put("HEAD STYLE", (parser, options) -> {((TargetOptions)options).headStyle = parser.parseStringExpression();});
				modifiers.put("BODY STYLE", (parser, options) -> {((TargetOptions)options).bodyStyle = parser.parseStringExpression();});
				modifiers.put("CELL STYLE", (parser, options) -> {((TargetOptions)options).cellStyle = parser.parseStringExpression();});
			}

			public Parser() {
				super(modifiers);
			}

			@Override
			protected FileOptions makeDefaultOptions() {
				return new TargetOptions();
			}
		}
	}

	private Variable<String> variable;
	private TargetOptions options;
	private Headers headers;

	private int rowIndex;
	private boolean isHeader;
	private StringBuilder content;

	public Html(Variable<String> variable, TargetOptions options) {
		this.variable = variable;
		this.options = options;

		rowIndex = 0;
		isHeader = false;
		content = new StringBuilder();
	}

	@Override
	public void setHeaders(Headers headers) {
		this.headers = headers;
	}

	@Override
	public SourceHeaders getSourceHeaders() {
		return (SourceHeaders)headers;
	}

	@Override
	public TargetHeaders getTargetHeaders() {
		return (TargetHeaders)headers;
	}

	@Override
	public String getName() {
		return variable.getName();
	}

	@Override
	public void writeColumn(int columnIndex, Object object) {

		if (columnIndex == 1) {
			rowIndex++;
			isHeader = false;

			if (rowIndex == 1) {
				String cellStyle = options.getCellStyle();
				if (cellStyle != null) {
					startTag("style", null, true);
					content.append("th, td {");
					content.append(cellStyle);
					content.append("}");
					endLine();
					endTag("style");
				}

				startTag("table", options.getTableStyle(), true);
				if (headers.exist()) {
					startTag("thead", options.getHeadStyle(), true);
					isHeader = true;
				}
				else {
					startTag("tbody", options.getBodyStyle(), true);
				}
			}
			else {
				endTag("tr");
				if ((rowIndex == 2) && headers.exist()) {
					endTag("thead");
					startTag("tbody", options.getBodyStyle(), true);
				}
			}

			boolean rowNeedsStartTag = true;
			if (object instanceof String) {
				String value = (String)object;
				if (value.startsWith("<tr")) {
					int endTagIndex = value.indexOf('>') + 1;
					content.append(value.substring(0, endTagIndex));
					endLine();
					object = value.substring(endTagIndex);
					rowNeedsStartTag = false;
				}
			}
			if (rowNeedsStartTag){
				startTag("tr", null, true);
			}
		}

		String columnTag = isHeader ? "th" : "td";
		boolean columnNeedsStartTag = true;
		if (object instanceof String) {
			String value = (String)object;
			if (value.startsWith("<" + columnTag)) {
				columnNeedsStartTag = false;
			}
		}
		if (columnNeedsStartTag){
			startTag(columnTag, null, false);
		}
		content.append(object.toString());
		endTag(columnTag);
	}

	private void startTag(String tag, String style, boolean eol) {
		
		content.append("<");
		content.append(tag);
		if (style != null) {
			content.append(" style=\"");
			content.append(style);
			content.append("\"");
		}
		content.append(">");
		if (eol) {
			endLine();
		}
	}

	private void endTag(String tag) {
		
		content.append("</");
		content.append(tag);
		content.append(">");
		endLine();
	}

	private void endLine() {
		content.append(String.format("%n"));
	}

	@Override
	public void flush() {

		if (0 < rowIndex) {
			endTag("tr");
		}
		if ((rowIndex == 1) && headers.exist()) {
			endTag("thead");
		}
		else {
			endTag("tbody");
		}
		endTag("table");

		variable.setValue(content.toString());
	}

	@Override
	public void close() {}

	// Never called.

	@Override public Object readColumn(int columnIndex) { return null; }
	@Override public boolean hasRow() { return false; }
}
