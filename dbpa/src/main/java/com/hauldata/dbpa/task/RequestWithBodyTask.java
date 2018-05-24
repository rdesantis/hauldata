/*
 * Copyright (c) 2017, 2018, Ronald DeSantis
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

package com.hauldata.dbpa.task;

import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.InputMismatchException;
import java.util.List;
import java.util.NoSuchElementException;

import com.hauldata.dbpa.expression.Expression;
import com.hauldata.util.tokenizer.JsonTokenizer;

public abstract class RequestWithBodyTask extends RequestTask {

	protected Expression<String> requestTemplate;

	public RequestWithBodyTask(
			Prologue prologue,
			Expression<String> url,
			List<Header> headers,
			Expression<String> requestTemplate,
			List<SourceWithAliases> sourcesWithAliases,
			Expression<String> responseTemplate,
			List<TargetWithKeepers> targetsWithKeepers) {
		super(prologue, url, headers, sourcesWithAliases, responseTemplate, targetsWithKeepers);

		this.requestTemplate = requestTemplate;
	}

	@Override
	protected JsonRequestTemplate prepareRequestTemplate(
			RequestTaskEvaluatedParameters parameters,
			List<List<String>> sourceColumnNames) throws InputMismatchException, IOException {

		return (new JsonRequestTemplateParser(((RequestWithBodyTaskEvaluatedParameters)parameters).requestTemplate, sourceColumnNames)).parse();
	}
}

/**
 * Derived class implementation for PUT and POST
 */
class RequestWithBodyTaskEvaluatedParameters extends RequestTaskEvaluatedParameters {

	public String requestTemplate;

	public RequestWithBodyTaskEvaluatedParameters(
			Expression<String> url,
			List<RequestTask.Header> headers,
			Expression<String> requestTemplate,
			List<RequestTask.SourceWithAliases> sourcesWithAliases,
			Expression<String> responseTemplate,
			List<RequestTask.TargetWithKeepers> targetsWithKeepers) {
		super(url, headers, sourcesWithAliases, responseTemplate, targetsWithKeepers);

		this.requestTemplate = requestTemplate.evaluate();
	}
}

class JsonRequestTemplateParser {

	private String requestTemplate;
	private List<List<String>> sourceColumnNames;

	private JsonTokenizer tokenizer = null;
	private ArrayList<JsonTemplateDynamicElement> dynamicElements = null;

	public JsonRequestTemplateParser(String requestTemplate, List<List<String>> sourceColumnNames) {
		this.requestTemplate = requestTemplate;
		this.sourceColumnNames = sourceColumnNames;
	}

	public JsonRequestTemplate parse() throws InputMismatchException, IOException {

		tokenizer = new JsonTokenizer(new StringReader(requestTemplate));
		tokenizer.useDelimiter("...");

		JsonRequestTemplate template = null;
		try {
			if (tokenizer.hasNextQuoted()) {
				// TEXT_PLAIN

				String requestBody = tokenizer.nextQuoted().getBody();
				if (tokenizer.hasNext()) {
					throw new RuntimeException("Unexpected token in request template");
				}

				if (sourceColumnNames.size() == 0) {
					template = new JsonStaticTextTemplate(requestBody);
				}
				else {
					template = new JsonDynamicTextTemplate(requestBody, sourceColumnNames);
				}
			}
			else {
				// APPLICATION_JSON

				dynamicElements = new ArrayList<JsonTemplateDynamicElement>();
				template = new JsonStructureTemplate(parseStructure(), dynamicElements, sourceColumnNames);
			}
		}
		finally {
			tokenizer.close();
		}

		return template;
	}

	private JsonTemplateSection parseStructure() throws InputMismatchException, IOException {

		JsonTemplateSection section = new JsonTemplateSection();

		if (tokenizer.hasNextDelimiter("[")) {
			parseArray(section);
		}
		else if (tokenizer.hasNextDelimiter("{")) {
			parseObject(section);
		}
		else {
			throw new RuntimeException("Request string is not valid JSON");
		}

		return section;
	}

	/**
	 * Parse an array.
	 *
	 * @param section is the section being built from the parsed structure.
	 * @return true if the array is dynamic or it contains a dynamic element.
	 */
	private boolean parseArray(JsonTemplateSection section) throws InputMismatchException, IOException {

		tokenizer.nextDelimiter("[");

		section.addLiteral("[");

		JsonTemplateSection potentialDynamicElement = new JsonTemplateSection();
		boolean containsDynamic = parseElement(potentialDynamicElement);

		boolean isDynamic = false;
		boolean hasMoreElements = false;

		if ((hasMoreElements = tokenizer.skipDelimiter(","))) {
			if ((isDynamic = tokenizer.hasNextDelimiter("..."))) {
				if (containsDynamic) {
					throw new RuntimeException("Dynamic structures cannot be nested in the JSON request template");
				}
				else {
					section.addElement(makeDynamicArrayElement(potentialDynamicElement));

					tokenizer.nextToken();

					hasMoreElements = false;
				}
			}
		}

		if (!isDynamic) {
			merge(section, potentialDynamicElement);

			if (hasMoreElements) {
				do {
					section.addLiteral(",");

					if (parseElement(section)) {
						containsDynamic = true;
					}
				} while (tokenizer.skipDelimiter(","));
			}
		}

		tokenizer.nextDelimiter("]");

		section.addLiteral("]");

		return isDynamic || containsDynamic;
	}

	/**
	 * Parse an object.
	 *
	 * @param section is the section being built from the parsed structure.
	 * @return true if the object is dynamic or it contains a dynamic element.
	 */
	private boolean parseObject(JsonTemplateSection section) throws InputMismatchException, IOException {

		tokenizer.nextDelimiter("{");

		section.addLiteral("{");

		JsonTemplateSection potentialDynamicElement = new JsonTemplateSection();
		DynamicStatus dynamicStatus = parseKeyValue(potentialDynamicElement, true);
		boolean containsDynamic = dynamicStatus.isValueDynamic;

		boolean isDynamic = false;
		boolean hasMoreElements = false;

		if ((hasMoreElements = tokenizer.skipDelimiter(","))) {
			if ((isDynamic = tokenizer.hasNextDelimiter("..."))) {
				if (containsDynamic) {
					throw new RuntimeException("Dynamic structures cannot be nested in the JSON request template");
				}
				else if (!dynamicStatus.isKeyDynamic) {
					throw new RuntimeException("A dynamic object in the JSON request template must start with a KEEP name from a JOIN clause");
				}
				else {
					section.addElement(makeDynamicObjectElement(potentialDynamicElement));

					tokenizer.nextToken();

					hasMoreElements = false;
				}
			}
		}

		if (!isDynamic) {
			if (dynamicStatus.isKeyDynamic) {
				throw new RuntimeException("An object in the JSON request template that starts with a KEEP name from a JOIN clause must be a dynamic object");
			}
			else {
				merge(section, potentialDynamicElement);

				if (hasMoreElements) {
					do {
						section.addLiteral(",");

						if (parseKeyValue(potentialDynamicElement, false).isValueDynamic) {
							containsDynamic = true;
						}
					} while (tokenizer.skipDelimiter(","));
				}
			}
		}

		tokenizer.nextDelimiter("}");

		section.addLiteral("}");

		return isDynamic || containsDynamic;
	}

	/**
	 * Parse a JSON "key : value" pair.
	 */
	private DynamicStatus parseKeyValue(JsonTemplateSection section, boolean allowName) throws InputMismatchException, IOException {

		boolean isKeyDynamic = parseKey(section, allowName);

		tokenizer.nextDelimiter(":");

		boolean isValueDynamic = parseElement(section);

		return new DynamicStatus(isKeyDynamic, isValueDynamic);
	}

	/**
	 * Parse the key of a JSON key:value pair.
	 *
	 * @return true if the key is an unquoted name.
	 */
	private boolean parseKey(JsonTemplateSection section, boolean allowName)  throws InputMismatchException, NoSuchElementException, IOException {

		if (tokenizer.hasNextQuoted()) {

			section.addLiteral(tokenizer.nextQuoted().getImage());

			return false;
		}
		else if (tokenizer.hasNextWord()) {

			String name = tokenizer.nextWord();

			if (!allowName) {
				throw new RuntimeException("An unquoted field name in the JSON request template can only be used in the first field of a dynamic object: " + name);
			}

			section.addElement(makeIdentifier(name));

			return true;
		}
		else {
			throw new RuntimeException("Unexpected token in the JSON request template where a field name is expected: " + tokenizer.nextToken().getImage());
		}
	}

	/**
	 * Parse a JSON element.
	 *
	 * @return true if the element is dynamic or it contains a dynamic element
	 */
	private boolean parseElement(JsonTemplateSection section) throws InputMismatchException, IOException {

		if (tokenizer.hasNextDelimiter("[")) {
			return parseArray(section);
		}
		else if (tokenizer.hasNextDelimiter("{")) {
			return parseObject(section);
		}
		else if (tokenizer.hasNextWord()) {
			section.addElement(makeIdentifier(tokenizer.nextWord()));
		}
		else {
			section.addLiteral(tokenizer.nextToken().getImage());
		}
		return false;
	}

	/**
	 * Merge the wrongly-diagnosed potential dynamic elements into the enclosing section.
	 */
	private void merge(JsonTemplateSection section, JsonTemplateSection potentialDynamicElement) {

		// Note that there is always one more literal than there are elements in a section.

		section.addLiteral(potentialDynamicElement.literals.get(0));

		int i = 0;
		while (i < section.elements.size()) {
			section.addElement(section.elements.get(i));
			section.addLiteral(potentialDynamicElement.literals.get(++i));
		}
	}

	/**
	 * @throws RuntimeException if the word is not a KEEP name
	 */
	private JsonTemplateIdentifier makeIdentifier(String word) {

		int sourceIndex = 0;
		int columnIndex = -1;
		for (; sourceIndex < sourceColumnNames.size(); ++sourceIndex) {
			if (-1 < (columnIndex = sourceColumnNames.get(sourceIndex).indexOf(word))) {
				break;
			}
		}
		if (columnIndex == -1) {
			throw new RuntimeException("An unquoted field name in the JSON request template must be a KEEP name: " + word);
		}

		return new JsonTemplateIdentifier(sourceIndex, columnIndex);
	}

	private JsonTemplateDynamicArrayElement makeDynamicArrayElement(JsonTemplateSection potentialDynamicElement) {

		JsonTemplateDynamicArrayElement dynamicElement = new JsonTemplateDynamicArrayElement(potentialDynamicElement);
		addDynamicElement(dynamicElement);
		return dynamicElement;
	}

	private JsonTemplateDynamicObjectElement makeDynamicObjectElement(JsonTemplateSection potentialDynamicElement) {

		JsonTemplateDynamicObjectElement dynamicElement =
				new JsonTemplateDynamicObjectElement((JsonTemplateIdentifier)potentialDynamicElement.elements.get(0), potentialDynamicElement.elements.get(1));
		addDynamicElement(dynamicElement);
		return dynamicElement;
	}

	/**
	 * Validate that the dynamic element makes valid references to column names
	 * and then add it to the list.
	 */
	private void addDynamicElement(JsonTemplateDynamicElement dynamicElement) {

		DynamicElementColumnInspector inspector = new DynamicElementColumnInspector();
		dynamicElement.visitIdentifiers(inspector);

		if (!inspector.referencedSource_0_ && !sourceColumnNames.get(0).isEmpty()) {
			throw new RuntimeException("A dynamic structure in the JSON request must reference at least one FROM column");
		}

		if (inspector.sourceIndex == -1) {
			throw new RuntimeException("A dynamic structure in the JSON request must reference at least one JOIN column");
		}

		for (JsonTemplateDynamicElement previousDynamicElement : dynamicElements) {
			if (inspector.sourceIndex == previousDynamicElement.sourceIndex) {
				throw new RuntimeException("Two dynamic structures in the JSON request cannot reference columns from the same JOIN");
			}
		}

		dynamicElement.sourceIndex = inspector.sourceIndex;
		dynamicElements.add(dynamicElement);
	}

	private static class DynamicElementColumnInspector implements JsonTemplateElement.IdentifierVisitor {
		public boolean referencedSource_0_ = false;
		public int sourceIndex = -1;

		public void visit(JsonTemplateIdentifier identifier) {
			if (identifier.sourceIndex == 0) {
				referencedSource_0_ = true;
			}
			else if (sourceIndex == -1) {
				sourceIndex = identifier.sourceIndex;
			}
			else if (sourceIndex != identifier.sourceIndex) {
				throw new RuntimeException("A dynamic structure in the JSON request cannot reference more than one JOIN column");
			}
		}
	}
}

class DynamicStatus {
	public boolean isKeyDynamic;
	public boolean isValueDynamic;

	DynamicStatus(boolean keyIsDynamic, boolean valueIsDynamic) {
		this.isKeyDynamic = keyIsDynamic;
		this.isValueDynamic = valueIsDynamic;
	}
}

abstract class JsonSingleRowRequestTemplate implements JsonRequestTemplate {
	private boolean hasNext = true;

	@Override
	public boolean next(RequestTaskEvaluatedParameters parameters) throws SQLException {
		boolean hadNext = hasNext;
		hasNext = false;
		return hadNext;
	}
}

class JsonStaticTextTemplate extends JsonSingleRowRequestTemplate {
	public String requestBody;

	public JsonStaticTextTemplate(String requestBody) {
		this.requestBody = requestBody;
	}

	@Override
	public String composeRequest(RequestTaskEvaluatedParameters parameters) {
		return requestBody;
	}
}

abstract class JsonMultiRowRequestTemplate implements JsonRequestTemplate {

	@Override
	public boolean next(RequestTaskEvaluatedParameters parameters) throws SQLException, InterruptedException {
		return parameters.getSource(0).next();
	}
}

class JsonDynamicTextTemplate extends JsonMultiRowRequestTemplate {
	public int columnIndex;

	public JsonDynamicTextTemplate(String name, List<List<String>> sourceColumnNames) {

		columnIndex = sourceColumnNames.get(0).indexOf(name.trim());
		if (columnIndex == -1) {
			throw new RuntimeException("Request column name not found: " + name);
		}
	}

	@Override
	public String composeRequest(RequestTaskEvaluatedParameters parameters) throws SQLException {
		return parameters.getSource(0).getObject(columnIndex).toString();
	}
}

class JsonStructureTemplate extends JsonMultiRowRequestTemplate {

	// The dynamicElements list is sorted by sourceIndex with a null element at the head so that
	// the index of each element within dynamicElements matches its sourceIndex.

	public JsonTemplateSection definition;
	public ArrayList<JsonTemplateDynamicElement> dynamicElements;

	public JsonStructureTemplate(
			JsonTemplateSection definition,
			ArrayList<JsonTemplateDynamicElement> dynamicElements, List<List<String>> sourceColumnNames) {

		if (dynamicElements.size() != (sourceColumnNames.size() - 1)) {
			throw new RuntimeException("There must be exactly one dynamic structure in the JSON request for each JOIN");
		}

		Collections.sort(dynamicElements, (a, b) -> Integer.compare(a.sourceIndex, b.sourceIndex));
		dynamicElements.add(0, null);

		this.definition = definition;
		this.dynamicElements = dynamicElements;
	}

	@Override
	public String composeRequest(RequestTaskEvaluatedParameters parameters) throws SQLException, InterruptedException {

		StringBuffer image = new StringBuffer();
		definition.render(parameters.sourcesWithAliases, image);
		return image.toString();
	}
}

interface JsonTemplateElement {
	void visitIdentifiers(IdentifierVisitor visitor);

	@FunctionalInterface
	public static interface IdentifierVisitor {
		void visit(JsonTemplateIdentifier identifier);
	}

	void render(List<RequestTaskEvaluatedParameters.SourceWithAliases> sourcesWithAliases, StringBuffer image) throws SQLException, InterruptedException;
}

class JsonTemplateSection implements JsonTemplateElement {
	public int sourceIndex = -1;

	// Literals and elements alternate in a section.  A section always starts and ends with a literal.
	// Therefore, there is always one more literal than elements.

	public ArrayList<String> literals = new ArrayList<String>();
	public ArrayList<JsonTemplateElement> elements = new ArrayList<JsonTemplateElement>();

	public JsonTemplateSection() {
		// Be sure that the section always starts with a literal, even if it is an empty string literal.
		literals.add("");
	}

	public void addLiteral(String literal) {
		int last = literals.size() - 1;
		literals.set(last, literals.get(last) + literal);
	}

	public void addElement(JsonTemplateElement element) {
		elements.add(element);
		// Be sure that an element is always followed by a literal, even if it is an empty string literal.
		literals.add("");
	}

	@Override
	public void visitIdentifiers(IdentifierVisitor visitor) {
		for (JsonTemplateElement element : elements) {
			element.visitIdentifiers(visitor);
		}
	}

	@Override
	public void render(List<RequestTaskEvaluatedParameters.SourceWithAliases> sourcesWithAliases, StringBuffer image) throws SQLException, InterruptedException {

		int i = 0;
		for (; i < elements.size(); ++i) {
			image.append(literals.get(i));
			elements.get(i).render(sourcesWithAliases, image);
		}
		image.append(literals.get(i));
	}
}

abstract class JsonTemplateDynamicElement implements JsonTemplateElement {
	public int sourceIndex = -1;

	@Override
	public void render(List<RequestTaskEvaluatedParameters.SourceWithAliases> sourcesWithAliases, StringBuffer image) throws SQLException, InterruptedException {

		boolean isHead = true;
		while (next(sourcesWithAliases.get(0), sourcesWithAliases.get(sourceIndex))) {
			if (isHead) {
				isHead = false;
			}
			else {
				image.append(",");
			}
			renderElement(sourcesWithAliases, image);
		}
	}

	protected abstract void renderElement(List<RequestTaskEvaluatedParameters.SourceWithAliases> sourcesWithAliases, StringBuffer image) throws SQLException, InterruptedException;

	private boolean isFirst = true;
	private boolean hasNext = false;

	private boolean next(
			RequestTaskEvaluatedParameters.SourceWithAliases fromSource,
			RequestTaskEvaluatedParameters.SourceWithAliases joinSource) throws SQLException, InterruptedException {

		if (isFirst) {
			hasNext = joinSource.source.next();
			isFirst = false;
		}

		boolean nextIsJoined = false;
		while (hasNext && !(nextIsJoined = areJoined(fromSource, joinSource))) {
			hasNext = joinSource.source.next();
		}

		return nextIsJoined;
	}

	/**
	 * @return true if all like-named columns have equal values
	 */
	private boolean areJoined(
			RequestTaskEvaluatedParameters.SourceWithAliases fromSource,
			RequestTaskEvaluatedParameters.SourceWithAliases joinSource) throws SQLException {

		for (int fromColumnIndex = 0; fromColumnIndex < fromSource.columnNameAliases.size(); ++fromColumnIndex) {

			String fromColumnName = fromSource.columnNameAliases.get(fromColumnIndex);
			int joinColumnIndex = joinSource.columnNameAliases.indexOf(fromColumnName);

			if (joinColumnIndex != -1) {

				Object fromValue = fromSource.source.getObject(fromColumnIndex + 1);
				Object joinValue = joinSource.source.getObject(joinColumnIndex + 1);

				if (!fromValue.equals(joinValue)) {
					return false;
				}
			}
		}
		return true;
	}
}

class JsonTemplateDynamicArrayElement extends JsonTemplateDynamicElement {
	private JsonTemplateElement element;

	public JsonTemplateDynamicArrayElement(JsonTemplateElement element) {
		this.element = element;
	}

	@Override
	public void visitIdentifiers(IdentifierVisitor visitor) {
		element.visitIdentifiers(visitor);
	}

	@Override
	protected void renderElement(
			List<RequestTaskEvaluatedParameters.SourceWithAliases> sourcesWithAliases,
			StringBuffer image) throws SQLException, InterruptedException {

		element.render(sourcesWithAliases, image);
	}
}

class JsonTemplateDynamicObjectElement extends JsonTemplateDynamicElement {
	private JsonTemplateIdentifier fieldName;
	private JsonTemplateElement element;

	public JsonTemplateDynamicObjectElement(JsonTemplateIdentifier fieldName, JsonTemplateElement element) {
		this.fieldName = fieldName;
		this.element = element;
	}

	@Override
	public void visitIdentifiers(IdentifierVisitor visitor) {
		fieldName.visitIdentifiers(visitor);
		element.visitIdentifiers(visitor);
	}

	@Override
	protected void renderElement(
			List<RequestTaskEvaluatedParameters.SourceWithAliases> sourcesWithAliases,
			StringBuffer image) throws SQLException, InterruptedException {

		fieldName.render(sourcesWithAliases, image);
		image.append(":");
		element.render(sourcesWithAliases, image);
	}
}

class JsonTemplateIdentifier implements JsonTemplateElement {
	public int sourceIndex;
	public int columnIndex;

	public JsonTemplateIdentifier(int sourceIndex, int columnIndex) {
		this.sourceIndex = sourceIndex;
		this.columnIndex = columnIndex;
	}

	@Override
	public void visitIdentifiers(IdentifierVisitor visitor) {
		visitor.visit(this);
	}

	@Override
	public void render(List<RequestTaskEvaluatedParameters.SourceWithAliases> sourcesWithAliases, StringBuffer image) throws SQLException {
		image.append(sourcesWithAliases.get(sourceIndex).source.getObject(columnIndex).toString());
	}
}
