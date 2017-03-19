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

package com.hauldata.dbpa.task;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.stream.JsonParser;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.hauldata.dbpa.datasource.DataSource;
import com.hauldata.dbpa.datasource.DataTarget;
import com.hauldata.dbpa.datasource.Source;
import com.hauldata.dbpa.datasource.TableDataTarget;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.file.Columns;
import com.hauldata.dbpa.file.SourceHeaders;
import com.hauldata.dbpa.process.Context;

public abstract class RequestTask extends Task {

	public static final ContentType supportedContentType = ContentType.APPLICATION_JSON;
	public static final int maxResponseContentLength = 4096;

	public enum ResponseType { value, object, list, map };

	public static class Parameters {

		public static class Header {
			public Expression<String> name;
			public boolean isNullable;
			public Expression<String> value;

			public Header(
					Expression<String> name,
					boolean isNullable,
					Expression<String> value) {

				this.name = name;
				this.isNullable = isNullable;
				this.value = value;
			}
		}

		private Expression<String> url;
		private List<Header> headers;

		private List<Expression<String>> requestFields;
		private List<Expression<String>> keepFields;
		private List<Expression<String>> responseFields;
		private Expression<String> statusField;
		private Expression<String> messageField;

		private ResponseType responseType;

		public Parameters() {}

		public void add(
				Expression<String> url,
				List<Header> headers) {

			this.url = url;
			this.headers = headers;
		}

		public void add(
				List<Expression<String>> requestFields,
				List<Expression<String>> keepFields,
				List<Expression<String>> responseFields,
				Expression<String> statusField,
				Expression<String> messageField,
				ResponseType responseType) {

			this.requestFields = requestFields;
			this.keepFields = keepFields;
			this.responseFields = responseFields;
			this.statusField = statusField;
			this.messageField = messageField;

			this.responseType = responseType;
		}

		public Expression<String> getUrl() { return url; }
		public List<Header> getHeaders() { return headers; }
		public List<Expression<String>> getRequestFields() { return requestFields; }
		public List<Expression<String>> getKeepFields() { return keepFields; }
		public List<Expression<String>> getResponseFields() { return responseFields; }
		public Expression<String> getStatusField() { return statusField; }
		public Expression<String> getMessageField() { return messageField; }

		public ResponseType getResponseType() { return responseType; }
	}

	private Source source;
	private Parameters parameters;
	private DataTarget target;

	protected RequestTask(
			Prologue prologue,
			Source source,
			Parameters parameters,
			DataTarget target) {
		super(prologue);

		this.source = source;
		this.parameters = parameters;
		this.target = target;
	}

	@Override
	protected void execute(Context context) throws Exception {

		EvaluatedParameters evaluatedParameters = evaluateParameters(parameters);

		executeRequest(context, source, evaluatedParameters, target, parameters.getResponseType());
	}

	protected static class EvaluatedParameters {

		public static class Header {
			public String name;
			public String value;

			public Header(
					String name,
					String value) {

				this.name = name;
				this.value = value;
			}
		}

		private String url;
		private List<Header> headers;

		private List<String> requestFields;
		private Set<Integer> substituteIndexes;
		private List<Integer> keepIndexes;
		private List<String> responseFields;
		private String statusField;
		private String messageField;


		public EvaluatedParameters() {}

		public void add(
				String url,
				List<Header> headers,

				List<String> requestFields,
				Set<Integer> substituteIndexes,
				List<Integer> keepIndexes,
				List<String> responseFields,
				String statusField,
				String messageField) {

			this.url = url;
			this.headers = headers;

			this.requestFields = requestFields;
			this.substituteIndexes = substituteIndexes;
			this.keepIndexes = keepIndexes;
			this.responseFields = responseFields;
			this.statusField = statusField;
			this.messageField = messageField;
		}

		public String getUrl() { return url; }
		public List<Header> getHeaders() { return headers; }

		public List<String> getRequestFields() { return requestFields; }
		public Set<Integer> getSubstituteIndexes() { return substituteIndexes; }
		public List<Integer> getKeepIndexes() { return keepIndexes; }
		public List<String> getResponseFields() { return responseFields; }
		public String getStatusField() { return statusField; }
		public String getMessageField() { return messageField; }
	}

	private EvaluatedParameters evaluateParameters(Parameters rawParameters) {

		EvaluatedParameters parameters = makeEvaluatedParameters();

		evaluateBody(rawParameters, parameters);

		// Evaluate base parameters.

		String url = rawParameters.getUrl().evaluate();
		if (url == null) {
			throw new RuntimeException("URL evaluates to NULL");
		}

		List<EvaluatedParameters.Header> headers = new LinkedList<EvaluatedParameters.Header>();
		if (rawParameters.getHeaders() != null) {

			for (Parameters.Header header : rawParameters.getHeaders()) {

				String name = header.name.evaluate();
				String value = header.value.evaluate();
				if (name != null) {
					headers.add(new EvaluatedParameters.Header(name, value));
				}
				else if (!header.isNullable) {
					throw new RuntimeException("Non-nullable header name evaluates to NULL");
				}
			}
		}

		List<String> requestFields = new ArrayList<String>();
		for (Expression<String> requestField : rawParameters.getRequestFields()) {
			requestFields.add(requestField.evaluate());
		}

		if (!allFieldsAreUnique(requestFields)) {
			throw new RuntimeException("Request fields do not evaluate to unique values");
		}

		Set<Integer> substituteIndexes = new TreeSet<Integer>();
		for (int i = 0; i < requestFields.size(); ++i) {

			String requestField = requestFields.get(i);
			if ((requestField != null) && url.contains("{" + requestField + "}")) {
				substituteIndexes.add(i + 1);
			}
		}

		List<Integer> keepIndexes = new LinkedList<Integer>();
		if (rawParameters.getKeepFields() != null) {
			// Respect KEEP clause.

			for (Expression<String> keepFieldExpression : rawParameters.getKeepFields()) {

				String keepField = keepFieldExpression.evaluate();
				if (keepField == null) {
					throw new RuntimeException("KEEP field evaluates to NULL");
				}

				int requestFieldIndex = requestFields.indexOf(keepField);
				int bodyFieldIndex = getBodyFieldsIndexOf(parameters, keepField);
				int fieldIndex;

				if (0 <= requestFieldIndex && 0 <= bodyFieldIndex) {
					throw new RuntimeException("KEEP field is ambiguous; name appears as both a request field and body field value");
				}
				else if (0 <= requestFieldIndex) {
					fieldIndex = requestFieldIndex;
				}
				else if (0 <= bodyFieldIndex) {
					fieldIndex = requestFields.size() + bodyFieldIndex;
				}
				else {
					throw new RuntimeException("KEEP field does not evaluate to a request or body field value");
				}

				keepIndexes.add(fieldIndex + 1);
			}
		}
		else {
			// Keep all non-NULL named request fields.

			for (int i = 0; i < requestFields.size(); ++i) {
				String requestField = requestFields.get(i);
				if (requestField != null) {
					keepIndexes.add(i + 1);
				}
			}
		}

		List<String> responseFields = new ArrayList<String>();
		if (rawParameters.getResponseFields() != null) {

			for (Expression<String> responseFieldExpression : rawParameters.getResponseFields()) {

				String responseField = responseFieldExpression.evaluate();
				if (responseField == null) {
					throw new RuntimeException("RESPONSE field evaluates to NULL");
				}
				responseFields.add(responseField);
			}
		}

		String statusField = rawParameters.getStatusField().evaluate();
		if (statusField == null) {
			throw new RuntimeException("STATUS field evaluates to NULL");
		}

		String messageField = null;
		if (rawParameters.getMessageField() != null) {
			messageField = rawParameters.getMessageField().evaluate();
		}

		// Bundle up the parameters.

		parameters.add(url, headers, requestFields, substituteIndexes, keepIndexes, responseFields, statusField, messageField);

		return parameters;
	}

	protected EvaluatedParameters makeEvaluatedParameters() { return new EvaluatedParameters(); }

	protected void evaluateBody(Parameters baseRawParameters, EvaluatedParameters baseEvaluatedParameters) {}

	protected int getBodyFieldsIndexOf(EvaluatedParameters baseEvaluatedParameters, String keepField) { return -1; }

	protected boolean allFieldsAreUnique(List<String> fields) {
		return fields.stream().collect(Collectors.toSet()).size() == fields.size();
	}

	private void executeRequest(
			Context context,
			Source source,
			EvaluatedParameters parameters,
			DataTarget target,
			ResponseType responseType) {

		int sourceColumnCount = getSourceColumnCount(parameters);
		int targetParameterCount = getTargetParameterCount(parameters);

		CloseableHttpClient client = null;
		HttpRequestBase request = null;
		CloseableHttpResponse response = null;

		try {
			// Prepare to write target rows.

			SourceHeaders headers = null;
			Columns columns = null;

			if (target instanceof TableDataTarget) {
				// Prepare to write to table.

				ArrayList<String> captions = new ArrayList<String>();

				for (int keepIndex : parameters.getKeepIndexes()) {

					List<String> fields;
					int fieldIndex;
					if (keepIndex <= parameters.getRequestFields().size()) {
						fields = parameters.getRequestFields();
						fieldIndex = keepIndex;
					}
					else {
						fields = getBodyFields(parameters);
						fieldIndex = keepIndex - parameters.getRequestFields().size();
					}
					captions.add(fields.get(fieldIndex - 1));
				}

				for (String responseField : parameters.getResponseFields()) {
					captions.add(responseField);
				}

				captions.add(parameters.getStatusField());

				if (parameters.getMessageField() != null) {
					captions.add(parameters.getMessageField());
				}

				headers = new SourceHeaders(true, false, false, captions);
				columns = new Columns(null, headers);
			}

			target.prepareStatement(context, headers, columns);

			if (target.getParameterCount() != targetParameterCount) {
				throw new RuntimeException("Wrong number of target parameters");
			}

			// Prepare to issue HTTP requests.

			client = HttpClients.createDefault();

			// Loop over source rows.

			source.executeQuery(context);

			if (source.getColumnCount() != sourceColumnCount) {
				throw new RuntimeException("Wrong number of source columns");
			}

			while (source.next()) {

				request = buildRequest(parameters, source);

				// Execute the request and interpret the response to build the target row.

				response = client.execute(request);

				int statusCode = response.getStatusLine().getStatusCode();

				if (200 <= statusCode && statusCode < 300) {
					interpretResponse(statusCode, source, parameters, target, response, responseType);
				}
				else {
					interpretFailedResponse(statusCode, source, parameters, target, response);
				}

				response.close();
				response = null;
			}

			target.executeBatch();

			source.done(context);
		}
		catch (SQLException ex) {
			DataSource.throwDatabaseExecutionFailed(ex);
		}
		catch (InterruptedException ex) {
			throw new RuntimeException("Web service request was terminated due to interruption");
		}
		catch (URISyntaxException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Invalid resolved URL: " + message);
		}
		catch (IOException /* catches ClientProtocolException */ ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("HTTP protocol or I/O error: " + message);
		}
		finally {
			if (response != null) try { response.close(); } catch (Exception ex) {}
			if (client != null) try { client.close(); } catch (Exception ex) {}

			target.close(context);
			source.close(context);
		}
	}

	protected int getSourceColumnCount(EvaluatedParameters parameters) {
		return parameters.getRequestFields().size();
	}

	private int getTargetParameterCount(EvaluatedParameters parameters) {
		return
				parameters.getKeepIndexes().size() +
				parameters.getResponseFields().size() +
				1 +
				(parameters.getMessageField() != null ? 1 : 0);
	}

	protected List<String> getBodyFields(EvaluatedParameters baseEvaluatedParameters) { return null; }

	/**
	 * @throws SQLException
	 * @throws URISyntaxException
	 */
	private HttpRequestBase buildRequest(EvaluatedParameters parameters, Source source) throws SQLException, URISyntaxException  {

		String resolvedUrl = parameters.getUrl();

		if (!parameters.getSubstituteIndexes().isEmpty()) {
			for (int substituteIndex : parameters.getSubstituteIndexes()) {

				String template = "{" + parameters.getRequestFields().get(substituteIndex - 1) + "}";
				String replacement = (String)source.getObject(substituteIndex);
				resolvedUrl = resolvedUrl.replace(template, replacement);
			}
		}

		HttpRequestBase request = makeRequest();

		request.setURI(new URI(resolvedUrl));

		for (EvaluatedParameters.Header header : parameters.getHeaders()) {
			request.addHeader(header.name, header.value);
		}

		addBody(request, parameters, source);

		return request;
	}

	protected abstract HttpRequestBase makeRequest();

	/**
	 * @throws SQLException
	 */
	protected void addBody(HttpRequestBase baseRequest, EvaluatedParameters baseEvaluatedParameters, Source source) throws SQLException {}

	/**
	 * @throws SQLException
	 * @throws IllegalStateException
	 * @throws IOException
	 */
	private void interpretResponse(
			int statusCode,
			Source source,
			EvaluatedParameters parameters,
			DataTarget target,
			CloseableHttpResponse response,
			ResponseType expectedResponseType) throws SQLException, IllegalStateException, IOException {

		JsonParser parser = null;
		try {
			if (!parameters.getResponseFields().isEmpty()) {

				HttpEntity entity = response.getEntity();
				if (entity != null) {

					if (!hasSupportedContentLength (entity)) {
//						throw new RuntimeException("Unknown or excessive response content length");
					}

					if (!hasSupportedContentType(entity)) {
						throw new RuntimeException("Unsupported content type in response");
					}

					if (expectedResponseType == ResponseType.value) {
						String value = getContentAsString(entity);

						interpretNakedValue(statusCode, source, parameters, target, value);
					}
					else {
						parser = Json.createParser(entity.getContent());
						JsonParser.Event event = parser.next();

						switch (event) {
						case START_OBJECT: {
							if (expectedResponseType == ResponseType.map) {
								interpretMapEntity(statusCode, source, parameters, target, parser);
							}
							else {
								if (expectedResponseType != null && expectedResponseType != ResponseType.object) {
									throwWrongEntityType(expectedResponseType, ResponseType.object);
								}
								interpretObjectEntity(statusCode, source, parameters, target, parser);
							}
							break;
						}
						case START_ARRAY: {
							if (expectedResponseType != null && expectedResponseType != ResponseType.list) {
								throwWrongEntityType(expectedResponseType, ResponseType.list);
							}
							interpretListEntity(statusCode, source, parameters, target, parser);
							break;
						}
						default:
							throw new RuntimeException("Response has unknown entity type");
						}
					}
				}
				else {
					throw new RuntimeException("Response did not include an entity as expected");
				}
			}
			else {
				interpretNoEntity(statusCode, source, parameters, target);
			}
		}
		finally {
			if (parser != null) try { parser.close(); } catch (Exception ex) {}
		}
	}

	private void throwWrongEntityType(ResponseType expectedResponseType, ResponseType actualResponseType) {
		throw new RuntimeException(
				"Response entity is " + actualResponseType.name().toUpperCase() +
				" but " + expectedResponseType.name().toUpperCase() + " was specified");
	}

	private void interpretNoEntity(
			int statusCode,
			Source source,
			EvaluatedParameters parameters,
			DataTarget target) throws SQLException {

		int columnIndex = keepSourceColumns(source, parameters, target);

		finishTargetColumns(statusCode, parameters, columnIndex, target);
	}

	private void interpretNakedValue(
			int statusCode,
			Source source,
			EvaluatedParameters parameters,
			DataTarget target,
			String value) throws SQLException {

		int columnIndex = keepSourceColumns(source, parameters, target);

		if (!parameters.getResponseFields().isEmpty()) {

			target.setObject(columnIndex++, value);

			for (int i = 1; i < parameters.getResponseFields().size(); ++i) {
				target.setObject(columnIndex++, null);
			}
		}

		finishTargetColumns(statusCode, parameters, columnIndex, target);
	}

	private void interpretValueEntity(
			int statusCode,
			Source source,
			EvaluatedParameters parameters,
			DataTarget target,
			JsonParser parser,
			JsonParser.Event event) throws SQLException {

		int columnIndex = keepSourceColumns(source, parameters, target);

		if (!parameters.getResponseFields().isEmpty()) {

			int parameterType = getParameterType(target, columnIndex);
			Object entityFieldValue = getValue(parser, event, parameterType);
			target.setObject(columnIndex++, entityFieldValue);

			for (int i = 1; i < parameters.getResponseFields().size(); ++i) {
				target.setObject(columnIndex++, null);
			}
		}

		finishTargetColumns(statusCode, parameters, columnIndex, target);
	}

	private void interpretObjectEntity(
			int statusCode,
			Source source,
			EvaluatedParameters parameters,
			DataTarget target,
			JsonParser parser) throws SQLException {

		int columnIndex = keepSourceColumns(source, parameters, target);

		// Set target columns that match response fields.

		Set<Integer> fieldColumnIndexes = new HashSet<Integer>();

		JsonParser.Event event;
		while ((event = parser.next()).equals(JsonParser.Event.KEY_NAME)) {

			String key = parser.getString();
			int fieldIndex = parameters.getResponseFields().indexOf(key);

			if (-1 < fieldIndex) {

				int fieldColumnIndex = columnIndex + fieldIndex;
				fieldColumnIndexes.add(fieldColumnIndex);

				int parameterType = getParameterType(target, fieldColumnIndex);
				Object entityFieldValue = getValue(parser, parser.next(), parameterType);
				target.setObject(fieldColumnIndex, entityFieldValue);
			}
			else {
				skipValue(parser);
			}
		}

		if (event != JsonParser.Event.END_OBJECT) {
			throw new RuntimeException("Object entity has unexpected structure");
		}

		// Set unmatched target columns NULL.

		for (int i = 0; i < parameters.getResponseFields().size(); ++i) {
			if (!fieldColumnIndexes.contains(columnIndex)) {
				target.setObject(columnIndex, null);
			}
			columnIndex++;
		}

		finishTargetColumns(statusCode, parameters, columnIndex, target);
	}

	private void interpretListEntity(
			int statusCode,
			Source source,
			EvaluatedParameters parameters,
			DataTarget target,
			JsonParser parser) throws SQLException {

		JsonParser.Event event;
		while ((event = parser.next()) != JsonParser.Event.END_ARRAY) {

			switch (event) {
			default:
				interpretValueEntity(statusCode, source, parameters, target, parser, event);
				break;
			case START_OBJECT:
				interpretObjectEntity(statusCode, source, parameters, target, parser);
				break;
			case START_ARRAY:
				throw new RuntimeException("Array entity with array elements is not supported");
			}
		}
	}

	private void interpretMapEntity(
			int statusCode,
			Source source,
			EvaluatedParameters parameters,
			DataTarget target,
			JsonParser parser) {

		// TODO Auto-generated method stub
		throw new RuntimeException("MAP response type is not implemented");
	}

	private boolean hasSupportedContentLength(HttpEntity entity) {

		long contentLength = entity.getContentLength();
		return (0 <= contentLength) && (contentLength <= maxResponseContentLength);
	}

	private boolean hasSupportedContentType(HttpEntity entity) {

		ContentType contentType = ContentType.getOrDefault(entity);
		return
				contentType.getMimeType().equals(supportedContentType.getMimeType()) &&
				(contentType.getCharset() == null || contentType.getCharset().equals(supportedContentType.getCharset()));
	}

	private int getParameterType(DataTarget target, int columnIndex) {

		int parameterType = Types.NULL;
		try {
			// Some databases (MySQL) will not have metadata with parameter types available here.
			parameterType = target.getParameterType(columnIndex);
		}
		catch (SQLException ex) {}

		return parameterType;
	}

	private int keepSourceColumns(
			Source source,
			EvaluatedParameters parameters,
			DataTarget target) throws SQLException {

		int columnIndex = 1;
		if (!parameters.getKeepIndexes().isEmpty()) {
			for (int keepIndex : parameters.getKeepIndexes()) {
				Object sourceColumnValue = source.getObject(keepIndex);
				target.setObject(columnIndex++, sourceColumnValue);
			}
		}

		return columnIndex;
	}

	private int finishTargetColumns(
			int statusCode,
			EvaluatedParameters parameters,
			int columnIndex,
			DataTarget target) throws SQLException {

		target.setObject(columnIndex++, statusCode);

		if (parameters.getMessageField() != null) {
			target.setObject(columnIndex++, null);
		}

		target.addBatch();

		return columnIndex;
	}

	/**
	 * @throws SQLException
	 * @throws IllegalStateException
	 * @throws IOException
	 */
	private void interpretFailedResponse(
			int statusCode,
			Source source,
			EvaluatedParameters parameters,
			DataTarget target,
			CloseableHttpResponse response) throws SQLException, IllegalStateException, IOException {

		JsonReader reader = null;

		try {
			int columnIndex = keepSourceColumns(source, parameters, target);

			// Set all target columns that would come from the response to NULL.

			if (!parameters.getResponseFields().isEmpty()) {
				for (int i = 0; i < parameters.getResponseFields().size(); ++i) {
					target.setObject(columnIndex++, null);
				}
			}

			// If the response is from Jersey exception mapper, retrieve error message.
			// Otherwise, use the whole response body text as the error message,
			// regardless of the content type.

			String message = null;

			HttpEntity entity = response.getEntity();
			if (entity != null) {

				if (hasSupportedContentLength(entity) && hasSupportedContentType(entity)) {

					reader = Json.createReader(entity.getContent());

					JsonObject responseObject = reader.readObject();

					JsonValue fieldValue = responseObject.get("message");
					if (fieldValue != null) {
						message = ((JsonString)fieldValue).getString();
					}

					reader.close();
					reader = null;
				}
				else {
					message = getContentAsString(entity);
				}
			}

			target.setObject(columnIndex++, statusCode);

			if (parameters.getMessageField() != null) {
				target.setObject(columnIndex++, message);
			}

			target.addBatch();
		}
		finally {
			if (reader != null) try { reader.close(); } catch (Exception ex) {}
		}
	}

	private String getContentAsString(HttpEntity entity) throws IllegalStateException, IOException {

		// See http://stackoverflow.com/questions/309424/read-convert-an-inputstream-to-a-string

		InputStream content = entity.getContent();
		String newLine = System.getProperty("line.separator");
		String message = new BufferedReader(new InputStreamReader(content)).lines().collect(Collectors.joining(newLine));

		return message;
	}

	/**
	 * Retrieve the next value from the JSON parser converted if necessary to an object type
	 * acceptable to PreparedStatement.setObject(Object) for a specified java.sql.Types type
	 *
	 * @param parser is the JSON parser from which the value is retrieved
	 * @param event is the event type of the value
	 * @param parameterType is the java.sql.Types type to which the returned object must be acceptable
	 * @return the value object or null if the field value is null or the field is not present in the JSON object
	 */
	private Object getValue(JsonParser parser, JsonParser.Event event, int parameterType) {

		if (parameterType == Types.NULL) {
			// Caller could not determine the exact type needed for setObject(Object).
			// Return the actual value cast to the nearest type match.

			switch (event) {
			case VALUE_NULL:
				return null;
			case VALUE_FALSE:
				return (Boolean)false;
			case VALUE_TRUE:
				return (Boolean)true;
			case VALUE_NUMBER:
				parameterType = Types.DECIMAL;
				break;
			case VALUE_STRING:
				parameterType = Types.VARCHAR;
				break;
			default:
				throw new RuntimeException("Unsupported value type in response");
			}
		}
		else {
			switch (parameterType) {
			case Types.TINYINT:
			case Types.INTEGER:
			case Types.BIGINT:
			case Types.DECIMAL:
			case Types.NUMERIC:
			case Types.DOUBLE:
			case Types.FLOAT:
				if (!event.equals(JsonParser.Event.VALUE_NUMBER)) {
					throw new RuntimeException("Value in response is not a number where expected");
				}
			}
		}

		switch (parameterType) {
		case Types.TINYINT:
		case Types.INTEGER:
			return (Integer)parser.getInt();

		case Types.BIGINT:
			return (Long)parser.getLong();

		case Types.DECIMAL:
		case Types.NUMERIC:
			return parser.getBigDecimal();

		case Types.DOUBLE:
		case Types.FLOAT:
			return parser.getBigDecimal().doubleValue();

		case Types.CHAR:
		case Types.NCHAR:
		case Types.VARCHAR:
		case Types.NVARCHAR:
			return parser.getString();

		case Types.DATE:
			return LocalDate.parse(parser.getString());

		case Types.TIMESTAMP:
			return LocalDateTime.parse(parser.getString());

		case Types.BOOLEAN: {
			switch (event) {
			case VALUE_FALSE:
				return (Boolean)false;
			case VALUE_TRUE:
				return (Boolean)true;
			case VALUE_NUMBER: {
				if (parser.isIntegralNumber()) {
					long longValue = parser.getLong();
					if (longValue == 0) {
						return (Boolean)false;
					}
					else if (longValue == 1) {
						return (Boolean)true;
					}
				}
			}
				// Note intentional fall-through!
			default:
				throw new ClassCastException("Can't cast response value to BOOLEAN");
			}
		}

		default:
			throw new RuntimeException("Unsupported column type in target");
		}
	}

	/**
	 * Skip the value portion of name, value pair.
	 * Value may be a stand-alone value, an object, or an array.
	 * @param parser
	 */
	private void skipValue(JsonParser parser) {
		skipEvent(parser, parser.next());
	}

	private void skipEvent(JsonParser parser, JsonParser.Event event) {

		switch (event) {
		case START_OBJECT:
			skipStructure(parser, JsonParser.Event.END_OBJECT);
			break;
		case START_ARRAY:
			skipStructure(parser, JsonParser.Event.END_ARRAY);
			break;
		default:
			break;
		}
	}

	private void skipStructure(JsonParser parser, JsonParser.Event endEvent) {

		JsonParser.Event event;
		while ((event = parser.next()) != endEvent) {
			skipEvent(parser, event);
		}
	}
}
