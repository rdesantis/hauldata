package com.hauldata.dbpa.task;

import java.io.IOException;
import java.io.StringReader;
import java.util.NoSuchElementException;
import java.util.Properties;

import com.hauldata.dbpa.connection.Connection;
import com.hauldata.dbpa.connection.DatabaseConnection;
import com.hauldata.dbpa.connection.EmailConnection;
import com.hauldata.dbpa.connection.FtpConnection;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;
import com.hauldata.util.tokenizer.KeywordValueTokenizer;
import com.hauldata.util.tokenizer.Quoted;
import com.hauldata.util.tokenizer.Token;

public class ConnectTask extends Task {

	private Connection connection;
	private boolean inherit;
	private Expression<String> properties;

	public ConnectTask(
			Prologue prologue,
			Connection connection,
			boolean inherit,
			Expression<String> properties) {

		super(prologue);
		this.connection = connection;
		this.inherit = inherit;
		this.properties = properties;
	}

	@Override
	protected void execute(Context context) {

		Properties properties = null;
		if (this.properties != null) {
			String keywordValues = this.properties.evaluate();
			properties = parse(keywordValues, inherit, defaultProperties(context, connection));
		}
		else {
			properties = context.connectionProps;
		}

		connection.setProperties(properties);
	}

	private Properties defaultProperties(Context context, Connection connection) {
		if (connection instanceof DatabaseConnection) {
			return context.connectionProps;
		}
		if (connection instanceof EmailConnection) {
			return context.sessionProps;
		}
		if (connection instanceof FtpConnection) {
			return context.ftpProps;
		}
		else {
			throw new RuntimeException("Unsupported connection type " + connection.getClass().getName());
		}
	}

	private Properties parse(
			String keywordValues,
			boolean inherit,
			Properties defaults) {

		Properties properties = inherit ? new Properties(defaults) : new Properties();

		KeywordValueTokenizer tokenizer = new KeywordValueTokenizer(new StringReader(keywordValues));
		try {
			while (tokenizer.hasNext()) {
				String keyword = tokenizer.nextWord();
	
				Token valueToken = tokenizer.nextToken();
				if (valueToken == null) {
					throw new RuntimeException("Missing value in connection properties");
				}
				String value = valueToken instanceof Quoted ? ((Quoted)valueToken).getBody() : valueToken.getImage();
	
				properties.setProperty(keyword, value);
			}
		}
		catch (NoSuchElementException ex) { // also catches InputMismatchException
			throw new RuntimeException("Property name not found where expected in connection properties");
		}
		catch (IOException ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error attempting to read connection properties: " + message, ex);
		}
		finally {
			try { tokenizer.close(); } catch (Exception ex) {}
		}

		return properties;
	}
}
