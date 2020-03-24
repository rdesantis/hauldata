/*
 * Copyright (c) 2020, Ronald DeSantis
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

package com.hauldata.dbpa.connection;

import java.util.Properties;

import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.process.TaskSetParser;

public abstract class ConnectionReference {

	public static ConnectionReference create(Connection connection) {
		return new NamedConnectionReference(connection);
	}

	public static ConnectionReference createDefault(String typeName) {
		if (typeName.equals(TaskSetParser.KW.DATABASE.name())) {
			return new ConnectionReference() {
				public Connection getConnection(Context context) {
					return context.resolveConnection((DatabaseConnection)null);
				}
			};
		}
		else if (typeName.equals(TaskSetParser.KW.FTP.name())) {
			return new ConnectionReference() {
				public Connection getConnection(Context context) {
					return context.resolveConnection((FtpConnection)null);
				}
			};
		}
		else if (typeName.equals(TaskSetParser.KW.EMAIL.name())) {
			return new ConnectionReference() {
				public Connection getConnection(Context context) {
					return context.resolveConnection((EmailConnection)null);
				}
			};
		}
		else {
			return null;
		}
	}

	public void setProperties(Context context, Properties properties) {
		getConnection(context).setProperties(properties);
	}

	public Properties getProperties(Context context) {
		return getConnection(context).getProperties();
	}

	public abstract Connection getConnection(Context context);
}

class NamedConnectionReference extends ConnectionReference {

	private Connection connection;

	NamedConnectionReference(Connection connection) {
		this.connection = connection;
	}

	@Override
	public Connection getConnection(Context context) {
		return connection;
	}
}
