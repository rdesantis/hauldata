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

package com.hauldata.dbpa.task;

import java.util.Properties;

import com.hauldata.dbpa.connection.Connection;
import com.hauldata.dbpa.connection.DatabaseConnection;
import com.hauldata.dbpa.connection.EmailConnection;
import com.hauldata.dbpa.connection.FtpConnection;
import com.hauldata.dbpa.process.Context;

public abstract class ConnectTask extends Task {

	protected Connection connection;

	protected ConnectTask(
			Prologue prologue,
			Connection connection) {
		super(prologue);
		this.connection = connection;
	}

	@Override
	protected void execute(Context context) {
		connection.setProperties(getConnectionProperties(context));
	}

	protected abstract Properties getConnectionProperties(Context context);

	protected Properties defaultProperties(Context context, Connection connection) {
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
}
