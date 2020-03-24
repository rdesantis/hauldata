/*
 * Copyright (c) 2019, 2020, Ronald DeSantis
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

import com.hauldata.dbpa.connection.ConnectionReference;
import com.hauldata.dbpa.connection.DatabaseConnection;
import com.hauldata.dbpa.connection.EmailConnection;
import com.hauldata.dbpa.connection.FtpConnection;
import com.hauldata.dbpa.process.Context;

public abstract class ConnectTask extends Task {

	protected ConnectionReference reference;

	protected ConnectTask(
			Prologue prologue,
			ConnectionReference reference) {
		super(prologue);
		this.reference = reference;
	}

	@Override
	protected void execute(Context context) {
		reference.setProperties(context, getNewProperties(context));
	}

	protected abstract Properties getNewProperties(Context context);

	protected Properties getDefaultProperties(Context context) {
		if (reference.getConnection(context) instanceof DatabaseConnection) {
			return context.connectionProps;
		}
		if (reference.getConnection(context) instanceof EmailConnection) {
			return context.sessionProps;
		}
		if (reference.getConnection(context) instanceof FtpConnection) {
			return context.ftpProps;
		}
		else {
			throw new RuntimeException("Unsupported connection type " + reference.getConnection(context).getClass().getName());
		}
	}
}
