/*
 * Copyright (c) 2018, Ronald DeSantis
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

import com.hauldata.dbpa.connection.FtpConnection;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;

public class MoveTask extends Task {

	private boolean isBinary;
	private FtpConnection connection;
	private Expression<String> fromName;
	private Expression<String> toName;

	public MoveTask(
			Prologue prologue,
			boolean isBinary,
			FtpConnection connection,
			Expression<String> fromName,
			Expression<String> toName) {

		super(prologue);
		this.isBinary = isBinary;
		this.connection = connection;
		this.fromName = fromName;
		this.toName = toName;
	}

	@Override
	protected void execute(Context context) throws Exception {

		String fromName = this.fromName.evaluate();
		String toName = this.toName.evaluate();

		if (fromName == null || toName == null) {
			throw new RuntimeException("Source or destination file path expression evaluates to NULL");
		}

		FtpConnection.Manager manager = null;
		try {
			manager = context.getManager(connection, isBinary);

			manager.moveRemoteToRemote(fromName, toName);
		}
		catch (Exception ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error attempting to move remote file: " + message, ex);
		}
		finally {
			if (manager != null) manager.close();
		}
	}
}
