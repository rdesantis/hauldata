/*
 * Copyright (c) 2016, 2019, Ronald DeSantis
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

import java.util.List;

import org.apache.commons.vfs2.FileSystemException;

import com.hauldata.dbpa.connection.FtpConnection;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;

public abstract class FtpTask extends Task {

	private boolean isBinary;
	private FtpConnection connection;
	private List<Expression<String>> fromNames;
	private Expression<String> toName;

	public FtpTask(
			Prologue prologue,
			boolean isBinary,
			FtpConnection connection,
			List<Expression<String>> fromNames,
			Expression<String> toName) {
		
		super(prologue);
		this.isBinary = isBinary;
		this.connection = connection;
		this.fromNames = fromNames;
		this.toName = toName;
	}

	@Override
	protected void execute(Context context) {

		String evaluatedToName = (toName != null) ? toName.evaluate() : null;

		FtpConnection.Manager manager = null;
		try {
			manager = context.getManager(connection, isBinary);

			Copier copier = getCopier(context, manager, evaluatedToName);

			if (!copier.isToDirectory() && (fromNames.size() != 1)) {
				throw new IllegalArgumentException("Cannot transfer multiple source files to the same target file; target must be an existing directory");
			}

			for (Expression<String> fromName : fromNames) {
				
				String evaluatedFromName = fromName.evaluate();

				copier.copy(context, manager, evaluatedFromName);
			}
		}
		catch (IllegalArgumentException ex) {
			throw ex;
		}
		catch (Exception ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error attempting to transfer file: " + message, ex);
		}
		finally {
			if (manager != null) manager.close();
		}
	}

	protected interface Copier {

		boolean isToDirectory();

		void copy(
				Context context,
				FtpConnection.Manager manager,
				String fromFileName) throws FileSystemException;
	}

	protected abstract Copier getCopier(Context context, FtpConnection.Manager manager, String toName) throws FileSystemException;
}
