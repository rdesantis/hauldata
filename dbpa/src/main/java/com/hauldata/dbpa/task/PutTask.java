/*
 * Copyright (c) 2016, Ronald DeSantis
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

import java.nio.file.Path;
import java.util.List;

import com.hauldata.dbpa.connection.FtpConnection;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;

public class PutTask extends Task {

	private boolean isBinary;
	private List<Expression<String>> localNames;
	private Expression<String> remoteName;

	public PutTask(
			Prologue prologue,
			boolean isBinary,
			List<Expression<String>> localNames,
			Expression<String> remoteName) {
		
		super(prologue);
		this.isBinary = isBinary;
		this.localNames = localNames;
		this.remoteName = remoteName;
	}

	@Override
	protected void execute(Context context) {

		String remotePathName = remoteName.evaluate();
		
		FtpConnection.Manager manager = null;
		try {
			manager = context.getManager(isBinary);

			for (Expression<String> localName : localNames) {

				Path localFilePath = context.getWritePath(localName.evaluate());
				context.files.assureNotOpen(localFilePath);

				String localFileName = localFilePath.toString();

				String remoteFileName = remotePathName + "/" + localFilePath.getFileName().toString();

				manager.copy(localFileName, remoteFileName);
			}
		}
		catch (Exception ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error attempting to transfer file: " + message, ex);
		}
		finally {
			if (manager != null) manager.close();
		}
	}
}
