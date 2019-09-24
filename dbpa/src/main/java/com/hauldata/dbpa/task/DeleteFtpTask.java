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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.vfs2.FileSystemException;

import com.hauldata.dbpa.connection.FtpConnection;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;

public class DeleteFtpTask extends Task {

	private FtpConnection connection;
	private List<Expression<String>> files;

	public DeleteFtpTask(
			Prologue prologue,
			FtpConnection connection,
			List<Expression<String>> files) {

		super(prologue);
		this.connection = connection;
		this.files = files;
	}

	@Override
	protected void execute(Context context) throws Exception {

		List<String> patterns = files.stream().map(f -> f.evaluate()).collect(Collectors.toList());

		FtpConnection.Manager manager = null;
		try {
			manager = context.getManager(connection, false);

			for (String pattern : patterns) {
				String[] parentAndFileName = com.hauldata.dbpa.process.Files.getParentAndFileName(pattern);

				try {
					List<String> foundNames = context.getManager(connection, false).findRemote(parentAndFileName[0], parentAndFileName[1]);
					for (String foundName : foundNames) {
						manager.deleteRemote(parentAndFileName[0] + '/' + foundName);
					}
				}
				catch (FileSystemException ex) {
					throw new RuntimeException(ex.toString());
				}
			}
		}
		catch (Exception ex) {
			String message = (ex.getMessage() != null) ? ex.getMessage() : ex.getClass().getName();
			throw new RuntimeException("Error attempting to delete remote file: " + message, ex);
		}
		finally {
			if (manager != null) manager.close();
		}
	}
}
