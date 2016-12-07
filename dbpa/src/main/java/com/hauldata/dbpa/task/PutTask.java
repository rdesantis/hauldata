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

import org.apache.commons.vfs2.FileSystemException;

import com.hauldata.dbpa.connection.FtpConnection;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;

public class PutTask extends FtpTask {

	public PutTask(
			Prologue prologue,
			boolean isBinary,
			FtpConnection connection,
			List<Expression<String>> fromNames,
			Expression<String> toName) {
		
		super(prologue, isBinary, connection, fromNames, toName);
	}

	@Override
	protected void copy(
			Context context,
			FtpConnection.Manager manager,
			String fromFileName,
			String toDirectoryName) throws FileSystemException
	{
		Path localFilePath = context.getWritePath(fromFileName);
		context.files.assureNotOpen(localFilePath);

		String localFileName = localFilePath.toString();

		String fileName = localFilePath.getFileName().toString();
		String remoteFileName = (toDirectoryName != null) ? toDirectoryName + "/" + fileName : fileName;

		manager.copyLocalToRemote(localFileName, remoteFileName);
	}
}
