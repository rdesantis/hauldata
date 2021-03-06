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

import java.nio.file.Path;
import java.util.List;

import org.apache.commons.vfs2.FileSystemException;

import com.hauldata.dbpa.connection.FtpConnection;
import com.hauldata.dbpa.expression.Expression;
import com.hauldata.dbpa.process.Context;
import com.hauldata.dbpa.process.Files;

public class GetTask extends FtpTask {

	public GetTask(
			Prologue prologue,
			boolean isBinary,
			FtpConnection connection,
			List<Expression<String>> fromNames,
			Expression<String> toName) {
		
		super(prologue, isBinary, connection, fromNames, toName);
	}

	@Override
	protected Copier getCopier(Context context, FtpConnection.Manager manager, String toName) {
		return new GetCopier(context, toName);
	}

	class GetCopier implements Copier {

		private String toName;
		private boolean toDirectory;

		GetCopier(Context context, String toName) {
			this.toName = toName;

			toDirectory = (toName == null);
			if (!toDirectory) {
				Path toPath = context.getReadPath(toName);
				toDirectory = java.nio.file.Files.isDirectory(toPath);
			}
		}

		@Override
		public boolean isToDirectory() { return toDirectory; }

		@Override
		public void copy(
				Context context,
				FtpConnection.Manager manager,
				String fromFileName) throws FileSystemException {

			String fileName = Files.getFileName(fromFileName);

			String toFileName = (toName == null) ? fileName : toDirectory ? toName + "/" + fileName : toName;

			Path localFilePath = context.getReadPath(toFileName);
			context.files.assureNotOpen(localFilePath);

			String localFileName = localFilePath.toString();

			manager.copyLocalFromRemote(localFileName, fromFileName);
		}
	}
}
