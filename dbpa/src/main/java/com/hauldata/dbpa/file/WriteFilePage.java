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

package com.hauldata.dbpa.file;

import java.io.IOException;

public class WriteFilePage extends WritePage {

	protected static class Factory implements WritePage.Factory {

		private File.Factory fileFactory;

		Factory(File.Factory fileFactory) {
			this.fileFactory = fileFactory;
		}

		@Override
		public WritePage create(File.Owner fileOwner, PageIdentifier id, WriteHeaders headers) throws IOException {
			FlatFile file = (FlatFile)File.getForCreate(fileOwner, id.getPath(), fileFactory);
			file.setHeaders(headers);
			file.create();
			file.setOpen(true);
			return new WriteFilePage(file);
		}

		@Override
		public WritePage append(File.Owner fileOwner, PageIdentifier id) throws IOException {
			FlatFile file = (FlatFile)File.getForAppend(fileOwner, id.getPath(), fileFactory);
			if (!file.isOpen()) {
				file.setHeaders(new WriteHeaders());
				file.append();
				file.setOpen(true);
			}
			return new WriteFilePage(file);
		}

		@Override
		public WritePage write(File.Owner fileOwner, PageIdentifier id, WriteHeaders headers) throws IOException {
			FlatFile file = (FlatFile)File.getForWrite(fileOwner, id.getPath(), fileFactory);
			if (!file.isOpen()) {
				file.setHeaders(headers);
				file.create();
				file.setOpen(true);
			}
			return new WriteFilePage(file);
		}
	}

	protected WriteFilePage(FlatFile file) {
		super(file);
	}
}
