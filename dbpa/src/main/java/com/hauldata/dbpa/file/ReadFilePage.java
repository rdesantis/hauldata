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

public class ReadFilePage extends ReadPage {

	protected static class Factory implements ReadPage.Factory {

		private File.Factory fileFactory;

		Factory(File.Factory fileFactory) {
			this.fileFactory = fileFactory;
		}

		@Override
		public ReadPage open(File.Owner fileOwner, PageIdentifier id, ReadHeaders headers) throws IOException {
			FlatFile file = (FlatFile)File.getForOpen(fileOwner, id.getPath(), fileFactory);
			file.setHeaders(headers);
			file.open();
			file.setOpen(true);
			return new ReadFilePage(file);
		}

		@Override
		public ReadPage load(File.Owner fileOwner, PageIdentifier id) throws IOException {
			FlatFile file = (FlatFile)File.getForLoad(fileOwner, id.getPath(), fileFactory);
			file.load();
			return new ReadFilePage(file);
		}

		@Override
		public ReadPage read(File.Owner fileOwner, PageIdentifier id, ReadHeaders headers) throws IOException {
			FlatFile file = (FlatFile)File.getForRead(fileOwner, id.getPath(), fileFactory);
			if (!file.isOpen()) {
				file.setHeaders(headers);
				file.open();
				file.setOpen(true);
			}
			else {
				file.load();
			}
			return new ReadFilePage(file);
		}
	}

	protected FlatFile file;

	protected ReadFilePage(FlatFile file) {
		super(file);
	}
}
