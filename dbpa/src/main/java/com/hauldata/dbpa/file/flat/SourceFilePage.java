/*
 * Copyright (c) 2016, 2017, Ronald DeSantis
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

package com.hauldata.dbpa.file.flat;

import java.io.IOException;

import com.hauldata.dbpa.file.File;
import com.hauldata.dbpa.file.PageIdentifier;
import com.hauldata.dbpa.file.PhysicalPageIdentifier;
import com.hauldata.dbpa.file.SourceHeaders;
import com.hauldata.dbpa.file.SourcePage;

public class SourceFilePage extends SourcePage {

	public static class Factory implements SourcePage.Factory {

		private File.Factory fileFactory;

		public Factory(File.Factory fileFactory) {
			this.fileFactory = fileFactory;
		}

		@Override
		public SourcePage open(File.Owner fileOwner, PageIdentifier id, SourceHeaders headers) throws IOException {
			FlatFile file = (FlatFile)File.getForOpen(fileOwner, ((PhysicalPageIdentifier)id).getPath(), fileFactory);
			file.setHeaders(headers);
			file.open();
			file.setOpen(true);
			return new SourceFilePage(file);
		}

		@Override
		public SourcePage load(File.Owner fileOwner, PageIdentifier id) throws IOException {
			FlatFile file = (FlatFile)File.getForLoad(fileOwner, ((PhysicalPageIdentifier)id).getPath(), fileFactory);
			file.load();
			return new SourceFilePage(file);
		}

		@Override
		public SourcePage read(File.Owner fileOwner, PageIdentifier id, SourceHeaders headers) throws IOException {
			FlatFile file = (FlatFile)File.getForRead(fileOwner, ((PhysicalPageIdentifier)id).getPath(), fileFactory);
			if (!file.isOpen()) {
				file.setHeaders(headers);
				file.open();
				file.setOpen(true);
			}
			else {
				file.load();
			}
			return new SourceFilePage(file);
		}
	}

	protected FlatFile file;

	protected SourceFilePage(FlatFile file) {
		super(file);
	}
}
