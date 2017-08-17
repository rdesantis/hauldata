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

package com.hauldata.dbpa.file;

import java.io.IOException;

public class TargetSheetPage extends TargetPage {

	protected static class Factory implements TargetPage.Factory {

		private Book.Factory bookFactory;
		private Sheet.Factory sheetFactory;

		Factory(Book.Factory bookFactory, Sheet.Factory sheetFactory) {
			this.bookFactory = bookFactory;
			this.sheetFactory = sheetFactory;
		}

		@Override
		public TargetPage create(File.Owner fileOwner, PageIdentifier id, File.Options options, TargetHeaders headers) throws IOException {

			Book book = appendBook(fileOwner, id);
			Sheet sheet = (Sheet)Sheet.getForCreate(book, ((SheetIdentifier)id).getSheetName(), sheetFactory, options);
			sheet.setHeaders(headers);
			sheet.create();
			sheet.setOpen(true);

			return new TargetSheetPage(book, sheet);
		}

		@Override
		public TargetPage append(File.Owner fileOwner, PageIdentifier id) throws IOException {

			Book book = appendBook(fileOwner, id);
			Sheet sheet = (Sheet)Sheet.getForAppend(book, ((SheetIdentifier)id).getSheetName(), sheetFactory);
			if (!sheet.isOpen()) {
				sheet.setHeaders(new TargetHeaders());
				sheet.append();
				sheet.setOpen(true);
			}
			return new TargetSheetPage(book, sheet);
		}

		@Override
		public TargetPage write(File.Owner fileOwner, PageIdentifier id, File.Options options, TargetHeaders headers) throws IOException {

			Book book = appendBook(fileOwner, id);
			Sheet sheet = (Sheet)Sheet.getForWrite(book, ((SheetIdentifier)id).getSheetName(), sheetFactory, options);
			if (!sheet.isOpen()) {
				sheet.setHeaders(headers);
				sheet.create();
				sheet.setOpen(true);
			}
			return new TargetSheetPage(book, sheet);
		}

		private Book appendBook(File.Owner fileOwner, PageIdentifier id) throws IOException {

			Book book = (Book)Book.getForAppend(fileOwner, id.getPath(), bookFactory);
			if (!book.isOpen()) {
				book.create();
				book.setOpen(true);
			}
			else {
				book.append();
			}
			return book;
		}
	}

	protected Book book;

	protected TargetSheetPage(Book book, Sheet sheet) {
		super(sheet);
		this.book = book;
	}
}
