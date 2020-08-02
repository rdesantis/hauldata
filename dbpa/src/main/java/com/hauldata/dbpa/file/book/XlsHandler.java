package com.hauldata.dbpa.file.book;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Workbook;
import com.hauldata.dbpa.file.book.XlsxBook.WorkbookFactory;
import com.hauldata.dbpa.file.book.XlsxBook.WorkbookWrapper;

public class XlsHandler extends XlHandler {

	private static class XlsBookWrapper implements WorkbookWrapper {
		private HSSFWorkbook book = null;

		public XlsBookWrapper(String filename) throws FileNotFoundException, IOException {
			book = new HSSFWorkbook(new FileInputStream(filename));
		}

		public XlsBookWrapper() {
			book = new HSSFWorkbook();
		}

		@Override
		public Workbook getBook() {
			return book;
		}

		@Override
		public void close() throws IOException {
			if (book != null) {
				book.close();
			}
		}
	}

	public static void register(String name) {
		register(name, "XLS File", "XLS Sheet", new WorkbookFactory() {

			@Override
			public WorkbookWrapper newSourceBook(String filename) throws InvalidFormatException, IOException {
				return new XlsBookWrapper(filename);
			}

			@Override
			public WorkbookWrapper newTargetBook() {
				return new XlsBookWrapper();
			}
		});
	}
}
