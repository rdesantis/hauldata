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

package com.hauldata.dbpa.file.book;

import java.awt.HeadlessException;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.FontUnderline;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFFont;

import com.hauldata.dbpa.file.PageOptions;
import com.hauldata.dbpa.file.TargetHeaders;
import com.hauldata.dbpa.file.book.XlsxTargetBook.XlsxCellStyle;
import com.hauldata.dbpa.file.html.HtmlOptions;

public class XlsxTargetSheet extends XlsxSheet {

	private SXSSFSheet sheet;
	private int rowIndex;
	private ArrayList<Object> rowValues;
	private int columnCount;

	private ResolvedSheetStyles sheetStyles;

	public XlsxTargetSheet(Book owner, String name, PageOptions options) {
		super(owner, name, options);

		sheet = null;
		rowIndex = 0;
		rowValues = new ArrayList<Object>();
		columnCount = 0;
	}

	public static class TargetOptions extends HtmlOptions {

		public static final TargetOptions DEFAULT = new TargetOptions();

		private boolean styled = false;

		public boolean isStyled() {
			return styled;
		}

		public static class Parser extends HtmlOptions.Parser {

			static Map<String, Modifier> modifiers;

			static {
				modifiers = new HashMap<String, Modifier>();
				modifiers.put("STYLED", (parser, options) -> {((TargetOptions)options).styled = true;});
			}

			protected Parser() {
				super(modifiers);
			}

			@Override
			protected PageOptions makeDefaultOptions() {
				return new TargetOptions();
			}
		}
	}

	protected TargetOptions getTargetOptions() {
		return getOptions() != null ? (TargetOptions)getOptions() : TargetOptions.DEFAULT;
	}

	// Node overrides

	@Override
	public void create() throws IOException {

		sheetStyles = new ResolvedSheetStyles(getTargetOptions());

		sheet = getOwner().getBook().createSheet(getName());
		sheet.trackAllColumnsForAutoSizing();

		// The following is duplicated in DsvFile.create() and should probably be moved to common code
		// but TxtFile has a different implementation.

		TargetHeaders headers = getTargetHeaders();
		if (headers.exist() && !headers.fromMetadata()) {
			for (int columnIndex = 1; columnIndex <= headers.getColumnCount(); ++columnIndex) {
				writeColumn(columnIndex, headers.getCaption(columnIndex - 1));
			}
		}
	}

	@Override
	public void append() throws IOException {
		if (!isOpen() || !isWritable()) {
			throw new RuntimeException("Appending a sheet in an existing XLSX book is not supported: " + getName());
		}
	}

	@Override
	public void close() throws IOException {}

	// PageNode overrides

	@Override
	public void writeColumn(int columnIndex, Object object) throws IOException {

		if (columnIndex == 1) {
			if (0 < rowIndex) {
				writeRow(rowValues, rowIndex, getRowPosition(rowIndex));
			}
			rowIndex++;
		}

		if (rowIndex == 1) {
			rowValues.add(object);
		}
		else {
			rowValues.set(columnIndex - 1, object);
		}

		// Can't depend on the number of columns in the last row for true column count,
		// because trailing null columns are not written.  Must detect attempted writes.

		if (columnCount < columnIndex) {
			columnCount = columnIndex;
		}
	}

	private RowPosition getRowPosition(int rowIndex) {
		return
				rowIndex == 1 ? (headers.exist() ? RowPosition.HEADER : RowPosition.TOP) :
				rowIndex == 2 ? (headers.exist() ? RowPosition.NEXT : RowPosition.MIDDLE) :
				RowPosition.MIDDLE;
	}

	private void writeRow(ArrayList<Object> rowValues, int rowIndex, RowPosition rowPosition) {

		Styles rowStyles = null;
		if (getTargetOptions().isStyled()) {

			ValueStyles valueStyles = ValueStyles.parse(TableTag.TR, rowValues.get(0));
			rowValues.set(0, valueStyles.value);
			rowStyles = valueStyles.styles;
		}

		Row row = sheet.createRow(rowIndex - 1);

		int columnIndex = 0;
		for (Object object : rowValues) {

			Styles cellStyles = null;
			if (getTargetOptions().isStyled()) {

				TableTag tag = (rowPosition == RowPosition.HEADER) ? TableTag.TH : TableTag.TD;
				ValueStyles valueStyles = ValueStyles.parse(tag, object);
				object = valueStyles.value;
				cellStyles = valueStyles.styles;
			}

			Cell cell = row.createCell(columnIndex++);

			setCellValue(cell, object);

			if (!sheetStyles.areDefault() || (rowStyles != null) || (cellStyles != null)) {
				setCellStyle(cell, sheetStyles, rowStyles, cellStyles, rowPosition, getColumnPosition(columnIndex));
			}
		}
	}

	private ColumnPosition getColumnPosition(int columnIndex) {
		return
				columnIndex == 1 ? (columnCount == 1 ? ColumnPosition.SINGLE : ColumnPosition.LEFT) :
				columnIndex == columnCount ? ColumnPosition.RIGHT :
				ColumnPosition.MIDDLE;
	}

	private void setCellValue(Cell cell, Object object) {

		if (object == null) {
			// Leave cell empty
		}
		else if (object instanceof Short || object instanceof Integer) {
			cell.setCellValue(((Number)object).doubleValue());
			cell.setCellStyle(getOwner().getCellStyle(XlsxCellStyle.integer));
		}
		else if (object instanceof Long || object instanceof BigInteger) {
			cell.setCellValue(object.toString());
		}
		else if (object instanceof BigDecimal && ((BigDecimal)object).scale() == 2) {
			cell.setCellValue(((Number)object).doubleValue());
			cell.setCellStyle(getOwner().getCellStyle(XlsxCellStyle.money));
		}
		else if (object instanceof Number) {
			cell.setCellValue(((Number)object).doubleValue());
		}
		else if (object instanceof String) {
			cell.setCellValue((String)object);
		}
		else if (object instanceof Boolean) {
			cell.setCellValue((Boolean)object);
		}
		else if (object instanceof Date || object instanceof LocalDateTime) {

			Date date;
			LocalTime time;

			if (object instanceof Date) {
				date = (Date)object;
				Instant instant = Instant.ofEpochMilli(date.getTime());
				time = LocalDateTime.ofInstant(instant, ZoneId.systemDefault()).toLocalTime();
			}
			else {
				LocalDateTime dateTime = (LocalDateTime)object;
				date = Date.from(dateTime.atZone(ZoneId.systemDefault()).toInstant());
				time = dateTime.toLocalTime();
			}

			cell.setCellValue(date);

			CellStyle style = null;
			if (time.equals(LocalTime.MIDNIGHT)) {
				style = getOwner().getCellStyle(XlsxCellStyle.date);
			}
			else {
				style = getOwner().getCellStyle(XlsxCellStyle.datetime);
			}
			cell.setCellStyle(style);
		}
		else {
			cell.setCellValue(object.toString());
		}
	}

	private void setCellStyle(
			Cell cell,
			ResolvedSheetStyles sheetStyles,
			Styles rowStyles,
			Styles cellStyles,
			RowPosition rowPosition,
			ColumnPosition columnPosition) {

		CellStyle originalStyle = cell.getCellStyle();

		Styles styles = sheetStyles.resolve(rowStyles, cellStyles, rowPosition, columnPosition);

		short formatIndex = originalStyle.getIndex();

		StylesWithFormatting stylesWithFormatting = new StylesWithFormatting(styles, formatIndex);

		CellStyle finalStyle = stylesWithFormatting.getCellStyle(getOwner().getBook(), getOwner().stylesUsed, getOwner().fontsUsed, getOwner().colorsUsed);

		if (finalStyle != originalStyle) {
			cell.setCellStyle(finalStyle);
		}
	}

	private XlsxTargetBook getOwner() {
		return (XlsxTargetBook)owner;
	}

	@Override
	public void flush() throws IOException {

		if (0 < rowIndex) {
			writeRow(rowValues, rowIndex, RowPosition.BOTTOM);
		}

		try {
			for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
				sheet.autoSizeColumn(columnIndex);
			}
		}
		catch (HeadlessException ex) {
			// Per https://poi.apache.org/spreadsheet/quick-guide.html:
			//
			// Warning:
			// To calculate column width Sheet.autoSizeColumn uses Java2D classes that throw exception if graphical environment is not available.
			// In case if graphical environment is not available, you must tell Java that you are running in headless mode and set the following system property:
			// java.awt.headless=true . You should also ensure that the fonts you use in your workbook are available to Java.
			//
			// Per http://www.oracle.com/technetwork/articles/javase/headless-136834.html#headlessexception:
			//
			// You can also use the following command line if you plan to run the same application in both a headless and a traditional environment:
			// java -Djava.awt.headless=true
		}

		if (headers.exist()) {
			sheet.createFreezePane(0, 1);
		}
	}

	// Never called.
	@Override public void open() {}
	@Override public void load() {}
	@Override public Object readColumn(int columnIndex) { return null; }
	@Override public boolean hasRow() { return false; }
}

class StylesWithFormatting {

	public Styles styles;
	public short formatIndex;

	StylesWithFormatting(Styles styles, short formatIndex) {
		this.styles = styles;
		this.formatIndex = formatIndex;
	}

	@Override
	public int hashCode() {
		return
				styles.hashCode() ^ formatIndex << 12;
	}

	@Override
	public boolean equals(Object obj) {

		if (!(obj instanceof StylesWithFormatting)) { return false; }

		StylesWithFormatting other = (StylesWithFormatting)obj;
		return
				styles.equals(other.styles) &&
				(formatIndex == other.formatIndex);
	}

	/**
	 * Translate styling to workbook CellStyle.
	 *
	 * @param stylesUsed tracks the styles that have been used in the workbook; it will be updated
	 * @param fontsUsed tracks the fonts that have been used in the workbook; it may be updated
	 * @param colorsUsed tracks the colors that have been used in the workbook; it may be updated
	 */
	public CellStyle getCellStyle(
			SXSSFWorkbook book,
			Map<StylesWithFormatting, XSSFCellStyle> stylesUsed,
			Map<FontStyles, XSSFFont> fontsUsed,
			Map<Integer, XSSFColor> colorsUsed) {

		XSSFCellStyle cellStyle = stylesUsed.get(this);
		if (cellStyle != null) {
			return cellStyle;
		}

		cellStyle = (XSSFCellStyle)book.createCellStyle();
		cellStyle.cloneStyleFrom(book.getCellStyleAt(formatIndex));

		if (styles.bottomBorderStyle != null) {
			cellStyle.setBorderBottom(resolveBorderStyle(styles.bottomBorderStyle, styles.bottomBorderWidth));
		}
		if (styles.leftBorderStyle != null) {
			cellStyle.setBorderLeft(resolveBorderStyle(styles.leftBorderStyle, styles.leftBorderWidth));
		}
		if (styles.rightBorderStyle != null) {
			cellStyle.setBorderRight(resolveBorderStyle(styles.rightBorderStyle, styles.rightBorderWidth));
		}
		if (styles.topBorderStyle != null) {
			cellStyle.setBorderTop(resolveBorderStyle(styles.topBorderStyle, styles.topBorderWidth));
		}

		if (styles.bottomBorderColor != null) {
			cellStyle.setBottomBorderColor(getColor(styles.bottomBorderColor, book, colorsUsed));
		}
		if (styles.leftBorderColor != null) {
			cellStyle.setLeftBorderColor(getColor(styles.leftBorderColor, book, colorsUsed));
		}
		if (styles.rightBorderColor != null) {
			cellStyle.setRightBorderColor(getColor(styles.rightBorderColor, book, colorsUsed));
		}
		if (styles.topBorderColor != null) {
			cellStyle.setTopBorderColor(getColor(styles.topBorderColor, book, colorsUsed));
		}

		if (styles.backgroundColor != null) {
			cellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
			cellStyle.setFillForegroundColor(getColor(styles.backgroundColor, book, colorsUsed));
		}

		if (!styles.font.areDefault()) {
			cellStyle.setFont(getFont(styles.font, book, fontsUsed, colorsUsed));
		}

		stylesUsed.put(this, cellStyle);

		return cellStyle;
	}

	private static BorderStyle resolveBorderStyle(BorderStyle borderStyle, BorderStyle borderWidth) {

		switch (borderStyle) {
		case MEDIUM:
			return (borderWidth != null) ? borderWidth : borderStyle;
		case MEDIUM_DASHED:
			return (borderWidth == BorderStyle.THIN) ? BorderStyle.DASHED : BorderStyle.MEDIUM_DASHED;
		default:
			return borderStyle;
		}
	}

	private static XSSFColor getColor(Integer rgb, SXSSFWorkbook book, Map<Integer, XSSFColor> colorsUsed) {

		XSSFColor color = colorsUsed.get(rgb);
		if (color != null) {
			return color;
		}

		color = new XSSFColor(new java.awt.Color(rgb));

		colorsUsed.put(rgb, color);

		return color;
	}

	private static Font getFont(FontStyles fontStyles, SXSSFWorkbook book, Map<FontStyles, XSSFFont> fontsUsed, Map<Integer, XSSFColor> colorsUsed) {

		XSSFFont font = fontsUsed.get(fontStyles);
		if (font != null) {
			return font;
		}

		font = (XSSFFont)book.createFont();

		if (fontStyles.color != null) {
			font.setColor(getColor(fontStyles.color, book, colorsUsed));
		}

		if (fontStyles.fontStyle != null) {
			switch (fontStyles.fontStyle) {
			case NORMAL:
				break;
			case ITALIC:
				font.setItalic(true);
				break;
			}
		}

		if (fontStyles.fontWeight != null) {
			switch (fontStyles.fontWeight) {
			case NORMAL:
				break;
			case BOLD:
				font.setBold(true);
				break;
			}
		}

		if (fontStyles.textDecorationLine != null) {
			switch (fontStyles.textDecorationLine) {
			case NONE:
				break;
			case LINE_THROUGH:
				font.setStrikeout(true);
				break;
			case UNDERLINE:
				font.setUnderline((fontStyles.textDecorationStyle == FontStyles.TextDecorationStyle.DOUBLE) ? FontUnderline.DOUBLE : FontUnderline.SINGLE);
				break;
			}
		}

		fontsUsed.put(fontStyles, font);

		return font;
	}
}

enum TableTag { TR, TH, TD };

class ValueStyles {

	public Object value;
	public Styles styles;

	private static Pattern[] patterns;

	static {
		final String regex = "\\A<%s +style *= *\"(.+?)\" *>(.*)\\z";

		patterns = new Pattern[TableTag.values().length];
		patterns[TableTag.TR.ordinal()] = Pattern.compile(String.format(regex, "tr"));
		patterns[TableTag.TH.ordinal()] = Pattern.compile(String.format(regex, "th"));
		patterns[TableTag.TD.ordinal()] = Pattern.compile(String.format(regex, "td"));
	}

	private ValueStyles(Object value, Styles styles) {
		this.value = value;
		this.styles = styles;
	}

	/**
	 * @param tag identifies an HTML tag
	 * @param object is potentially a string with HTML styling
	 * @return an always non-NULL object with fields set as follows.
	 * <p>
	 * If object is a string that starts with the indicated HTML tag
	 * including a style attribute, value returns the string with
	 * the tag removed, and styles returns a non-null Styles object
	 * reflecting the styling.
	 * <p>
	 * Otherwise, value returns the original object and styles returns null.
	 */
	public static ValueStyles parse(TableTag tag, Object object) {

		if (object instanceof String) {
			Matcher matcher = patterns[tag.ordinal()].matcher((String)object);

			if (matcher.find()) {
				String value = matcher.group(2);
				String styling = matcher.group(1);
				Styles styles = Styles.parse(styling);
				return new ValueStyles(value, styles);
			}
		}
		return new ValueStyles(object, null);
	}
}

enum RowPosition { HEADER, NEXT /* after the header row */, TOP /* if there is no header row */, MIDDLE, BOTTOM };

enum ColumnPosition { SINGLE /* if there is only one column */, LEFT , MIDDLE, RIGHT };

class ResolvedSheetStyles extends SheetStyles {

	private RowStyles[] rowStyles;

	public RowStyles getStyles(RowPosition rowPosition) {
		return rowStyles[rowPosition.ordinal()];
	}

	private boolean areDefault = true;

	public boolean areDefault() {
		return areDefault;
	}

	public ResolvedSheetStyles(XlsxTargetSheet.TargetOptions options) {

		super(options);

		rowStyles = new RowStyles[RowPosition.values().length];

		rowStyles[RowPosition.HEADER.ordinal()] = new RowStyles();
		rowStyles[RowPosition.NEXT.ordinal()] = new RowStyles();
		rowStyles[RowPosition.TOP.ordinal()] = new RowStyles();
		rowStyles[RowPosition.MIDDLE.ordinal()] = new RowStyles();
		rowStyles[RowPosition.BOTTOM.ordinal()] = new RowStyles();

		for (RowPosition rowPosition : RowPosition.values()) {
			for (ColumnPosition columnPosition : ColumnPosition.values()) {

				Styles positionStyles = super.resolve(null, null, rowPosition, columnPosition);
				getStyles(rowPosition).setStyles(columnPosition, positionStyles);

				if (!positionStyles.areDefault()) {
					areDefault = false;
				}
			}
		}
	}

	public Styles resolve(
			Styles rowStyles,
			Styles cellStyles,
			RowPosition rowPosition,
			ColumnPosition columnPosition) {

		if ((rowStyles == null) && (cellStyles == null)) {
			return getStyles(rowPosition).getStyles(columnPosition);
		}
		else {
			return super.resolve(rowStyles, cellStyles, rowPosition, columnPosition);
		}
	}
}

class SheetStyles {

	public Styles tableStyles;
	public Styles headStyles;
	public Styles bodyStyles;
	public Styles headCellStyles;
	public Styles bodyCellStyles;

	public SheetStyles(XlsxTargetSheet.TargetOptions options) {

		tableStyles = Styles.parse(options.getTableStyle());
		headStyles = Styles.parse(options.getHeadStyle());
		bodyStyles = Styles.parse(options.getBodyStyle());
		headCellStyles = Styles.parse(options.getHeadCellStyle());
		bodyCellStyles = Styles.parse(options.getBodyCellStyle());
	}

	public Styles resolve(
			Styles rowStyles,
			Styles cellStyles,
			RowPosition rowPosition,
			ColumnPosition columnPosition) {

		Styles result = new Styles();

		// In styling shared borders between cells, keep in mind that styles are determined left to right across cells and
		// top to bottom across rows.  Therefore, a shared border style may already be determined, in which case it should
		// not be overridden except by cell-specific styling and sometimes row-specific styling.

		// Top borders

		switch (rowPosition) {
		case HEADER: {

			result.topBorderWidth = Styles.resolve(Styles.topBorderWidthGetter, cellStyles, headCellStyles, rowStyles, headStyles, tableStyles);
			result.topBorderStyle = Styles.resolve(Styles.topBorderStyleGetter, cellStyles, headCellStyles, rowStyles, headStyles, tableStyles);
			result.topBorderColor = Styles.resolve(Styles.topBorderColorGetter, cellStyles, headCellStyles, rowStyles, headStyles, tableStyles);

			break;
		}
		case TOP: {

			result.topBorderWidth = Styles.resolve(Styles.topBorderWidthGetter, cellStyles, bodyCellStyles, rowStyles, bodyStyles, tableStyles);
			result.topBorderStyle = Styles.resolve(Styles.topBorderStyleGetter, cellStyles, bodyCellStyles, rowStyles, bodyStyles, tableStyles);
			result.topBorderColor = Styles.resolve(Styles.topBorderColorGetter, cellStyles, bodyCellStyles, rowStyles, bodyStyles, tableStyles);

			break;
		}
		case NEXT:
		case MIDDLE:
		case BOTTOM: {

			result.topBorderWidth = Styles.resolve(Styles.topBorderWidthGetter, cellStyles, rowStyles);
			result.topBorderStyle = Styles.resolve(Styles.topBorderStyleGetter, cellStyles, rowStyles);
			result.topBorderColor = Styles.resolve(Styles.topBorderColorGetter, cellStyles, rowStyles);

			break;
		}
		}

		// Left and right borders, backgrounds, fonts

		switch (rowPosition) {
		case HEADER: {
			switch (columnPosition) {
			case SINGLE:
			case LEFT:{
				result.leftBorderWidth = Styles.resolve(Styles.leftBorderWidthGetter, cellStyles, headCellStyles, rowStyles, headStyles, tableStyles);
				result.leftBorderStyle = Styles.resolve(Styles.leftBorderStyleGetter, cellStyles, headCellStyles, rowStyles, headStyles, tableStyles);
				result.leftBorderColor = Styles.resolve(Styles.leftBorderColorGetter, cellStyles, headCellStyles, rowStyles, headStyles, tableStyles);
				break;
			}
			case MIDDLE:
			case RIGHT: {
				result.leftBorderWidth = Styles.resolve(Styles.leftBorderWidthGetter, cellStyles);
				result.leftBorderStyle = Styles.resolve(Styles.leftBorderStyleGetter, cellStyles);
				result.leftBorderColor = Styles.resolve(Styles.leftBorderColorGetter, cellStyles);
				break;
			}
			}

			switch (columnPosition) {
			case LEFT:
			case MIDDLE: {
				result.rightBorderWidth = Styles.resolve(Styles.rightBorderWidthGetter, cellStyles, headCellStyles);
				result.rightBorderStyle = Styles.resolve(Styles.rightBorderStyleGetter, cellStyles, headCellStyles);
				result.rightBorderColor = Styles.resolve(Styles.rightBorderColorGetter, cellStyles, headCellStyles);
				break;
			}
			case SINGLE:
			case RIGHT: {
				result.rightBorderWidth = Styles.resolve(Styles.rightBorderWidthGetter, cellStyles, headCellStyles, rowStyles, headStyles, tableStyles);
				result.rightBorderStyle = Styles.resolve(Styles.rightBorderStyleGetter, cellStyles, headCellStyles, rowStyles, headStyles, tableStyles);
				result.rightBorderColor = Styles.resolve(Styles.rightBorderColorGetter, cellStyles, headCellStyles, rowStyles, headStyles, tableStyles);
				break;
			}
			}

			result.backgroundColor = Styles.resolve(Styles.backgroundColorGetter, cellStyles, headCellStyles, rowStyles, headStyles, tableStyles);

			result.font.color = Styles.resolve(FontStyles.colorGetter, cellStyles, headCellStyles, rowStyles, headStyles, tableStyles);
			result.font.fontStyle = Styles.resolve(FontStyles.fontStyleGetter, cellStyles, headCellStyles, rowStyles, headStyles, tableStyles);
			result.font.fontWeight = Styles.resolve(FontStyles.fontWeightGetter, cellStyles, headCellStyles, rowStyles, headStyles, tableStyles);
			result.font.textDecorationLine = Styles.resolve(FontStyles.textDecorationLineGetter, cellStyles, headCellStyles, rowStyles, headStyles, tableStyles);
			result.font.textDecorationStyle = Styles.resolve(FontStyles.textDecorationStyleGetter, cellStyles, headCellStyles, rowStyles, headStyles, tableStyles);

			break;
		}
		case NEXT:
		case TOP:
		case MIDDLE:
		case BOTTOM: {
			switch (columnPosition) {
			case SINGLE:
			case LEFT:{
				result.leftBorderWidth = Styles.resolve(Styles.leftBorderWidthGetter, cellStyles, bodyCellStyles, rowStyles, bodyStyles, tableStyles);
				result.leftBorderStyle = Styles.resolve(Styles.leftBorderStyleGetter, cellStyles, bodyCellStyles, rowStyles, bodyStyles, tableStyles);
				result.leftBorderColor = Styles.resolve(Styles.leftBorderColorGetter, cellStyles, bodyCellStyles, rowStyles, bodyStyles, tableStyles);
				break;
			}
			case MIDDLE:
			case RIGHT: {
				result.leftBorderWidth = Styles.resolve(Styles.leftBorderWidthGetter, cellStyles);
				result.leftBorderStyle = Styles.resolve(Styles.leftBorderStyleGetter, cellStyles);
				result.leftBorderColor = Styles.resolve(Styles.leftBorderColorGetter, cellStyles);
				break;
			}
			}

			switch (columnPosition) {
			case LEFT:
			case MIDDLE: {
				result.rightBorderWidth = Styles.resolve(Styles.rightBorderWidthGetter, cellStyles, bodyCellStyles);
				result.rightBorderStyle = Styles.resolve(Styles.rightBorderStyleGetter, cellStyles, bodyCellStyles);
				result.rightBorderColor = Styles.resolve(Styles.rightBorderColorGetter, cellStyles, bodyCellStyles);
				break;
			}
			case SINGLE:
			case RIGHT: {
				result.rightBorderWidth = Styles.resolve(Styles.rightBorderWidthGetter, cellStyles, bodyCellStyles, rowStyles, bodyStyles, tableStyles);
				result.rightBorderStyle = Styles.resolve(Styles.rightBorderStyleGetter, cellStyles, bodyCellStyles, rowStyles, bodyStyles, tableStyles);
				result.rightBorderColor = Styles.resolve(Styles.rightBorderColorGetter, cellStyles, bodyCellStyles, rowStyles, bodyStyles, tableStyles);
				break;
			}
			}

			result.backgroundColor = Styles.resolve(Styles.backgroundColorGetter, cellStyles, bodyCellStyles, rowStyles, bodyStyles, tableStyles);

			result.font.color = Styles.resolve(FontStyles.colorGetter, cellStyles, bodyCellStyles, rowStyles, bodyStyles, tableStyles);
			result.font.fontStyle = Styles.resolve(FontStyles.fontStyleGetter, cellStyles, bodyCellStyles, rowStyles, bodyStyles, tableStyles);
			result.font.fontWeight = Styles.resolve(FontStyles.fontWeightGetter, cellStyles, bodyCellStyles, rowStyles, bodyStyles, tableStyles);
			result.font.textDecorationLine = Styles.resolve(FontStyles.textDecorationLineGetter, cellStyles, bodyCellStyles, rowStyles, bodyStyles, tableStyles);
			result.font.textDecorationStyle = Styles.resolve(FontStyles.textDecorationStyleGetter, cellStyles, bodyCellStyles, rowStyles, bodyStyles, tableStyles);

			break;
		}
		}

		// Bottom borders

		switch (rowPosition) {
		case HEADER: {

			result.bottomBorderWidth = Styles.resolve(Styles.resolve(
					Styles.bottomBorderWidthGetter, cellStyles, headCellStyles, rowStyles, headStyles),
					Styles.topBorderWidthGetter, bodyCellStyles, bodyStyles);
			result.bottomBorderStyle = Styles.resolve(Styles.resolve(
					Styles.bottomBorderStyleGetter, cellStyles, headCellStyles, rowStyles, headStyles),
					Styles.topBorderStyleGetter, bodyCellStyles, bodyStyles);
			result.bottomBorderColor = Styles.resolve(Styles.resolve(
					Styles.bottomBorderColorGetter, cellStyles, headCellStyles, rowStyles, headStyles),
					Styles.topBorderColorGetter, bodyCellStyles, bodyStyles);

			break;
		}
		case NEXT:
		case TOP:
		case MIDDLE: {

			result.bottomBorderWidth = Styles.resolve(Styles.resolve(
					Styles.bottomBorderWidthGetter, cellStyles, bodyCellStyles, rowStyles),
					Styles.topBorderWidthGetter, bodyCellStyles);
			result.bottomBorderStyle = Styles.resolve(Styles.resolve(
					Styles.bottomBorderStyleGetter, cellStyles, bodyCellStyles, rowStyles),
					Styles.topBorderStyleGetter, bodyCellStyles);
			result.bottomBorderColor = Styles.resolve(Styles.resolve(
					Styles.bottomBorderColorGetter, cellStyles, bodyCellStyles, rowStyles),
					Styles.topBorderColorGetter, bodyCellStyles);

			break;
		}
		case BOTTOM: {

			result.bottomBorderWidth = Styles.resolve(Styles.bottomBorderWidthGetter, cellStyles, bodyCellStyles, rowStyles, bodyStyles, tableStyles);
			result.bottomBorderStyle = Styles.resolve(Styles.bottomBorderStyleGetter, cellStyles, bodyCellStyles, rowStyles, bodyStyles, tableStyles);
			result.bottomBorderColor = Styles.resolve(Styles.bottomBorderColorGetter, cellStyles, bodyCellStyles, rowStyles, bodyStyles, tableStyles);

			break;
		}
		}

		return result;
	}
}

class RowStyles {

	private Styles[] styles;

	public RowStyles() {
		styles = new Styles[ColumnPosition.values().length];

		styles[ColumnPosition.SINGLE.ordinal()] = new Styles();
		styles[ColumnPosition.LEFT.ordinal()] = new Styles();
		styles[ColumnPosition.MIDDLE.ordinal()] = new Styles();
		styles[ColumnPosition.RIGHT.ordinal()] = new Styles();
	}

	public Styles getStyles(ColumnPosition columnPosition) {
		return styles[columnPosition.ordinal()];
	}

	public void setStyles(ColumnPosition columnPosition, Styles styles) {
		this.styles[columnPosition.ordinal()] = styles;
	}
}

abstract class AnyStyles {

	public static <Type> boolean areSame(Type one, Type other) {
		return (one == null) ? (other == null) : one.equals(other);
	}

	@FunctionalInterface
	public static interface StylesGetter<Type> {
		Type get(Styles styles);
	}
}

class FontStyles extends AnyStyles {

	public Integer color = null;
	public FontStyle fontStyle = null;
	public FontWeight fontWeight = null;
	public TextDecorationLine textDecorationLine = null;
	public TextDecorationStyle textDecorationStyle = null;

	public static final StylesGetter<Integer> colorGetter = new StylesGetter<Integer>() {public Integer get(Styles styles) {return styles.font.color;}};
	public static final StylesGetter<FontStyle> fontStyleGetter = new StylesGetter<FontStyle>() {public FontStyle get(Styles styles) {return styles.font.fontStyle;}};
	public static final StylesGetter<FontWeight> fontWeightGetter = new StylesGetter<FontWeight>() {public FontWeight get(Styles styles) {return styles.font.fontWeight;}};
	public static final StylesGetter<TextDecorationLine> textDecorationLineGetter = new StylesGetter<TextDecorationLine>() {public TextDecorationLine get(Styles styles) {return styles.font.textDecorationLine;}};
	public static final StylesGetter<TextDecorationStyle> textDecorationStyleGetter = new StylesGetter<TextDecorationStyle>() {public TextDecorationStyle get(Styles styles) {return styles.font.textDecorationStyle;}};

	@Override
	public int hashCode() {
		return
				// 20 bits total
				((color != null) ? color.intValue() : 0x1000000) ^
				((fontStyle != null) ? fontStyle.ordinal() : FontStyle.values().length) << 12 ^
				((fontWeight != null) ? fontWeight.ordinal() : FontWeight.values().length) << 14 ^
				((textDecorationLine != null) ? textDecorationLine.ordinal() : TextDecorationLine.values().length) << 16 ^
				((textDecorationStyle != null) ? textDecorationStyle.ordinal() : TextDecorationStyle.values().length) << 18;
	}

	@Override
	public boolean equals(Object obj) {

		if (!(obj instanceof FontStyles)) { return false; }

		FontStyles other = (FontStyles)obj;
		return
				areSame(color, other.color) &&
				areSame(fontStyle, other.fontStyle) &&
				areSame(fontWeight, other.fontWeight) &&
				areSame(textDecorationLine, other.textDecorationLine) &&
				areSame(textDecorationStyle, other.textDecorationStyle);
	}

	public boolean areDefault() {
		return
				color == null &&
				fontStyle == null &&
				fontWeight == null &&
				textDecorationLine == null &&
				textDecorationStyle == null;
	}

	public enum FontStyle { NORMAL, ITALIC };

	public static boolean isFontStyle(String fontProperty) {
		return getFontStyle(fontProperty) != null;
	}

	public static FontStyle getFontStyle(String fontProperty) {
		switch (fontProperty) {
		case "normal": return FontStyle.NORMAL;
		case "italic": return FontStyle.ITALIC;
		default: return null;
		}
	}

	public enum FontWeight { NORMAL, BOLD };

	public static boolean isFontWeight(String fontProperty) {
		return getFontWeight(fontProperty) != null;
	}

	public static FontWeight getFontWeight(String fontProperty) {
		switch (fontProperty) {
		case "normal": return FontWeight.NORMAL;
		case "bold": return FontWeight.BOLD;
		default: return null;
		}
	}

	public enum TextDecorationLine { NONE, LINE_THROUGH, UNDERLINE };

	public static boolean isTextDecorationLine(String textProperty) {
		return getTextDecorationLine(textProperty) != null;
	}

	public static TextDecorationLine getTextDecorationLine(String textProperty) {
		switch (textProperty) {
		case "none": return TextDecorationLine.NONE;
		case "line-through": return TextDecorationLine.LINE_THROUGH;
		case "underline": return TextDecorationLine.UNDERLINE;
		default: return null;
		}
	}

	public enum TextDecorationStyle { SINGLE, DOUBLE };

	public static boolean isTextDecorationStyle(String textProperty) {
		return getTextDecorationStyle(textProperty) != null;
	}

	public static TextDecorationStyle getTextDecorationStyle(String textProperty) {
		switch (textProperty) {
		case "solid": return TextDecorationStyle.SINGLE;
		case "double": return TextDecorationStyle.DOUBLE;
		default: return null;
		}
	}
}

class Styles extends AnyStyles {

	public BorderStyle bottomBorderWidth = null;
	public BorderStyle leftBorderWidth = null;
	public BorderStyle rightBorderWidth = null;
	public BorderStyle topBorderWidth = null;

	public BorderStyle bottomBorderStyle = null;
	public BorderStyle leftBorderStyle = null;
	public BorderStyle rightBorderStyle = null;
	public BorderStyle topBorderStyle = null;

	public Integer bottomBorderColor = null;
	public Integer leftBorderColor = null;
	public Integer rightBorderColor = null;
	public Integer topBorderColor = null;

	public Integer backgroundColor = null;

	public FontStyles font = new FontStyles();

	public static final StylesGetter<BorderStyle> bottomBorderWidthGetter = new StylesGetter<BorderStyle>() {public BorderStyle get(Styles styles) {return styles.bottomBorderWidth;}};
	public static final StylesGetter<BorderStyle> leftBorderWidthGetter = new StylesGetter<BorderStyle>() {public BorderStyle get(Styles styles) {return styles.leftBorderWidth;}};
	public static final StylesGetter<BorderStyle> rightBorderWidthGetter = new StylesGetter<BorderStyle>() {public BorderStyle get(Styles styles) {return styles.rightBorderWidth;}};
	public static final StylesGetter<BorderStyle> topBorderWidthGetter = new StylesGetter<BorderStyle>() {public BorderStyle get(Styles styles) {return styles.topBorderWidth;}};

	public static final StylesGetter<BorderStyle> bottomBorderStyleGetter = new StylesGetter<BorderStyle>() {public BorderStyle get(Styles styles) {return styles.bottomBorderStyle;}};
	public static final StylesGetter<BorderStyle> leftBorderStyleGetter = new StylesGetter<BorderStyle>() {public BorderStyle get(Styles styles) {return styles.leftBorderStyle;}};
	public static final StylesGetter<BorderStyle> rightBorderStyleGetter = new StylesGetter<BorderStyle>() {public BorderStyle get(Styles styles) {return styles.rightBorderStyle;}};
	public static final StylesGetter<BorderStyle> topBorderStyleGetter = new StylesGetter<BorderStyle>() {public BorderStyle get(Styles styles) {return styles.topBorderStyle;}};

	public static final StylesGetter<Integer> bottomBorderColorGetter = new StylesGetter<Integer>() {public Integer get(Styles styles) {return styles.bottomBorderColor;}};
	public static final StylesGetter<Integer> leftBorderColorGetter = new StylesGetter<Integer>() {public Integer get(Styles styles) {return styles.leftBorderColor;}};
	public static final StylesGetter<Integer> rightBorderColorGetter = new StylesGetter<Integer>() {public Integer get(Styles styles) {return styles.rightBorderColor;}};
	public static final StylesGetter<Integer> topBorderColorGetter = new StylesGetter<Integer>() {public Integer get(Styles styles) {return styles.topBorderColor;}};

	public static final StylesGetter<Integer> backgroundColorGetter = new StylesGetter<Integer>() {public Integer get(Styles styles) {return styles.backgroundColor;}};

	@Override
	public int hashCode() {
		return
				((bottomBorderWidth != null) ? bottomBorderWidth.ordinal() : BorderStyle.values().length) ^
				((leftBorderWidth != null) ? leftBorderWidth.ordinal() : BorderStyle.values().length) << 4 ^
				((rightBorderWidth != null) ? rightBorderWidth.ordinal() : BorderStyle.values().length) << 8 ^
				((topBorderWidth != null) ? topBorderWidth.ordinal() : BorderStyle.values().length) << 12 ^

				((bottomBorderStyle != null) ? bottomBorderStyle.ordinal() : BorderStyle.values().length) << 16 ^
				((leftBorderStyle != null) ? leftBorderStyle.ordinal() : BorderStyle.values().length) << 20 ^
				((rightBorderStyle != null) ? rightBorderStyle.ordinal() : BorderStyle.values().length) << 24 ^
				((topBorderStyle != null) ? topBorderStyle.ordinal() : BorderStyle.values().length) << 28 ^

				((bottomBorderColor != null) ? bottomBorderColor.intValue() : 0x1000000) ^
				((leftBorderColor != null) ? leftBorderColor.intValue() : 0x1000000) << 1 ^
				((rightBorderColor != null) ? rightBorderColor.intValue() : 0x1000000) << 2 ^
				((topBorderColor != null) ? topBorderColor.intValue() : 0x1000000) << 3 ^

				((backgroundColor != null) ? backgroundColor.intValue() : 0x1000000) << 5 ^

				font.hashCode() << 8;
	}

	@Override
	public boolean equals(Object obj) {

		if (!(obj instanceof Styles)) { return false; }

		Styles other = (Styles)obj;
		return
				areSame(bottomBorderWidth, other.bottomBorderWidth) &&
				areSame(leftBorderWidth, other.leftBorderWidth) &&
				areSame(rightBorderWidth, other.rightBorderWidth) &&
				areSame(topBorderWidth, other.topBorderWidth) &&

				areSame(bottomBorderStyle, other.bottomBorderStyle) &&
				areSame(leftBorderStyle, other.leftBorderStyle) &&
				areSame(rightBorderStyle, other.rightBorderStyle) &&
				areSame(topBorderStyle, other.topBorderStyle) &&

				areSame(bottomBorderColor, other.bottomBorderColor) &&
				areSame(leftBorderColor, other.leftBorderColor) &&
				areSame(rightBorderColor, other.rightBorderColor) &&
				areSame(topBorderColor, other.topBorderColor) &&

				areSame(backgroundColor, other.backgroundColor) &&

				font.equals(other.font);
	}

	public boolean areDefault() {
		return
				bottomBorderWidth == null &&
				leftBorderWidth == null &&
				rightBorderWidth == null &&
				topBorderWidth == null &&

				bottomBorderStyle == null &&
				leftBorderStyle == null &&
				rightBorderStyle == null &&
				topBorderStyle == null &&

				bottomBorderColor == null &&
				leftBorderColor == null &&
				rightBorderColor == null &&
				topBorderColor == null &&

				backgroundColor == null &&

				font.areDefault();
	}

	/**
	 * @param styling is an HTML style attribute string
	 * @return an always non-null Styles object reflecting the styling
	 */
	public static Styles parse(String styling) {

		Styles result = new Styles();

		if (styling != null) {

			String[] properties = styling.split(";");
			for (String property : properties) {

				String[] keywordValue = property.split(":");
				if (keywordValue.length == 2) {

					String keyword = keywordValue[0].toLowerCase().trim();
					String value = keywordValue[1].toLowerCase().trim();

					if (keyword.equals("background-color")) {
						result.backgroundColor = getColor(value);
					}
					else if (keyword.equals("border")) {
						String[] borderProperties = value.split(" +");
						for (String borderProperty : borderProperties) {
							if (isBorderWidth(borderProperty)) {
								result.bottomBorderWidth = getBorderWidth(borderProperty);
								result.leftBorderWidth = getBorderWidth(borderProperty);
								result.rightBorderWidth = getBorderWidth(borderProperty);
								result.topBorderWidth = getBorderWidth(borderProperty);
							}
							else if (isBorderStyle(borderProperty)) {
								result.bottomBorderStyle = getBorderStyle(borderProperty);
								result.leftBorderStyle = getBorderStyle(borderProperty);
								result.rightBorderStyle = getBorderStyle(borderProperty);
								result.topBorderStyle = getBorderStyle(borderProperty);
							}
							else if (isColor(borderProperty)) {
								result.bottomBorderColor = getColor(borderProperty);
								result.leftBorderColor = getColor(borderProperty);
								result.rightBorderColor = getColor(borderProperty);
								result.topBorderColor = getColor(borderProperty);
							}
						}
					}
					else if (keyword.equals("border-bottom")) {
						String[] borderBottomProperties = value.split(" +");
						for (String borderProperty : borderBottomProperties) {
							if (isBorderWidth(borderProperty)) {
								result.bottomBorderWidth = getBorderWidth(borderProperty);
							}
							else if (isBorderStyle(borderProperty)) {
								result.bottomBorderStyle = getBorderStyle(borderProperty);
							}
							else if (isColor(borderProperty)) {
								result.bottomBorderColor = getColor(borderProperty);
							}
						}
					}
					else if (keyword.equals("border-bottom-color")) {
						result.bottomBorderColor = getColor(value);
					}
					else if (keyword.equals("border-bottom-style")) {
						result.bottomBorderStyle = getBorderStyle(value);
					}
					else if (keyword.equals("border-bottom-width")) {
						result.bottomBorderWidth = getBorderWidth(value);
					}
					else if (keyword.equals("border-color")) {
						result.bottomBorderColor = getColor(value);
						result.leftBorderColor = getColor(value);
						result.rightBorderColor = getColor(value);
						result.topBorderColor = getColor(value);
					}
					else if (keyword.equals("border-left")) {
						String[] borderLeftProperties = value.split(" +");
						for (String borderProperty : borderLeftProperties) {
							if (isBorderWidth(borderProperty)) {
								result.leftBorderWidth = getBorderWidth(borderProperty);
							}
							else if (isBorderStyle(borderProperty)) {
								result.leftBorderStyle = getBorderStyle(borderProperty);
							}
							else if (isColor(borderProperty)) {
								result.leftBorderColor = getColor(borderProperty);
							}
						}
					}
					else if (keyword.equals("border-left-color")) {
						result.leftBorderColor = getColor(value);
					}
					else if (keyword.equals("border-left-style")) {
						result.leftBorderStyle = getBorderStyle(value);
					}
					else if (keyword.equals("border-left-width")) {
						result.leftBorderWidth = getBorderWidth(value);
					}
					else if (keyword.equals("border-right")) {
						String[] borderRightProperties = value.split(" +");
						for (String borderProperty : borderRightProperties) {
							if (isBorderWidth(borderProperty)) {
								result.rightBorderWidth = getBorderWidth(borderProperty);
							}
							else if (isBorderStyle(borderProperty)) {
								result.rightBorderStyle = getBorderStyle(borderProperty);
							}
							else if (isColor(borderProperty)) {
								result.rightBorderColor = getColor(borderProperty);
							}
						}
					}
					else if (keyword.equals("border-right-color")) {
						result.rightBorderColor = getColor(value);
					}
					else if (keyword.equals("border-right-style")) {
						result.rightBorderStyle = getBorderStyle(value);
					}
					else if (keyword.equals("border-right-width")) {
						result.rightBorderWidth = getBorderWidth(value);
					}
					else if (keyword.equals("border-style")) {
						result.bottomBorderStyle = getBorderStyle(value);
						result.leftBorderStyle = getBorderStyle(value);
						result.rightBorderStyle = getBorderStyle(value);
						result.topBorderStyle = getBorderStyle(value);
					}
					else if (keyword.equals("border-top")) {
						String[] borderTopProperties = value.split(" ");
						for (String borderProperty : borderTopProperties) {
							if (isBorderWidth(borderProperty)) {
								result.topBorderWidth = getBorderWidth(borderProperty);
							}
							else if (isBorderStyle(borderProperty)) {
								result.topBorderStyle = getBorderStyle(borderProperty);
							}
							else if (isColor(borderProperty)) {
								result.topBorderColor = getColor(borderProperty);
							}
						}
					}
					else if (keyword.equals("border-top-color")) {
						result.topBorderColor = getColor(value);
					}
					else if (keyword.equals("border-top-style")) {
						result.topBorderStyle = getBorderStyle(value);
					}
					else if (keyword.equals("border-width")) {
						result.topBorderWidth = getBorderWidth(value);
					}
					else if (keyword.equals("color")) {
						result.font.color = getColor(value);
					}
					else if (keyword.equals("font")) {
						String[] fontProperties = value.split(" +");
						for (String fontProperty : fontProperties) {
							if (FontStyles.isFontStyle(fontProperty)) {
								result.font.fontStyle = FontStyles.getFontStyle(fontProperty);
							}
							else if (FontStyles.isFontWeight(fontProperty)) {
								result.font.fontWeight = FontStyles.getFontWeight(fontProperty);
							}
						}
					}
					else if (keyword.equals("font-style")) {
						result.font.fontStyle = FontStyles.getFontStyle(value);
					}
					else if (keyword.equals("font-weight")) {
						result.font.fontWeight = FontStyles.getFontWeight(value);
					}
					else if (keyword.equals("text-decoration")) {
						String[] textProperties = value.split(" +");
						for (String textProperty : textProperties) {
							if (FontStyles.isTextDecorationLine(textProperty)) {
								result.font.textDecorationLine = FontStyles.getTextDecorationLine(textProperty);
							}
							else if (FontStyles.isTextDecorationStyle(textProperty)) {
								result.font.textDecorationStyle = FontStyles.getTextDecorationStyle(textProperty);
							}
						}
					}
					else if (keyword.equals("text-decoration-line")) {
						result.font.textDecorationLine = FontStyles.getTextDecorationLine(value);
					}
					else if (keyword.equals("text-decoration-style")) {
						result.font.textDecorationStyle = FontStyles.getTextDecorationStyle(value);
					}
				}
			}
		}

		return result;
	}

	private static boolean isColor(String property) {
		return getColor(property) != null;
	}

	private static Integer getColor(String property) {
		return cssColor.get(property);
	}

	private static Map<String, Integer> cssColor;
	static {
		// From https://www.w3schools.com/cssref/css_colors.asp
		cssColor = new HashMap<String, Integer>();
		putCssColor("AliceBlue", 0xF0F8FF);
		putCssColor("AntiqueWhite", 0xFAEBD7);
		putCssColor("Aqua", 0x00FFFF);
		putCssColor("Aquamarine", 0x7FFFD4);
		putCssColor("Azure", 0xF0FFFF);
		putCssColor("Beige", 0xF5F5DC);
		putCssColor("Bisque", 0xFFE4C4);
		putCssColor("Black", 0x000000);
		putCssColor("BlanchedAlmond", 0xFFEBCD);
		putCssColor("Blue", 0x0000FF);
		putCssColor("BlueViolet", 0x8A2BE2);
		putCssColor("Brown", 0xA52A2A);
		putCssColor("BurlyWood", 0xDEB887);
		putCssColor("CadetBlue", 0x5F9EA0);
		putCssColor("Chartreuse", 0x7FFF00);
		putCssColor("Chocolate", 0xD2691E);
		putCssColor("Coral", 0xFF7F50);
		putCssColor("CornflowerBlue", 0x6495ED);
		putCssColor("Cornsilk", 0xFFF8DC);
		putCssColor("Crimson", 0xDC143C);
		putCssColor("Cyan", 0x00FFFF);
		putCssColor("DarkBlue", 0x00008B);
		putCssColor("DarkCyan", 0x008B8B);
		putCssColor("DarkGoldenRod", 0xB8860B);
		putCssColor("DarkGray", 0xA9A9A9);
		putCssColor("DarkGrey", 0xA9A9A9);
		putCssColor("DarkGreen", 0x006400);
		putCssColor("DarkKhaki", 0xBDB76B);
		putCssColor("DarkMagenta", 0x8B008B);
		putCssColor("DarkOliveGreen", 0x556B2F);
		putCssColor("DarkOrange", 0xFF8C00);
		putCssColor("DarkOrchid", 0x9932CC);
		putCssColor("DarkRed", 0x8B0000);
		putCssColor("DarkSalmon", 0xE9967A);
		putCssColor("DarkSeaGreen", 0x8FBC8F);
		putCssColor("DarkSlateBlue", 0x483D8B);
		putCssColor("DarkSlateGray", 0x2F4F4F);
		putCssColor("DarkSlateGrey", 0x2F4F4F);
		putCssColor("DarkTurquoise", 0x00CED1);
		putCssColor("DarkViolet", 0x9400D3);
		putCssColor("DeepPink", 0xFF1493);
		putCssColor("DeepSkyBlue", 0x00BFFF);
		putCssColor("DimGray", 0x696969);
		putCssColor("DimGrey", 0x696969);
		putCssColor("DodgerBlue", 0x1E90FF);
		putCssColor("FireBrick", 0xB22222);
		putCssColor("FloralWhite", 0xFFFAF0);
		putCssColor("ForestGreen", 0x228B22);
		putCssColor("Fuchsia", 0xFF00FF);
		putCssColor("Gainsboro", 0xDCDCDC);
		putCssColor("GhostWhite", 0xF8F8FF);
		putCssColor("Gold", 0xFFD700);
		putCssColor("GoldenRod", 0xDAA520);
		putCssColor("Gray", 0x808080);
		putCssColor("Grey", 0x808080);
		putCssColor("Green", 0x008000);
		putCssColor("GreenYellow", 0xADFF2F);
		putCssColor("HoneyDew", 0xF0FFF0);
		putCssColor("HotPink", 0xFF69B4);
		putCssColor("IndianRed ", 0xCD5C5C);
		putCssColor("Indigo ", 0x4B0082);
		putCssColor("Ivory", 0xFFFFF0);
		putCssColor("Khaki", 0xF0E68C);
		putCssColor("Lavender", 0xE6E6FA);
		putCssColor("LavenderBlush", 0xFFF0F5);
		putCssColor("LawnGreen", 0x7CFC00);
		putCssColor("LemonChiffon", 0xFFFACD);
		putCssColor("LightBlue", 0xADD8E6);
		putCssColor("LightCoral", 0xF08080);
		putCssColor("LightCyan", 0xE0FFFF);
		putCssColor("LightGoldenRodYellow", 0xFAFAD2);
		putCssColor("LightGray", 0xD3D3D3);
		putCssColor("LightGrey", 0xD3D3D3);
		putCssColor("LightGreen", 0x90EE90);
		putCssColor("LightPink", 0xFFB6C1);
		putCssColor("LightSalmon", 0xFFA07A);
		putCssColor("LightSeaGreen", 0x20B2AA);
		putCssColor("LightSkyBlue", 0x87CEFA);
		putCssColor("LightSlateGray", 0x778899);
		putCssColor("LightSlateGrey", 0x778899);
		putCssColor("LightSteelBlue", 0xB0C4DE);
		putCssColor("LightYellow", 0xFFFFE0);
		putCssColor("Lime", 0x00FF00);
		putCssColor("LimeGreen", 0x32CD32);
		putCssColor("Linen", 0xFAF0E6);
		putCssColor("Magenta", 0xFF00FF);
		putCssColor("Maroon", 0x800000);
		putCssColor("MediumAquaMarine", 0x66CDAA);
		putCssColor("MediumBlue", 0x0000CD);
		putCssColor("MediumOrchid", 0xBA55D3);
		putCssColor("MediumPurple", 0x9370DB);
		putCssColor("MediumSeaGreen", 0x3CB371);
		putCssColor("MediumSlateBlue", 0x7B68EE);
		putCssColor("MediumSpringGreen", 0x00FA9A);
		putCssColor("MediumTurquoise", 0x48D1CC);
		putCssColor("MediumVioletRed", 0xC71585);
		putCssColor("MidnightBlue", 0x191970);
		putCssColor("MintCream", 0xF5FFFA);
		putCssColor("MistyRose", 0xFFE4E1);
		putCssColor("Moccasin", 0xFFE4B5);
		putCssColor("NavajoWhite", 0xFFDEAD);
		putCssColor("Navy", 0x000080);
		putCssColor("OldLace", 0xFDF5E6);
		putCssColor("Olive", 0x808000);
		putCssColor("OliveDrab", 0x6B8E23);
		putCssColor("Orange", 0xFFA500);
		putCssColor("OrangeRed", 0xFF4500);
		putCssColor("Orchid", 0xDA70D6);
		putCssColor("PaleGoldenRod", 0xEEE8AA);
		putCssColor("PaleGreen", 0x98FB98);
		putCssColor("PaleTurquoise", 0xAFEEEE);
		putCssColor("PaleVioletRed", 0xDB7093);
		putCssColor("PapayaWhip", 0xFFEFD5);
		putCssColor("PeachPuff", 0xFFDAB9);
		putCssColor("Peru", 0xCD853F);
		putCssColor("Pink", 0xFFC0CB);
		putCssColor("Plum", 0xDDA0DD);
		putCssColor("PowderBlue", 0xB0E0E6);
		putCssColor("Purple", 0x800080);
		putCssColor("RebeccaPurple", 0x663399);
		putCssColor("Red", 0xFF0000);
		putCssColor("RosyBrown", 0xBC8F8F);
		putCssColor("RoyalBlue", 0x4169E1);
		putCssColor("SaddleBrown", 0x8B4513);
		putCssColor("Salmon", 0xFA8072);
		putCssColor("SandyBrown", 0xF4A460);
		putCssColor("SeaGreen", 0x2E8B57);
		putCssColor("SeaShell", 0xFFF5EE);
		putCssColor("Sienna", 0xA0522D);
		putCssColor("Silver", 0xC0C0C0);
		putCssColor("SkyBlue", 0x87CEEB);
		putCssColor("SlateBlue", 0x6A5ACD);
		putCssColor("SlateGray", 0x708090);
		putCssColor("SlateGrey", 0x708090);
		putCssColor("Snow", 0xFFFAFA);
		putCssColor("SpringGreen", 0x00FF7F);
		putCssColor("SteelBlue", 0x4682B4);
		putCssColor("Tan", 0xD2B48C);
		putCssColor("Teal", 0x008080);
		putCssColor("Thistle", 0xD8BFD8);
		putCssColor("Tomato", 0xFF6347);
		putCssColor("Turquoise", 0x40E0D0);
		putCssColor("Violet", 0xEE82EE);
		putCssColor("Wheat", 0xF5DEB3);
		putCssColor("White", 0xFFFFFF);
		putCssColor("WhiteSmoke", 0xF5F5F5);
		putCssColor("Yellow", 0xFFFF00);
		putCssColor("YellowGreen", 0x9ACD32);
	}

	private static void putCssColor(String name, Integer rgb) {
		cssColor.put(name.toLowerCase(), rgb);
	}

	private static boolean isBorderStyle(String borderProperty) {
		return getBorderStyle(borderProperty) != null;
	}

	private static BorderStyle getBorderStyle(String borderProperty) {
		switch (borderProperty) {
		case "none": return BorderStyle.NONE;
		case "hidden": return BorderStyle.NONE;
		case "dotted": return BorderStyle.DOTTED;
		case "dashed": return BorderStyle.MEDIUM_DASHED;
		case "solid": return BorderStyle.MEDIUM;
		case "double": return BorderStyle.DOUBLE;
		default: return null;
		}
	}

	public static boolean isBorderWidth(String borderProperty) {
		return getBorderWidth(borderProperty) != null;
	}

	public static BorderStyle getBorderWidth(String borderProperty) {
		switch (borderProperty) {
		case "thin": return BorderStyle.THIN;
		case "medium": return BorderStyle.MEDIUM;
		case "thick": return BorderStyle.THICK;
		default: return null;
		}
	}

	@SafeVarargs
	public static <Type> Type resolve(StylesGetter<Type> getter, Styles... stylesArray) {
		for (Styles styles : stylesArray) {
			if (styles != null) {
				Type result = getter.get(styles);
				if (result != null) {
					return result;
				}
			}
		}
		return null;
	}

	public static <Type> Type resolve(Type firstStyle, StylesGetter<Type> getter, Styles... stylesArray) {
		return (firstStyle != null) ? firstStyle : resolve(getter, stylesArray);
	}
}
