/*
 * Copyright (c) 2016, 2018, Ronald DeSantis
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
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.FontUnderline;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFColor;
import org.apache.poi.xssf.usermodel.XSSFFont;

import com.hauldata.dbpa.file.PageOptions;
import com.hauldata.dbpa.file.TargetHeaders;
import com.hauldata.dbpa.file.book.BorderStyles.BorderEdge;
import com.hauldata.dbpa.file.book.BorderStyles.BorderWidth;
import com.hauldata.dbpa.file.book.XlsxTargetBook.XlsxCellStyle;
import com.hauldata.dbpa.file.html.HtmlOptions;

public class XlsxTargetSheet extends XlsxSheet {

	private SXSSFSheet sheet;
	private int rowIndex;
	private ArrayList<Object> rowValues;
	private int columnCount;
	private ArrayList<Styles> previousRowCellStyles;

	private ResolvedSheetStyles sheetStyles;

	public XlsxTargetSheet(Book owner, String name, PageOptions options) {
		super(owner, name, options);

		sheet = null;
		rowIndex = 0;
		rowValues = new ArrayList<Object>();
		columnCount = 0;
		previousRowCellStyles = null;
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
				previousRowCellStyles = writeRow(rowValues, rowIndex, getRowPosition(rowIndex), previousRowCellStyles);
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

	private ArrayList<Styles> writeRow(ArrayList<Object> rowValues, int rowIndex, RowPosition rowPosition, ArrayList<Styles> previousRowCellStyles) {

		ArrayList<Styles> rowCellStyles = new ArrayList<Styles>();

		Styles rowStyles = null;
		if (getTargetOptions().isStyled()) {

			ValueStyles valueStyles = ValueStyles.parse(TableTag.TR, rowValues.get(0));
			rowValues.set(0, valueStyles.value);
			rowStyles = valueStyles.styles;
		}

		Styles leftStyles = null;

		Row row = sheet.createRow(rowIndex - 1);

		int columnIndex = 0;
		while (columnIndex < columnCount) {

			Styles aboveStyles = (previousRowCellStyles != null) ? previousRowCellStyles.get(columnIndex) : null;

			Object object = rowValues.get(columnIndex);

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
				leftStyles = setCellStyle(cell, cellStyles, rowStyles, sheetStyles, rowPosition, getColumnPosition(columnIndex), leftStyles, aboveStyles);
			}
			else {
				leftStyles = null;
			}
			rowCellStyles.add(leftStyles);
		}

		return rowCellStyles;
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
			cell.setCellStyle(getOwner().getCellStyle(XlsxCellStyle.INTEGER));
		}
		else if (object instanceof Long || object instanceof BigInteger) {
			cell.setCellValue(object.toString());
		}
		else if (object instanceof BigDecimal && ((BigDecimal)object).scale() == 2) {
			cell.setCellValue(((Number)object).doubleValue());
			cell.setCellStyle(getOwner().getCellStyle(XlsxCellStyle.MONEY));
		}
		else if (object instanceof Number) {
			cell.setCellValue(((Number)object).doubleValue());
		}
		else if (object instanceof String) {

			object = BigDollars.parse((String)object);
			if (object instanceof BigDollars) {
				cell.setCellValue(((BigDollars)object).getValue().doubleValue());
				cell.setCellStyle(getOwner().getCellStyle(XlsxCellStyle.COMMA));
			}
			else {
				cell.setCellValue((String)object);
			}
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
				style = getOwner().getCellStyle(XlsxCellStyle.DATE);
			}
			else {
				style = getOwner().getCellStyle(XlsxCellStyle.DATETIME);
			}
			cell.setCellStyle(style);
		}
		else {
			cell.setCellValue(object.toString());
		}
	}

	private Styles setCellStyle(
			Cell cell,
			Styles cellStyles,
			Styles rowStyles,
			ResolvedSheetStyles sheetStyles,
			RowPosition rowPosition,
			ColumnPosition columnPosition,
			Styles leftStyles,
			Styles aboveStyles) {

		Styles styles = sheetStyles.resolve(cellStyles, rowStyles, rowPosition, columnPosition, leftStyles, aboveStyles);

		adjustAdjacentCellStyle(cell, styles, rowPosition, columnPosition, leftStyles, aboveStyles);

		CellStyle originalStyle = cell.getCellStyle();

		CellStyle finalStyle = composeCellStyle(cell, styles);

		if (finalStyle != originalStyle) {
			cell.setCellStyle(finalStyle);
		}

		return styles;
	}

	private void adjustAdjacentCellStyle(
			Cell cell,
			Styles styles,
			RowPosition rowPosition,
			ColumnPosition columnPosition,
			Styles leftStyles,
			Styles aboveStyles) {

		if (styles == null) {
			return;
		}

		if (
				(leftStyles != null) &&
				(columnPosition != ColumnPosition.LEFT) && (columnPosition != ColumnPosition.SINGLE) &&
				!leftStyles.rightBorder.equals(styles.leftBorder)) {

			leftStyles.rightBorder = styles.leftBorder;

			Cell leftCell = cell.getRow().getCell(cell.getColumnIndex() - 1);

			leftCell.setCellStyle(composeCellStyle(leftCell, leftStyles));
		}

		if (
				(aboveStyles != null) &&
				(rowPosition != RowPosition.HEADER) && (rowPosition != RowPosition.TOP) &&
				!aboveStyles.bottomBorder.equals(styles.topBorder)) {

			aboveStyles.bottomBorder = styles.topBorder;

			Cell aboveCell = sheet.getRow(cell.getRowIndex() - 1).getCell(cell.getColumnIndex());

			aboveCell.setCellStyle(composeCellStyle(aboveCell, aboveStyles));
		}
	}

	private CellStyle composeCellStyle(Cell cell, Styles styles) {

		CellStyle originalStyle = cell.getCellStyle();

		short formatIndex = originalStyle.getIndex();

		StylesWithFormatting stylesWithFormatting = new StylesWithFormatting(styles, formatIndex);

		return stylesWithFormatting.getCellStyle(getOwner().getBook(), getOwner().stylesUsed, getOwner().fontsUsed, getOwner().colorsUsed);
	}

	private XlsxTargetBook getOwner() {
		return (XlsxTargetBook)owner;
	}

	@Override
	public void flush() throws IOException {

		if (0 < rowIndex) {
			// Write the bottom row but prepare for the possibility that this sheet may be appended.
			// If it is, the bottom row needs to be re-written without styling for RowPosition.BOTTOM.
			// But note that writeRow() will alter the contents of rowValues when inline styling is
			// used. So make a deep copy of rowValues, the restore it.

			ArrayList<Object> originalRowValues = new ArrayList<Object>(rowValues);

			writeRow(rowValues, rowIndex, RowPosition.BOTTOM, previousRowCellStyles);

			rowValues = originalRowValues;
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

class BigDollars {

	private final BigDecimal value;

	/**
	 * @param value is a value to parse.
	 * @return a BigDollars object if the value is in the format of a number
	 * with comma-separated thousands and two decimal places.  Otherwise,
	 * returns the original value.
	 */
	static Object parse(String value) {

		final Pattern pattern = Pattern.compile("\\A(-?\\d{1,3})(,\\d{3})+\\.\\d{2}\\z");

		Matcher matcher = pattern.matcher(value);
		if (matcher.find()) {
			return new BigDollars(new BigDecimal(value.replace(",", "")));
		}

		return value;
	}

	private BigDollars(BigDecimal value) {
		this.value = value;
	}

	public BigDecimal getValue() {
		return value;
	}
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

		if (styles.bottomBorder.style != null) {
			cellStyle.setBorderBottom(resolveBorderStyle(styles.bottomBorder));
		}
		if (styles.leftBorder.style != null) {
			cellStyle.setBorderLeft(resolveBorderStyle(styles.leftBorder));
		}
		if (styles.rightBorder.style != null) {
			cellStyle.setBorderRight(resolveBorderStyle(styles.rightBorder));
		}
		if (styles.topBorder.style != null) {
			cellStyle.setBorderTop(resolveBorderStyle(styles.topBorder));
		}

		if (styles.bottomBorder.color != null) {
			cellStyle.setBottomBorderColor(getColor(styles.bottomBorder.color, book, colorsUsed));
		}
		if (styles.leftBorder.color != null) {
			cellStyle.setLeftBorderColor(getColor(styles.leftBorder.color, book, colorsUsed));
		}
		if (styles.rightBorder.color != null) {
			cellStyle.setRightBorderColor(getColor(styles.rightBorder.color, book, colorsUsed));
		}
		if (styles.topBorder.color != null) {
			cellStyle.setTopBorderColor(getColor(styles.topBorder.color, book, colorsUsed));
		}

		if (styles.backgroundColor != null) {
			cellStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
			cellStyle.setFillForegroundColor(getColor(styles.backgroundColor, book, colorsUsed));
		}

		if (styles.textAlign != null) {
			cellStyle.setAlignment(styles.textAlign);
		}

		if (!styles.font.areDefault()) {
			cellStyle.setFont(getFont(styles.font, book, fontsUsed, colorsUsed));
		}

		stylesUsed.put(this, cellStyle);

		return cellStyle;
	}

	private static BorderStyle resolveBorderStyle(BorderStyles border) {

		BorderWidth borderWidth = (border.width != null) ? border.width : BorderWidth.MEDIUM;
		BorderEdge borderStyle = (border.style != null) ? border.style : BorderEdge.NONE;

		if (borderWidth == BorderWidth.ZERO) {
			return BorderStyle.NONE;
		}

		switch (borderStyle) {
		case NONE:
		case HIDDEN:
			return BorderStyle.NONE;
		default:
		case SOLID:
			switch (borderWidth) {
			case THIN: return BorderStyle.THIN;
			default:
			case MEDIUM: return BorderStyle.MEDIUM;
			case THICK: return BorderStyle.THICK;
			}
		case DOUBLE:
			return BorderStyle.DOUBLE;
		case DASHED:
			switch (borderWidth) {
			case THIN: return BorderStyle.DASHED;
			default:
			case MEDIUM:
			case THICK: return BorderStyle.MEDIUM_DASHED;
			}
		case DOTTED:
			return BorderStyle.DOTTED;
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
	 * the tag removed, converted if possible to a numeric type,
	 * and styles returns a non-null Styles object reflecting the styling.
	 * <p>
	 * Otherwise, value returns the original object and styles returns null.
	 */
	public static ValueStyles parse(TableTag tag, Object object) {

		Styles styles = null;
		if (object instanceof String) {
			Matcher matcher = patterns[tag.ordinal()].matcher((String)object);

			if (matcher.find()) {
				object = objectOf(matcher.group(2));
				styles = Styles.parse(matcher.group(1));
			}
		}
		return new ValueStyles(object, styles);
	}

	/**
	 * If a string can be converted to a numeric type, convert it
	 *
	 * @param value is the string to test for convertibility
	 * @return a Numeric object that is the conversion of the value
	 * if it can be converted, or the original value if it cannot
	 * be converted.  However, a value starting with a leading zero
	 * always returns the original value, unless the zero is the
	 * only character or is followed immediately by a decimal point.
	 */
	private static Object objectOf(String value) {

		final Pattern pattern = Pattern.compile("\\A(-?(?:0|[1-9][0-9]*))(\\.\\d+)?\\z");

		Matcher matcher = pattern.matcher(value);
		if (matcher.find()) {
			String wholePart = matcher.group(1);
			String decimalPart = matcher.group(2);

			if (decimalPart == null) {
				if (wholePart.length() <= 9) {
					return new Integer(wholePart);
				}
				else if (wholePart.length() <= 19) {
					return new Long(wholePart);
				}
				else {
					return new BigInteger(wholePart);
				}
			}
			else {
				return new BigDecimal(value);
			}
		}

		return value;
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

		RowStyles previousRowStyles = null;

		for (RowPosition rowPosition : RowPosition.values()) {

			RowStyles thisRowStyles = getStyles(rowPosition);
			Styles leftStyles = null;

			for (ColumnPosition columnPosition : ColumnPosition.values()) {

				Styles aboveStyles = (previousRowStyles != null) ? previousRowStyles.getStyles(columnPosition) : null;

				Styles positionStyles = super.resolve(null, null, rowPosition, columnPosition, leftStyles, aboveStyles);
				thisRowStyles.setStyles(columnPosition, positionStyles);

				if (!positionStyles.areDefault()) {
					areDefault = false;
				}

				leftStyles = positionStyles;
			}

			previousRowStyles = thisRowStyles;
		}
	}

	public Styles resolve(
			Styles cellStyles,
			Styles rowStyles,
			RowPosition rowPosition,
			ColumnPosition columnPosition,
			Styles leftStyles,
			Styles aboveStyles) {

		if ((cellStyles == null) && (rowStyles == null) && (leftStyles == null) && (aboveStyles == null)) {
			return getStyles(rowPosition).getStyles(columnPosition);
		}
		else {
			return super.resolve(cellStyles, rowStyles, rowPosition, columnPosition, leftStyles, aboveStyles);
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
			Styles cellStyles,
			Styles rowStyles,
			RowPosition rowPosition,
			ColumnPosition columnPosition,
			Styles leftStyles,
			Styles aboveStyles) {

		Styles result = new Styles();

		// Top borders

		switch (rowPosition) {
		case HEADER: {
			resolveTopBorder(result, null, cellStyles, headCellStyles, rowStyles, headStyles, tableStyles);
			break;
		}
		case NEXT: {
			resolveTopBorder(result, aboveStyles, cellStyles, bodyCellStyles, rowStyles, bodyStyles);
			break;
		}
		case TOP: {
			resolveTopBorder(result, null, cellStyles, bodyCellStyles, rowStyles, bodyStyles, tableStyles);
			break;
		}
		case MIDDLE:
		case BOTTOM: {
			resolveTopBorder(result, aboveStyles, cellStyles, bodyCellStyles, rowStyles);
			break;
		}
		}

		// Left and right borders, backgrounds, fonts

		switch (rowPosition) {
		case HEADER: {
			switch (columnPosition) {
			case SINGLE:
			case LEFT:{
				resolveLeftBorder(result, null, cellStyles, headCellStyles, rowStyles, headStyles, tableStyles);
				break;
			}
			case MIDDLE:
			case RIGHT: {
				resolveLeftBorder(result, leftStyles, cellStyles, headCellStyles);
				break;
			}
			}

			switch (columnPosition) {
			case LEFT:
			case MIDDLE: {
				resolveRightBorder(result, cellStyles, headCellStyles);
				break;
			}
			case SINGLE:
			case RIGHT: {
				resolveRightBorder(result, cellStyles, headCellStyles, rowStyles, headStyles, tableStyles);
				break;
			}
			}

			resolveNonBorders(result, cellStyles, headCellStyles, rowStyles, headStyles, tableStyles);
			break;
		}
		case NEXT:
		case TOP:
		case MIDDLE:
		case BOTTOM: {
			switch (columnPosition) {
			case SINGLE:
			case LEFT:{
				resolveLeftBorder(result, null, cellStyles, bodyCellStyles, rowStyles, bodyStyles, tableStyles);
				break;
			}
			case MIDDLE:
			case RIGHT: {
				resolveLeftBorder(result, leftStyles, cellStyles, bodyCellStyles);
				break;
			}
			}

			switch (columnPosition) {
			case LEFT:
			case MIDDLE: {
				resolveRightBorder(result, cellStyles, bodyCellStyles);
				break;
			}
			case SINGLE:
			case RIGHT: {
				resolveRightBorder(result, cellStyles, bodyCellStyles, rowStyles, bodyStyles, tableStyles);
				break;
			}
			}

			resolveNonBorders(result, cellStyles, bodyCellStyles, rowStyles, bodyStyles, tableStyles);
			break;
		}
		}

		// Bottom borders

		switch (rowPosition) {
		case HEADER: {
			resolveBottomBorder(result, cellStyles, headCellStyles, rowStyles, headStyles);
			break;
		}
		case NEXT:
		case TOP:
		case MIDDLE: {
			resolveBottomBorder(result, cellStyles, bodyCellStyles, rowStyles);
			break;
		}
		case BOTTOM: {
			resolveBottomBorder(result, cellStyles, bodyCellStyles, rowStyles, bodyStyles, tableStyles);
			break;
		}
		}

		return result;
	}

	// Regarding shared borders between cells, in the CSS model border properties (width, style, color)
	// are not inherited and in the collapsing border model they are not resolved property by property.
	// Instead each shared border takes on all the properties of one of the competing borders.
	// See http://www.w3.org/TR/CSS21/tables.html#border-conflict-resolution.
	// "The rule of thumb is that at each edge the most "eye catching" border style is chosen."

	// Also, while border properties are not inherited, the first two elements of each stylesArray
	// are always essentially an inline style followed by a style defined in a <style> element,
	// so we must resolve between those two and we can do that property by property.

	private void resolveTopBorder(Styles result, Styles aboveStyles, Styles... stylesArray) {

		ArrayList<BorderStyles> competingStyles = new ArrayList<BorderStyles>();

		if (aboveStyles != null) {
			competingStyles.add(aboveStyles.bottomBorder);
		}

		competingStyles.add(new BorderStyles(
				resolve(Styles.topBorderWidthGetter, stylesArray[0], stylesArray[1]),
				resolve(Styles.topBorderStyleGetter, stylesArray[0], stylesArray[1]),
				resolve(Styles.topBorderColorGetter, stylesArray[0], stylesArray[1])
				));

		for (int i = 2; i < stylesArray.length; ++i) {
			if (stylesArray[i] != null) {
				competingStyles.add(stylesArray[i].topBorder);
			}
		}

		BorderStyles mostEyeCatchingStyles = BorderStyles.mostEyeCatching(competingStyles);

		result.topBorder = mostEyeCatchingStyles;
	}

	private void resolveLeftBorder(Styles result, Styles leftStyles, Styles... stylesArray) {

		ArrayList<BorderStyles> competingStyles = new ArrayList<BorderStyles>();

		if (leftStyles != null) {
			competingStyles.add(leftStyles.rightBorder);
		}

		competingStyles.add(new BorderStyles(
				resolve(Styles.leftBorderWidthGetter, stylesArray[0], stylesArray[1]),
				resolve(Styles.leftBorderStyleGetter, stylesArray[0], stylesArray[1]),
				resolve(Styles.leftBorderColorGetter, stylesArray[0], stylesArray[1])
				));

		for (int i = 2; i < stylesArray.length; ++i) {
			if (stylesArray[i] != null) {
				competingStyles.add(stylesArray[i].leftBorder);
			}
		}

		BorderStyles mostEyeCatchingStyles = BorderStyles.mostEyeCatching(competingStyles);

		result.leftBorder = mostEyeCatchingStyles;
	}

	private void resolveRightBorder(Styles result, Styles... stylesArray) {

		ArrayList<BorderStyles> competingStyles = new ArrayList<BorderStyles>();

		competingStyles.add(new BorderStyles(
				resolve(Styles.rightBorderWidthGetter, stylesArray[0], stylesArray[1]),
				resolve(Styles.rightBorderStyleGetter, stylesArray[0], stylesArray[1]),
				resolve(Styles.rightBorderColorGetter, stylesArray[0], stylesArray[1])
				));

		for (int i = 2; i < stylesArray.length; ++i) {
			if (stylesArray[i] != null) {
				competingStyles.add(stylesArray[i].rightBorder);
			}
		}

		BorderStyles mostEyeCatchingStyles = BorderStyles.mostEyeCatching(competingStyles);

		result.rightBorder = mostEyeCatchingStyles;
	}

	private void resolveBottomBorder(Styles result, Styles... stylesArray) {

		ArrayList<BorderStyles> competingStyles = new ArrayList<BorderStyles>();

		competingStyles.add(new BorderStyles(
				resolve(Styles.bottomBorderWidthGetter, stylesArray[0], stylesArray[1]),
				resolve(Styles.bottomBorderStyleGetter, stylesArray[0], stylesArray[1]),
				resolve(Styles.bottomBorderColorGetter, stylesArray[0], stylesArray[1])
				));

		for (int i = 2; i < stylesArray.length; ++i) {
			if (stylesArray[i] != null) {
				competingStyles.add(stylesArray[i].bottomBorder);
			}
		}

		BorderStyles mostEyeCatchingStyles = BorderStyles.mostEyeCatching(competingStyles);

		result.bottomBorder = mostEyeCatchingStyles;
	}

	private void resolveNonBorders(Styles result, Styles... stylesArray) {

		result.backgroundColor = resolve(Styles.backgroundColorGetter, stylesArray);
		result.textAlign = resolve(Styles.textAlignGetter, stylesArray);

		result.font.color = resolve(FontStyles.colorGetter, stylesArray);
		result.font.fontStyle = resolve(FontStyles.fontStyleGetter, stylesArray);
		result.font.fontWeight = resolve(FontStyles.fontWeightGetter, stylesArray);

		// In the CSS model, text_decoration-line and text-decoration-style are not inherited.
		// However, the first two arguments to this function are always essentially an inline style
		// followed by a style defined in a <style> element, so we must resolve between those two.

		result.font.textDecorationLine = resolve(FontStyles.textDecorationLineGetter, stylesArray[0], stylesArray[1]);
		result.font.textDecorationStyle = resolve(FontStyles.textDecorationStyleGetter, stylesArray[0], stylesArray[1]);
	}

	@SafeVarargs
	private static <Type> Type resolve(AnyStyles.StylesGetter<Type> getter, Styles... stylesArray) {
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

class BorderStyles extends AnyStyles {

	public BorderWidth width = null;
	public BorderEdge style = null;
	public Integer color = null;

	public BorderStyles() {}

	public BorderStyles(
			BorderWidth width,
			BorderEdge style,
			Integer color
			) {
		this.width = width;
		this.style = style;
		this.color = color;
	}

	@Override
	public int hashCode() {
		return
				// 30 bits total
				((width != null) ? width.ordinal() : BorderWidth.values().length) ^
				((style != null) ? style.ordinal() : BorderEdge.values().length) << 2 ^
				((color != null) ? color.intValue() : 0x1000000) << 6;
	}

	@Override
	public boolean equals(Object obj) {

		if (!(obj instanceof BorderStyles)) { return false; }

		BorderStyles other = (BorderStyles)obj;
		return
				areSame(width, other.width) &&
				areSame(style, other.style) &&
				areSame(color, other.color);
	}

	public boolean areDefault() {
		return
				width == null &&
				style == null &&
				color == null;
	}

	public enum BorderWidth { ZERO, MEDIUM, THIN, THICK };

	public static boolean isBorderWidth(String borderProperty) {
		return getBorderWidth(borderProperty) != null;
	}

	public static BorderWidth getBorderWidth(String borderProperty) {
		switch (borderProperty) {
		case "thin": return BorderWidth.THIN;
		case "medium": return BorderWidth.MEDIUM;
		case "thick": return BorderWidth.THICK;
		default: {
			final Pattern pattern = Pattern.compile("\\A(\\d{1,4})px\\z");

			Matcher matcher = pattern.matcher(borderProperty);
			if (matcher.find()) {
				int px = Integer.valueOf(matcher.group(1));
				if (px == 0) {
					return BorderWidth.ZERO;
				}
				else if (px == 1 || px == 2) {
					return BorderWidth.THIN;
				}
				else if (px == 3 || px == 4) {
					return BorderWidth.MEDIUM;
				}
				else {
					return BorderWidth.THICK;
				}
			}
			return null;
		}
		}
	}

	public enum BorderEdge { NONE, HIDDEN, DOTTED, DASHED, SOLID, DOUBLE, GROOVE, RIDGE, INSET, OUTSET };

	public static boolean isBorderStyle(String borderProperty) {
		return getBorderStyle(borderProperty) != null;
	}

	public static BorderEdge getBorderStyle(String borderProperty) {
		switch (borderProperty) {
		case "none": return BorderEdge.NONE;
		case "hidden": return BorderEdge.HIDDEN;
		case "dotted": return BorderEdge.DOTTED;
		case "dashed": return BorderEdge.DASHED;
		case "solid": return BorderEdge.SOLID;
		case "double": return BorderEdge.DOUBLE;
		case "groove": return BorderEdge.GROOVE;
		case "ridge": return BorderEdge.RIDGE;
		case "inset": return BorderEdge.INSET;
		case "outset": return BorderEdge.OUTSET;
		default: return null;
		}
	}

	static private Map<BorderWidth, Integer> widthPriority;
	static {
		widthPriority = new HashMap<BorderWidth, Integer>();
		widthPriority.put(BorderWidth.THICK, 0);
		widthPriority.put(BorderWidth.MEDIUM, 1);
		widthPriority.put(null, 1);							// Default width = medium
		widthPriority.put(BorderWidth.THIN, 2);
		widthPriority.put(BorderWidth.ZERO, 3);

		widthScale = widthPriority.values().stream().max(Integer::compare).get() + 1;
	}
	static private int widthScale;

	private static int widthRank(BorderStyles styles) {
		return ((styles.style == null) || (styles.style == BorderEdge.NONE)) ? widthScale : widthPriority.get(styles.width);
	}

	static private Map<BorderEdge, Integer> stylePriority;
	static {
		stylePriority = new HashMap<BorderEdge, Integer>();
		stylePriority.put(BorderEdge.HIDDEN, 0);
		stylePriority.put(BorderEdge.DOUBLE, 1);
		stylePriority.put(BorderEdge.SOLID, 2);
		stylePriority.put(BorderEdge.DASHED, 3);
		stylePriority.put(BorderEdge.DOTTED, 4);
		stylePriority.put(BorderEdge.RIDGE, 5);
		stylePriority.put(BorderEdge.OUTSET, 6);
		stylePriority.put(BorderEdge.GROOVE, 7);
		stylePriority.put(BorderEdge.INSET, 8);
		stylePriority.put(BorderEdge.NONE, 9);
		stylePriority.put(null, 9);							// Default style = none

		styleScale = stylePriority.values().stream().max(Integer::compare).get() + 1;
	}
	static private int styleScale;

	private static int styleRank(BorderStyles styles) { return stylePriority.get(styles.style); }

	/**
	 * Return the relative ranking of a border styling.  A lower rank takes precedence over a higher rank.
	 *
	 * @param styles is the border styling.
	 * @param order is the precedence of this styling over others, all other styling considerations being equal.
	 * @param length is the total number of stylings that will be ranked.
	 * @return
	 */
	static private int rank(BorderStyles styles, int order, int length) {
		/*
		 * From http://www.w3.org/TR/CSS21/tables.html#border-conflict-resolution:
		 *
		 * 1. Borders with the 'border-style' of 'hidden' take precedence over all other conflicting borders.
		 *    Any border with this value suppresses all borders at this location.
		 *
		 * 2. Borders with a style of 'none' have the lowest priority. Only if the border properties of all the elements
		 *    meeting at this edge are 'none' will the border be omitted (but note that 'none' is the default value for the border style.)
		 *
		 * 3. If none of the styles are 'hidden' and at least one of them is not 'none', then narrow borders are discarded in favor of wider ones.
		 *
		 *    If several have the same 'border-width' then styles are preferred in this order:
		 *    'double', 'solid', 'dashed', 'dotted', 'ridge', 'outset', 'groove', and the lowest: 'inset'.
		 *
		 * 4. If border styles differ only in color, then a style set on a cell wins over one on a row, which wins over a row group,
		 *    column, column group and, lastly, table. When two elements of the same type conflict, then the one further to the left
		 *    (if the table's 'direction' is 'ltr'; right, if it is 'rtl') and further to the top wins.
		 */

		if (styles.style == BorderEdge.HIDDEN) {
			return order;
		}

		return (widthRank(styles) * styleScale + styleRank(styles)) * length + order;
	}

	static public BorderStyles mostEyeCatching(ArrayList<BorderStyles> stylesList)  {

		Comparator<Integer> stylesComparator = Comparator.comparing(i -> rank(stylesList.get(i), i, stylesList.size()));

		int indexOfMostEyeCatching = IntStream.range(0, stylesList.size()).boxed().min(stylesComparator).get();

		return stylesList.get(indexOfMostEyeCatching);
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

	public BorderStyles bottomBorder = new BorderStyles();
	public BorderStyles leftBorder = new BorderStyles();
	public BorderStyles rightBorder = new BorderStyles();
	public BorderStyles topBorder = new BorderStyles();

	public Integer backgroundColor = null;
	public HorizontalAlignment textAlign = null;

	public FontStyles font = new FontStyles();

	public static final StylesGetter<BorderWidth> bottomBorderWidthGetter = new StylesGetter<BorderWidth>() {public BorderWidth get(Styles styles) {return styles.bottomBorder.width;}};
	public static final StylesGetter<BorderWidth> leftBorderWidthGetter = new StylesGetter<BorderWidth>() {public BorderWidth get(Styles styles) {return styles.leftBorder.width;}};
	public static final StylesGetter<BorderWidth> rightBorderWidthGetter = new StylesGetter<BorderWidth>() {public BorderWidth get(Styles styles) {return styles.rightBorder.width;}};
	public static final StylesGetter<BorderWidth> topBorderWidthGetter = new StylesGetter<BorderWidth>() {public BorderWidth get(Styles styles) {return styles.topBorder.width;}};

	public static final StylesGetter<BorderEdge> bottomBorderStyleGetter = new StylesGetter<BorderEdge>() {public BorderEdge get(Styles styles) {return styles.bottomBorder.style;}};
	public static final StylesGetter<BorderEdge> leftBorderStyleGetter = new StylesGetter<BorderEdge>() {public BorderEdge get(Styles styles) {return styles.leftBorder.style;}};
	public static final StylesGetter<BorderEdge> rightBorderStyleGetter = new StylesGetter<BorderEdge>() {public BorderEdge get(Styles styles) {return styles.rightBorder.style;}};
	public static final StylesGetter<BorderEdge> topBorderStyleGetter = new StylesGetter<BorderEdge>() {public BorderEdge get(Styles styles) {return styles.topBorder.style;}};

	public static final StylesGetter<Integer> bottomBorderColorGetter = new StylesGetter<Integer>() {public Integer get(Styles styles) {return styles.bottomBorder.color;}};
	public static final StylesGetter<Integer> leftBorderColorGetter = new StylesGetter<Integer>() {public Integer get(Styles styles) {return styles.leftBorder.color;}};
	public static final StylesGetter<Integer> rightBorderColorGetter = new StylesGetter<Integer>() {public Integer get(Styles styles) {return styles.rightBorder.color;}};
	public static final StylesGetter<Integer> topBorderColorGetter = new StylesGetter<Integer>() {public Integer get(Styles styles) {return styles.topBorder.color;}};

	public static final StylesGetter<Integer> backgroundColorGetter = new StylesGetter<Integer>() {public Integer get(Styles styles) {return styles.backgroundColor;}};
	public static final StylesGetter<HorizontalAlignment> textAlignGetter = new StylesGetter<HorizontalAlignment>() {public HorizontalAlignment get(Styles styles) {return styles.textAlign;}};

	@Override
	public int hashCode() {
		return
				((bottomBorder.width != null) ? bottomBorder.width.ordinal() : BorderWidth.values().length) ^
				((leftBorder.width != null) ? leftBorder.width.ordinal() : BorderWidth.values().length) << 2 ^
				((rightBorder.width != null) ? rightBorder.width.ordinal() : BorderWidth.values().length) << 4 ^
				((topBorder.width != null) ? topBorder.width.ordinal() : BorderWidth.values().length) << 6 ^

				((bottomBorder.style != null) ? bottomBorder.style.ordinal() : BorderEdge.values().length) << 8 ^
				((leftBorder.style != null) ? leftBorder.style.ordinal() : BorderEdge.values().length) << 12 ^
				((rightBorder.style != null) ? rightBorder.style.ordinal() : BorderEdge.values().length) << 16 ^
				((topBorder.style != null) ? topBorder.style.ordinal() : BorderEdge.values().length) << 20 ^

				((bottomBorder.color != null) ? bottomBorder.color.intValue() : 0x1000000) ^
				((leftBorder.color != null) ? leftBorder.color.intValue() : 0x1000000) << 1 ^
				((rightBorder.color != null) ? rightBorder.color.intValue() : 0x1000000) << 2 ^
				((topBorder.color != null) ? topBorder.color.intValue() : 0x1000000) << 3 ^

				((backgroundColor != null) ? backgroundColor.intValue() : 0x1000000) << 5 ^
				((textAlign != null) ? textAlign.ordinal() : HorizontalAlignment.values().length) << 28 ^

				font.hashCode() << 8;
	}

	@Override
	public boolean equals(Object obj) {

		if (!(obj instanceof Styles)) { return false; }

		Styles other = (Styles)obj;
		return
				bottomBorder.equals(other.bottomBorder) &&
				leftBorder.equals(other.leftBorder) &&
				rightBorder.equals(other.rightBorder) &&
				topBorder.equals(other.topBorder) &&

				areSame(backgroundColor, other.backgroundColor) &&
				areSame(textAlign, other.textAlign) &&

				font.equals(other.font);
	}

	public boolean areDefault() {
		return
				bottomBorder.areDefault() &&
				leftBorder.areDefault() &&
				rightBorder.areDefault() &&
				topBorder.areDefault() &&

				backgroundColor == null &&
				textAlign == null &&

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
							if (BorderStyles.isBorderWidth(borderProperty)) {
								result.bottomBorder.width = BorderStyles.getBorderWidth(borderProperty);
								result.leftBorder.width = BorderStyles.getBorderWidth(borderProperty);
								result.rightBorder.width = BorderStyles.getBorderWidth(borderProperty);
								result.topBorder.width = BorderStyles.getBorderWidth(borderProperty);
							}
							else if (BorderStyles.isBorderStyle(borderProperty)) {
								result.bottomBorder.style = BorderStyles.getBorderStyle(borderProperty);
								result.leftBorder.style = BorderStyles.getBorderStyle(borderProperty);
								result.rightBorder.style = BorderStyles.getBorderStyle(borderProperty);
								result.topBorder.style = BorderStyles.getBorderStyle(borderProperty);
							}
							else if (isColor(borderProperty)) {
								result.bottomBorder.color = getColor(borderProperty);
								result.leftBorder.color = getColor(borderProperty);
								result.rightBorder.color = getColor(borderProperty);
								result.topBorder.color = getColor(borderProperty);
							}
						}
					}
					else if (keyword.equals("border-bottom")) {
						String[] borderBottomProperties = value.split(" +");
						for (String borderProperty : borderBottomProperties) {
							if (BorderStyles.isBorderWidth(borderProperty)) {
								result.bottomBorder.width = BorderStyles.getBorderWidth(borderProperty);
							}
							else if (BorderStyles.isBorderStyle(borderProperty)) {
								result.bottomBorder.style = BorderStyles.getBorderStyle(borderProperty);
							}
							else if (isColor(borderProperty)) {
								result.bottomBorder.color = getColor(borderProperty);
							}
						}
					}
					else if (keyword.equals("border-bottom-color")) {
						result.bottomBorder.color = getColor(value);
					}
					else if (keyword.equals("border-bottom-style")) {
						result.bottomBorder.style = BorderStyles.getBorderStyle(value);
					}
					else if (keyword.equals("border-bottom-width")) {
						result.bottomBorder.width = BorderStyles.getBorderWidth(value);
					}
					else if (keyword.equals("border-color")) {
						result.bottomBorder.color = getColor(value);
						result.leftBorder.color = getColor(value);
						result.rightBorder.color = getColor(value);
						result.topBorder.color = getColor(value);
					}
					else if (keyword.equals("border-left")) {
						String[] borderLeftProperties = value.split(" +");
						for (String borderProperty : borderLeftProperties) {
							if (BorderStyles.isBorderWidth(borderProperty)) {
								result.leftBorder.width = BorderStyles.getBorderWidth(borderProperty);
							}
							else if (BorderStyles.isBorderStyle(borderProperty)) {
								result.leftBorder.style = BorderStyles.getBorderStyle(borderProperty);
							}
							else if (isColor(borderProperty)) {
								result.leftBorder.color = getColor(borderProperty);
							}
						}
					}
					else if (keyword.equals("border-left-color")) {
						result.leftBorder.color = getColor(value);
					}
					else if (keyword.equals("border-left-style")) {
						result.leftBorder.style = BorderStyles.getBorderStyle(value);
					}
					else if (keyword.equals("border-left-width")) {
						result.leftBorder.width = BorderStyles.getBorderWidth(value);
					}
					else if (keyword.equals("border-right")) {
						String[] borderRightProperties = value.split(" +");
						for (String borderProperty : borderRightProperties) {
							if (BorderStyles.isBorderWidth(borderProperty)) {
								result.rightBorder.width = BorderStyles.getBorderWidth(borderProperty);
							}
							else if (BorderStyles.isBorderStyle(borderProperty)) {
								result.rightBorder.style = BorderStyles.getBorderStyle(borderProperty);
							}
							else if (isColor(borderProperty)) {
								result.rightBorder.color = getColor(borderProperty);
							}
						}
					}
					else if (keyword.equals("border-right-color")) {
						result.rightBorder.color = getColor(value);
					}
					else if (keyword.equals("border-right-style")) {
						result.rightBorder.style = BorderStyles.getBorderStyle(value);
					}
					else if (keyword.equals("border-right-width")) {
						result.rightBorder.width = BorderStyles.getBorderWidth(value);
					}
					else if (keyword.equals("border-style")) {
						result.bottomBorder.style = BorderStyles.getBorderStyle(value);
						result.leftBorder.style = BorderStyles.getBorderStyle(value);
						result.rightBorder.style = BorderStyles.getBorderStyle(value);
						result.topBorder.style = BorderStyles.getBorderStyle(value);
					}
					else if (keyword.equals("border-top")) {
						String[] borderTopProperties = value.split(" ");
						for (String borderProperty : borderTopProperties) {
							if (BorderStyles.isBorderWidth(borderProperty)) {
								result.topBorder.width = BorderStyles.getBorderWidth(borderProperty);
							}
							else if (BorderStyles.isBorderStyle(borderProperty)) {
								result.topBorder.style = BorderStyles.getBorderStyle(borderProperty);
							}
							else if (isColor(borderProperty)) {
								result.topBorder.color = getColor(borderProperty);
							}
						}
					}
					else if (keyword.equals("border-top-color")) {
						result.topBorder.color = getColor(value);
					}
					else if (keyword.equals("border-top-style")) {
						result.topBorder.style = BorderStyles.getBorderStyle(value);
					}
					else if (keyword.equals("border-top-width")) {
						result.topBorder.width = BorderStyles.getBorderWidth(value);
					}
					else if (keyword.equals("border-width")) {
						result.bottomBorder.width = BorderStyles.getBorderWidth(value);
						result.leftBorder.width = BorderStyles.getBorderWidth(value);
						result.rightBorder.width = BorderStyles.getBorderWidth(value);
						result.topBorder.width = BorderStyles.getBorderWidth(value);
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
					else if (keyword.equals("text-align")) {
						result.textAlign = Styles.getTextAlign(value);
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
		Integer result = cssColor.get(property);
		if (result == null) {
			final Pattern pattern = Pattern.compile("\\#(\\p{XDigit}{6})\\z");
			Matcher matcher = pattern.matcher(property);
			if (matcher.find()) {
				result = Integer.valueOf(matcher.group(1), 16);
			}
		}
		return result;
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

	public static HorizontalAlignment getTextAlign(String textProperty) {
		switch (textProperty) {
		case "center": return HorizontalAlignment.CENTER;
		case "left": return HorizontalAlignment.LEFT;
		case "right": return HorizontalAlignment.RIGHT;
		default: return null;
		}
	}
}
