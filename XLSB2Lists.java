
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler;
import org.apache.poi.xssf.usermodel.XSSFComment;

import java.util.ArrayList;
import java.util.List;

public class XLSB2Lists implements XSSFSheetXMLHandler.SheetContentsHandler {

	private final List sheetAsList = new ArrayList<>();
	private List rowAsList;
	private int counter_ColumnIndex;

	@Override
	public void startRow(int rowNum) {
		rowAsList = new ArrayList<>();
		counter_ColumnIndex = 1;
	}

	@Override
	public void endRow(int rowNum) {

		sheetAsList.add(rowAsList);

	}

	@Override
	public void cell(String cellReference, String formattedValue, XSSFComment comment) {

		int columnIndex = 0;
		for (int i = 0; i < cellReference.length(); i++) {
			if (Character.isAlphabetic(cellReference.charAt(i))) {
				columnIndex *= 26;
				columnIndex += cellReference.charAt(i) - 'A' + 1;
			} else {
				break;
			}
		}
		if (counter_ColumnIndex == columnIndex) {
			rowAsList.add(formattedValue);
		} else {
			while (counter_ColumnIndex != columnIndex) {
				rowAsList.add(null);
				counter_ColumnIndex++;
			}
			rowAsList.add(formattedValue);
		}
		counter_ColumnIndex++;
	}

	@Override
	public void headerFooter(String text, boolean isHeader, String tagName) {

	}

	public List getSheetContentAsList() {
		return sheetAsList;
	}

}
