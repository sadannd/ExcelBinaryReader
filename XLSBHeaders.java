package com.automationedge.ps.workflow.steps.excelinput.binary;

import java.util.ArrayList;
import java.util.List;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler;
import org.apache.poi.xssf.usermodel.XSSFComment;

public class XLSBHeaders implements XSSFSheetXMLHandler.SheetContentsHandler {

	private final List sheetAsList = new ArrayList<>();
	private List rowAsList = new ArrayList<>();
	int startrow = 0;

	@Override
	public void startRow(int rowNum) {
		startrow++;
		
	}

	@Override
	public void endRow(int rowNum) {
		if (startrow == 1) {
			sheetAsList.add(rowNum, rowAsList);
		}

	}

	@Override
	public void cell(String cellReference, String formattedValue, XSSFComment comment) {
		if (startrow == 1) {
			rowAsList.add(formattedValue);
		}
	}

	@Override
	public void headerFooter(String text, boolean isHeader, String tagName) {

	}

	public List getSheetHeadersAsList() {
		if (!sheetAsList.isEmpty()) {
			return (List) sheetAsList.get(0);
		} else {
			return (List) sheetAsList;

		}
	}

}