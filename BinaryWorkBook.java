package com.automationedge.ps.workflow.steps.excelinput.binary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.xssf.eventusermodel.XSSFBReader;
import org.xml.sax.SAXException;

public class BinaryWorkBook {

	List<String> sheetNames = new ArrayList<String>();

	public void populateSheets(String filename) throws IOException, SAXException, OpenXML4JException {

		OPCPackage pkg = OPCPackage.open(filename);
		XSSFBReader r = new XSSFBReader(pkg);
		XSSFBReader.SheetIterator it = (XSSFBReader.SheetIterator) r.getSheetsData();
		while (it.hasNext()) {
			it.next();
			sheetNames.add(it.getSheetName());

		}

	}

	public List getSheetNames() {
		return sheetNames;
	}

}
