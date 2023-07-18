import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler;
import org.apache.poi.xssf.usermodel.XSSFComment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class XLSB2ListsBatch implements XSSFSheetXMLHandler.SheetContentsHandler {
    private static final Logger logger = LoggerFactory.getLogger(XLSB2ListsBatch.class);
    private final List sheetAsList = new ArrayList<>();
    private List rowAsList;
    private int counter_ColumnIndex;
    public ExcelInput excelInput;
    public int headerRowCount;
    public int[] start_row;
    public int sheetName_index;
    public Object[] row;
    public int[] start_col;
    public String[] sheetName;
    public int sheetRowCount;
    public FileObject file;
    public int nrOfFiles;
    public long limit;
    private int batchSize;


    public XLSB2ListsBatch(int headerRowCount, int[] start_row, int sheetName_index, Object[] row, int[] start_col, String[] sheetName, int sheetRowCount, FileObject file, int nrOfFiles, long limit, ExcelInput excelInput, int batchSize) {
        this.excelInput = excelInput;
        this.headerRowCount = headerRowCount;
        this.start_row = start_row;
        this.start_col = start_col;
        this.sheetName_index = sheetName_index;
        this.nrOfFiles = nrOfFiles;
        this.row = row;
        this.sheetName = sheetName;
        this.sheetRowCount = sheetRowCount;
        this.file = file;
        this.limit = limit;
        this.batchSize = batchSize;
    }

    @Override
    public void startRow(int rowNum) {
        rowAsList = new ArrayList<>();
        counter_ColumnIndex = 1;
    }

    @Override
    public void endRow(int rowNum) {
        /*
        * Clearing sheetAsList after each batch to release the memory used.
        * */
        sheetAsList.add(rowAsList);
        if (rowNum % batchSize == 0 && rowNum != 0) {
            try {
                excelInput.XLSBBatchProcessing(headerRowCount, start_row, sheetName_index, row, start_col, sheetName, sheetRowCount, file, nrOfFiles, limit);
                sheetAsList.clear();
            } catch (ProcessStudioValueException | FileSystemException | ProcessStudioStepException | java.text.ParseException e) {
                logger.info(e.toString());
            }
        }

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
