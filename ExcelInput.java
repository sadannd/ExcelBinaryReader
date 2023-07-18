/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/


import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;


import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.xssf.binary.XSSFBSharedStringsTable;
import org.apache.poi.xssf.binary.XSSFBSheetHandler;
import org.apache.poi.xssf.binary.XSSFBStylesTable;
import org.apache.poi.xssf.eventusermodel.XSSFBReader;

import org.xml.sax.SAXException;

/**
 * This class reads data from one or more Microsoft Excel files.
 *
 */
public class ExcelInput extends BaseStep implements StepInterface {
  private static Class<?> PKG = ExcelInputMeta.class; // for i18n purposes, needed by Translator2!!

  private ExcelInputMeta meta;

  private ExcelInputData data;

  private int startRowCounter_XLSB;

  private boolean isIncludeHeader_XLSB;
 
  public ExcelInput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, WorkflowMeta workflowMeta,
    Workflow workflow ) {
    super( stepMeta, stepDataInterface, copyNr, workflowMeta, workflow );
  }

  /**
   * Build an empty row based on the meta-data...
   *
   * @return
   */

  
  private Object[] fillRow( int startcolumn, ExcelInputRow excelInputRow ) throws ProcessStudioException {

    Object[] r = new Object[data.outputRowMeta.size()];

    // Keep track whether or not we handled an error for this line yet.
    boolean errorHandled = false;
    

    // Set values in the row...
    KCell cell = null;

    for ( int i = startcolumn; i < excelInputRow.cells.length && i - startcolumn < meta.getField().length; i++ ) {
      cell = excelInputRow.cells[i];
      int rowcolumn = i - startcolumn;

      if ( cell == null ) {
        r[rowcolumn] = null;
        continue;
      }

      ValueMetaInterface targetMeta = data.outputRowMeta.getValueMeta( rowcolumn );
      ValueMetaInterface sourceMeta = null;
 

      try {
        checkType( cell, targetMeta );
      } catch ( ProcessStudioException ex ) {
        if ( !meta.isErrorIgnored() ) {
          ex = new ProcessStudioCellValueException( ex, this.data.sheetnr, this.data.rownr, i, "" );
          throw ex;
        }
        if ( log.isBasic() ) {
          logBasic( BaseMessages.getString( PKG, "ExcelInput.Log.WarningProcessingExcelFile", "" + targetMeta, ""
            + data.filename, ex.getMessage() ) );
        }

        if ( !errorHandled ) {
          data.errorHandler.handleLineError( excelInputRow.rownr, excelInputRow.sheetName );
          errorHandled = true;
        }

        if ( meta.isErrorLineSkipped() ) {
          return null;
        }
      }

      KCellType cellType = cell.getType();
      
      if ( KCellType.BOOLEAN == cellType || KCellType.BOOLEAN_FORMULA == cellType ) {
        r[rowcolumn] = cell.getValue();
        sourceMeta = data.valueMetaBoolean;
      } else {
        if ( KCellType.DATE.equals( cellType ) || KCellType.DATE_FORMULA.equals( cellType ) ) {
          Date date = (Date) cell.getValue();
          long time = date.getTime();
          int offset = TimeZone.getDefault().getOffset( time );
          r[rowcolumn] = new Date( time - offset );
          sourceMeta = data.valueMetaDate;
        } else {
          if ( KCellType.LABEL == cellType || KCellType.STRING_FORMULA == cellType ) {
            String string = (String) cell.getValue();
            switch ( meta.getField()[rowcolumn].getTrimType() ) {
              case ExcelInputMeta.TYPE_TRIM_LEFT:
                string = Const.ltrim( string );
                break;
              case ExcelInputMeta.TYPE_TRIM_RIGHT:
                string = Const.rtrim( string );
                break;
              case ExcelInputMeta.TYPE_TRIM_BOTH:
                string = Const.trim( string );
                break;
              default:
                break;
            }
            r[rowcolumn] = string;
            sourceMeta = data.valueMetaString;
            if(StringUtils.isBlank(string) &&  (targetMeta.getType())!= ValueMetaInterface.TYPE_STRING )
            {            		
            		r[rowcolumn] = null;
            }
            
           //try code
            
          } else {
            if ( KCellType.NUMBER == cellType || KCellType.NUMBER_FORMULA == cellType ) {
              r[rowcolumn] = cell.getValue();
              sourceMeta = data.valueMetaNumber;
            } else {
              if ( log.isDetailed() ) {
                KCellType ct = cell.getType();
                logDetailed( BaseMessages.getString( PKG, "ExcelInput.Log.UnknownType", ( ( ct != null ) ? ct
                  .toString() : "null" ), cell.getContents() ) );
              }
              r[rowcolumn] = null;
            }
          }
        }
      }

      ExcelInputField field = meta.getField()[rowcolumn];

      // Change to the appropriate type if needed...
      //
      try {
        // Null stays null folks.
        //
        if ( sourceMeta != null && sourceMeta.getType() != targetMeta.getType() && r[rowcolumn] != null ) {
               	
          ValueMetaInterface sourceMetaCopy = sourceMeta.clone();
          sourceMetaCopy.setConversionMask( field.getFormat() );
          sourceMetaCopy.setGroupingSymbol( field.getGroupSymbol() );
          sourceMetaCopy.setDecimalSymbol( field.getDecimalSymbol() );
          sourceMetaCopy.setCurrencySymbol( field.getCurrencySymbol() );

          switch ( targetMeta.getType() ) {
          
          // Use case: we find a numeric value: convert it using the supplied format to the desired data type...
          //
            case ValueMetaInterface.TYPE_NUMBER:
            	
            case ValueMetaInterface.TYPE_INTEGER:
            	
              switch ( field.getType() ) {
                case ValueMetaInterface.TYPE_DATE:
                  // number to string conversion (20070522.00 --> "20070522")
                  //
                  ValueMetaInterface valueMetaNumber = new ValueMetaNumber( "num" );
                  valueMetaNumber.setConversionMask( "#" );
                  Object string = sourceMetaCopy.convertData( valueMetaNumber, r[rowcolumn] );

                  // String to date with mask...
                  //
                  r[rowcolumn] = targetMeta.convertData( sourceMetaCopy, string );
                  break;
                default:
                  r[rowcolumn] = targetMeta.convertData( sourceMetaCopy, r[rowcolumn] );
                  break;
              }
              break;
            // Use case: we find a date: convert it using the supplied format to String...
            //
            default:
              r[rowcolumn] = targetMeta.convertData( sourceMetaCopy, r[rowcolumn] );
          }
        }
      } catch ( ProcessStudioException ex ) {
        if ( !meta.isErrorIgnored() ) {
          ex = new ProcessStudioCellValueException( ex, this.data.sheetnr, cell.getRow(), i, field.getName() );
          throw ex;
        }
        if ( log.isBasic() ) {
          logBasic( BaseMessages.getString( PKG, "ExcelInput.Log.WarningProcessingExcelFile", "" + targetMeta, ""
            + data.filename, ex.toString() ) );
        }
        if ( !errorHandled ) {
          // check if we didn't log an error already for this one.
          data.errorHandler.handleLineError( excelInputRow.rownr, excelInputRow.sheetName );
          errorHandled = true;
        }

        if ( meta.isErrorLineSkipped() ) {
          return null;
        } else {
          r[rowcolumn] = null;
        }
      }
    }

    int rowIndex = meta.getField().length;

    // Do we need to include the filename?
    if ( !StringUtils.isBlank( meta.getFileField() ) ) {
      r[rowIndex] = data.filename;
      rowIndex++;
    }

    // Do we need to include the sheetname?
    if ( !StringUtils.isBlank( meta.getSheetField() ) ) {
      r[rowIndex] = excelInputRow.sheetName;
      rowIndex++;
    }

    // Do we need to include the sheet rownumber?
    if ( !StringUtils.isBlank( meta.getSheetRowNumberField() ) ) {
      r[rowIndex] = new Long( data.rownr );
      rowIndex++;
    }

    // Do we need to include the rownumber?
    if ( !StringUtils.isBlank( meta.getRowNumberField() ) ) {
      r[rowIndex] = new Long( getLinesWritten() + 1 );
      rowIndex++;
    }
    // Possibly add short filename...
    if ( !StringUtils.isBlank( meta.getShortFileNameField() ) ) {
      r[rowIndex] = data.shortFilename;
      rowIndex++;
    }
    // Add Extension
    if ( !StringUtils.isBlank( meta.getExtensionField() ) ) {
      r[rowIndex] = data.extension;
      rowIndex++;
    }
    // add path
    if ( !StringUtils.isBlank( meta.getPathField() ) ) {
      r[rowIndex] = data.path;
      rowIndex++;
    }
    // Add Size
    if ( !StringUtils.isBlank( meta.getSizeField() ) ) {
      r[rowIndex] = new Long( data.size );
      rowIndex++;
    }
    // add Hidden
    if ( !StringUtils.isBlank( meta.isHiddenField() ) ) {
      r[rowIndex] = new Boolean( data.hidden );
      rowIndex++;
    }
    // Add modification date
    if ( !StringUtils.isBlank( meta.getLastModificationDateField() ) ) {
      r[rowIndex] = data.lastModificationDateTime;
      rowIndex++;
    }
    // Add Uri
    if ( !StringUtils.isBlank( meta.getUriField() ) ) {
      r[rowIndex] = data.uriName;
      rowIndex++;
    }
    // Add RootUri
    if ( !StringUtils.isBlank( meta.getRootUriField() ) ) {
      r[rowIndex] = data.rootUriName;
      rowIndex++;
    }
    return r;
  }

  private void checkType( KCell cell, ValueMetaInterface v ) throws ProcessStudioException {
    if ( !meta.isStrictTypes() ) {
      return;
    }
    switch ( cell.getType() ) {
      case BOOLEAN:
        if ( !( v.getType() == ValueMetaInterface.TYPE_STRING || v.getType() == ValueMetaInterface.TYPE_NONE || v
          .getType() == ValueMetaInterface.TYPE_BOOLEAN ) ) {
          throw new ProcessStudioException( BaseMessages.getString( PKG, "ExcelInput.Exception.InvalidTypeBoolean", v
            .getTypeDesc() ) );
        }
        break;

      case DATE:
        if ( !( v.getType() == ValueMetaInterface.TYPE_STRING || v.getType() == ValueMetaInterface.TYPE_NONE || v
          .getType() == ValueMetaInterface.TYPE_DATE ) ) {
          throw new ProcessStudioException( BaseMessages.getString( PKG, "ExcelInput.Exception.InvalidTypeDate", cell
            .getContents(), v.getTypeDesc() ) );
        }
        break;

      case LABEL:
        if ( v.getType() == ValueMetaInterface.TYPE_BOOLEAN
          || v.getType() == ValueMetaInterface.TYPE_DATE || v.getType() == ValueMetaInterface.TYPE_INTEGER
          || v.getType() == ValueMetaInterface.TYPE_NUMBER ) {
          throw new ProcessStudioException( BaseMessages.getString( PKG, "ExcelInput.Exception.InvalidTypeLabel", cell
            .getContents(), v.getTypeDesc() ) );
        }
        break;

      case EMPTY:
        // OK
        break;

      case NUMBER:
        if ( !( v.getType() == ValueMetaInterface.TYPE_STRING
          || v.getType() == ValueMetaInterface.TYPE_NONE || v.getType() == ValueMetaInterface.TYPE_INTEGER
          || v.getType() == ValueMetaInterface.TYPE_BIGNUMBER || v.getType() == ValueMetaInterface.TYPE_NUMBER ) ) {
          throw new ProcessStudioException( BaseMessages.getString( PKG, "ExcelInput.Exception.InvalidTypeNumber", cell
            .getContents(), v.getTypeDesc() ) );
        }
        break;

      default:
        throw new ProcessStudioException( BaseMessages.getString( PKG, "ExcelInput.Exception.UnsupportedType", cell
          .getType().getDescription(), cell.getContents() ) );
    }
  }

    public boolean XLSBBatchProcessing(int headerRowCount, int[] start_row, int j, Object[] r, int[] start_col
            , String[] sheetName, int sheetrowcount, FileObject file, int i, long limit) throws ProcessStudioValueException, ParseException, FileSystemException, ProcessStudioStepException {
        if (!isIncludeHeader_XLSB) {
            headerRowCount = 0;
        }
        isIncludeHeader_XLSB = false;
        for (int z = headerRowCount; z < data.batchSheet.getSheetContentAsList()
                .size(); z++) {
            startRowCounter_XLSB++;

            if (start_row[j] < startRowCounter_XLSB) {

                r = new Object[data.outputRowMeta.size()];
                List RowList = (List) data.batchSheet.getSheetContentAsList().get(z);
                for (int k = start_col[j], m = 0; k < RowList.size() && m < data.outputRowMeta.size(); k++, m++) {

                    ValueMetaInterface targetMeta = data.outputRowMeta.getValueMeta(m);
                    ValueMetaInterface sourceMeta = data.valueMetaString;
                    switch (targetMeta.getType()) {

                        case ValueMetaInterface.TYPE_DATE:

                            SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
                            Date date = formatter.parse((String) RowList.get(k));
                            long time = date.getTime();
                            int offset = TimeZone.getDefault().getOffset(time);
                            r[m] = new Date(time - offset);

                            break;
                        default:

                            r[m] = targetMeta.convertData(sourceMeta, RowList.get(k));
                    }
                }
                int rowIndex = meta.getField().length;

                // Do we need to include the filename?
                if (!StringUtils.isBlank(meta.getFileField())) {
                    r[rowIndex] = KettleVFS.getFilename(data.files.getFile(i));
                    rowIndex++;
                }

                // Do we need to include the sheetname?
                if (!StringUtils.isBlank(meta.getSheetField())) {
                    r[rowIndex] = sheetName[j];
                    rowIndex++;
                }

                // Do we need to include the sheet
                // rownumber?
                if (!StringUtils.isBlank(meta.getSheetRowNumberField())) {
                    r[rowIndex] = new Long(sheetrowcount + 1);
                    rowIndex++;
                }

                // Do we need to include the rownumber?
                if (!StringUtils.isBlank(meta.getRowNumberField())) {
                    r[rowIndex] = new Long(getLinesWritten() + 1);
                    rowIndex++;
                }
                // Possibly add short filename...
                if (!StringUtils.isBlank(meta.getShortFileNameField())) {
                    r[rowIndex] = file.getName().getBaseName();
                    rowIndex++;
                }
                // Add Extension
                if (!StringUtils.isBlank(meta.getExtensionField())) {
                    r[rowIndex] = file.getName().getExtension();
                    rowIndex++;
                }
                // add path
                if (!StringUtils.isBlank(meta.getPathField())) {
                    r[rowIndex] = file.getParent();
                    rowIndex++;
                }
                // Add Size
                if (!StringUtils.isBlank(meta.getSizeField())) {
                    r[rowIndex] = new Long(file.getContent().getSize());
                    rowIndex++;
                }
                // add Hidden
                if (!StringUtils.isBlank(meta.isHiddenField())) {
                    r[rowIndex] = new Boolean(file.isHidden());
                    rowIndex++;
                }
                // Add modification date
                if (!StringUtils.isBlank(meta.getLastModificationDateField())) {
                    r[rowIndex] = new Date(file.getContent().getLastModifiedTime());
                    rowIndex++;
                }
                // Add Uri
                if (!StringUtils.isBlank(meta.getUriField())) {
                    r[rowIndex] = file.getName().getURI();
                    rowIndex++;
                }
                // Add RootUri
                if (!StringUtils.isBlank(meta.getRootUriField())) {
                    r[rowIndex] = file.getName().getRootURI();
                    rowIndex++;
                }
                if (getLinesWritten() >= limit && limit != 0) {
                    return false;
                }
                putRow(data.outputRowMeta, r);
                incrementLinesInput();
                sheetrowcount++;

            }
        }
        return true;
    }

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws ProcessStudioStepException {
	  Object[] r = null;
	  
	  try {
			meta = (ExcelInputMeta) smi;
			data = (ExcelInputData) sdi;

			if (first) {
				first = false;
				data.outputRowMeta = new RowMeta(); // start from scratch!
				meta.getFields(data.outputRowMeta, getStepname(), null, null, this, repository, metaStore);

				if (meta.isAcceptingFilenames()) {
					// Read the files from the specified input stream...
					data.files.getFiles().clear();

					int idx = -1;
					RowSet rowSet = findInputRowSet(meta.getAcceptingStepName());
					Object[] fileRow = getRowFrom(rowSet);
					while (fileRow != null) {
						if (idx < 0) {
							idx = rowSet.getRowMeta().indexOfValue(meta.getAcceptingField());
							if (idx < 0) {
								throw new ProcessStudioException(BaseMessages.getString(PKG,
										"ExcelInput.Error.FilenameFieldNotFound", "" + meta.getAcceptingField()));
							}
						}
						String fileValue = rowSet.getRowMeta().getString(fileRow, idx);
						try {
							data.files.addFile(KettleVFS.getFileObject(fileValue, getWorkflowMeta()));
						} catch (ProcessStudioFileException e) {
							throw new ProcessStudioException(
									BaseMessages.getString(PKG, "ExcelInput.Exception.CanNotCreateFileObject", fileValue),
									e);

						}

						// Grab another row
						fileRow = getRowFrom(rowSet);
					}

				}

				handleMissingFiles();
			}

			if (meta.getSpreadSheetType() == SpreadSheetType.BINARY) {
				r = new Object[data.outputRowMeta.size()];
				String[] sheetName = meta.getSheetName();
				long limit = meta.getRowLimit();
				int[] start_row = meta.getStartRow();
				int[] start_col = meta.getStartColumn();
				boolean isHeader = meta.startsWithHeader();
				int headerRowCount = 1;
				if (!isHeader) {
					headerRowCount = 0;
				}
				outerloop: for (int i = 0; i < data.files.nrOfFiles(); i++) {
					FileObject file = null;
					file = KettleVFS.getFileObject(KettleVFS.getFilename(data.files.getFile(i)));
					if (meta.isAddResultFile()) {
						ResultFile resultFile = new ResultFile(ResultFile.FILE_TYPE_GENERAL, file,
								getWorkflowMeta().getName(), toString());
						resultFile.setComment(BaseMessages.getString(PKG, "ExcelInput.Log.FileReadByStep"));
						addResultFile(resultFile);
					}

					try {
						OPCPackage pkg = OPCPackage.open(KettleVFS.getFilename(data.files.getFile(i)));
						XSSFBReader reader = new XSSFBReader(pkg);
						XSSFBSharedStringsTable sst = new XSSFBSharedStringsTable(pkg);
						XSSFBStylesTable xssfbStylesTable = reader.getXSSFBStylesTable();
						XSSFBReader.SheetIterator it = (XSSFBReader.SheetIterator) reader.getSheetsData();
						while (it.hasNext()) {

							InputStream is = it.next();
							String name = it.getSheetName();
							for (int j = 0; j < sheetName.length; j++) {

								if (sheetName[j].equals(name)) {
                                    startRowCounter_XLSB=0;

                                    int sheetrowcount = 0;
									if(meta.isEnableXLSBBatching())
                                    {
                                        isIncludeHeader_XLSB=true;
                                        data.batchSheet =new XLSB2ListsBatch(headerRowCount,start_row,j,r,start_col,sheetName,sheetrowcount,file,i,limit,this,meta.getBatchSize());
                                        XSSFBSheetHandler sheetHandler = new XSSFBSheetHandler(is, xssfbStylesTable,
                                                it.getXSSFBSheetComments(), sst, data.batchSheet, new DataFormatter(), false);
                                        sheetHandler.parse();
                                        if(!data.batchSheet.getSheetContentAsList().isEmpty())
                                        {
                                            if(!XLSBBatchProcessing(headerRowCount, start_row, j, r, start_col, sheetName, sheetrowcount, file, i, limit))
                                            {
                                                break outerloop;
                                            }
                                        }
                                    }
									else
                                    {
                                        XLSB2Lists Sheet=new XLSB2Lists();
                                        XSSFBSheetHandler sheetHandler = new XSSFBSheetHandler(is, xssfbStylesTable,
                                                it.getXSSFBSheetComments(), sst, Sheet, new DataFormatter(), false);
                                        sheetHandler.parse();

                                        for (int z = headerRowCount + start_row[j]; z < Sheet.getSheetContentAsList()
                                                .size(); z++) {

                                            r = new Object[data.outputRowMeta.size()];
                                            List RowList = (List) Sheet.getSheetContentAsList().get(z);
                                            for (int k = start_col[j], m = 0; k < RowList.size() && m < data.outputRowMeta.size(); k++, m++) {

                                                ValueMetaInterface targetMeta = data.outputRowMeta.getValueMeta(m);
                                                ValueMetaInterface sourceMeta = data.valueMetaString;
                                                switch (targetMeta.getType()) {

                                                    case ValueMetaInterface.TYPE_DATE:

                                                        SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
                                                        Date date = formatter.parse((String) RowList.get(k));
                                                        long time = date.getTime();
                                                        int offset = TimeZone.getDefault().getOffset(time);
                                                        r[m] = new Date(time - offset);

                                                        break;
                                                    default:

                                                        r[m] = targetMeta.convertData(sourceMeta, RowList.get(k));
                                                }
                                            }
                                            int rowIndex = meta.getField().length;

                                            // Do we need to include the filename?
                                            if (!StringUtils.isBlank(meta.getFileField())) {
                                                r[rowIndex] = KettleVFS.getFilename(data.files.getFile(i));
                                                rowIndex++;
                                            }

                                            // Do we need to include the sheetname?
                                            if (!StringUtils.isBlank(meta.getSheetField())) {
                                                r[rowIndex] = sheetName[j];
                                                rowIndex++;
                                            }

                                            // Do we need to include the sheet
                                            // rownumber?
                                            if (!StringUtils.isBlank(meta.getSheetRowNumberField())) {
                                                r[rowIndex] = new Long(sheetrowcount + 1);
                                                rowIndex++;
                                            }

                                            // Do we need to include the rownumber?
                                            if (!StringUtils.isBlank(meta.getRowNumberField())) {
                                                r[rowIndex] = new Long(getLinesWritten() + 1);
                                                rowIndex++;
                                            }
                                            // Possibly add short filename...
                                            if (!StringUtils.isBlank(meta.getShortFileNameField())) {
                                                r[rowIndex] = file.getName().getBaseName();
                                                rowIndex++;
                                            }
                                            // Add Extension
                                            if (!StringUtils.isBlank(meta.getExtensionField())) {
                                                r[rowIndex] = file.getName().getExtension();
                                                rowIndex++;
                                            }
                                            // add path
                                            if (!StringUtils.isBlank(meta.getPathField())) {
                                                r[rowIndex] = file.getParent();
                                                rowIndex++;
                                            }
                                            // Add Size
                                            if (!StringUtils.isBlank(meta.getSizeField())) {
                                                r[rowIndex] = new Long(file.getContent().getSize());
                                                rowIndex++;
                                            }
                                            // add Hidden
                                            if (!StringUtils.isBlank(meta.isHiddenField())) {
                                                r[rowIndex] = new Boolean(file.isHidden());
                                                rowIndex++;
                                            }
                                            // Add modification date
                                            if (!StringUtils.isBlank(meta.getLastModificationDateField())) {
                                                r[rowIndex] = new Date(file.getContent().getLastModifiedTime());
                                                rowIndex++;
                                            }
                                            // Add Uri
                                            if (!StringUtils.isBlank(meta.getUriField())) {
                                                r[rowIndex] = file.getName().getURI();
                                                rowIndex++;
                                            }
                                            // Add RootUri
                                            if (!StringUtils.isBlank(meta.getRootUriField())) {
                                                r[rowIndex] = file.getName().getRootURI();
                                                rowIndex++;
                                            }
                                            if (getLinesWritten() >= limit && limit != 0) {
                                                break outerloop;
                                            }
                                            putRow(data.outputRowMeta, r);
                                            incrementLinesInput();
                                            sheetrowcount++;
                                        }
                                    }

								}
							}
						}

					} catch (IOException | OpenXML4JException | SAXException e) {
						throw new ProcessStudioException(e.getMessage());
					}

				}

				setOutputDone();
				return false;

			}
			else {

				// See if we're not done processing...
				// We are done processing if the filenr >= number of files.
				if (data.filenr >= data.files.nrOfFiles()) {
					if (log.isDetailed()) {
						logDetailed(BaseMessages.getString(PKG, "ExcelInput.Log.NoMoreFiles", "" + data.filenr));
					}

					setOutputDone(); // signal end to receiver(s)
					return false; // end of data or error.
				}

				// also handle the case if we have a startRow == 0 and no headers
				// and a row limit > 0
				// in this case we have to stop a row "earlier", since we start a
				// row number 0 !!!
				if ((meta.getRowLimit() > 0 && data.rownr > meta.getRowLimit())
						|| (meta.readAllSheets() && meta.getRowLimit() > 0 && data.defaultStartRow == 0
								&& data.rownr > meta.getRowLimit() - 1)
						|| (!meta.readAllSheets() && meta.getRowLimit() > 0 && data.startRow[data.sheetnr] == 0
								&& data.rownr > meta.getRowLimit() - 1)) {
					// The close of the openFile is in dispose()
					if (log.isDetailed()) {
						logDetailed(BaseMessages.getString(PKG, "ExcelInput.Log.RowLimitReached", "" + meta.getRowLimit()));
					}

					setOutputDone(); // signal end to receiver(s)
					return false; // end of data or error.
				}

				r = getRowFromWorkbooks();

				if (r != null) {
					incrementLinesInput();

					// OK, see if we need to repeat values.
					if (data.previousRow != null) {
						for (int i = 0; i < meta.getField().length; i++) {
							ValueMetaInterface valueMeta = data.outputRowMeta.getValueMeta(i);
							Object valueData = r[i];

							if (valueMeta.isNull(valueData) && meta.getField()[i].isRepeated()) {
								// Take the value from the previous row.
								r[i] = data.previousRow[i];
							}
						}
					}

					// Remember this row for the next time around!
					data.previousRow = data.outputRowMeta.cloneRow(r);

					// Send out the good news: we found a row of data!
					putRow(data.outputRowMeta, r);

					return true;
				} else {
					// This row is ignored / eaten
					// We continue though.
					return true;
				}
			}
		} catch (Exception e) {
			CommonUtil.errorHandling(e.getMessage(), WorkflowFailureReason.UNKNOWN, r, this, "");
			setOutputDone();
			return false;
		}
	}
  


  private void handleMissingFiles() throws ProcessStudioException {
    List<FileObject> nonExistantFiles = data.files.getNonExistantFiles();

    if ( nonExistantFiles.size() != 0 ) {
      String message = FileInputList.getRequiredFilesDescription( nonExistantFiles );
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "ExcelInput.Log.RequiredFilesTitle" ), BaseMessages.getString(
          PKG, "ExcelInput.Warning.MissingFiles", message ) );
      }

      if ( meta.isErrorIgnored() ) {
        for ( FileObject fileObject : nonExistantFiles ) {
          data.errorHandler.handleNonExistantFile( fileObject );
        }
      } else {
        throw new ProcessStudioException( BaseMessages.getString(
          PKG, "ExcelInput.Exception.MissingRequiredFiles", message ) );
      }

    }

    List<FileObject> nonAccessibleFiles = data.files.getNonAccessibleFiles();
    if ( nonAccessibleFiles.size() != 0 ) {
      String message = FileInputList.getRequiredFilesDescription( nonAccessibleFiles );
      if ( log.isBasic() ) {
        logBasic( BaseMessages.getString( PKG, "ExcelInput.Log.RequiredFilesTitle" ), BaseMessages.getString(
          PKG, "ExcelInput.Log.RequiredFilesMsgNotAccessible", message ) );
      }

      if ( meta.isErrorIgnored() ) {
        for ( FileObject fileObject : nonAccessibleFiles ) {
          data.errorHandler.handleNonAccessibleFile( fileObject );
        }
      } else {
        throw new ProcessStudioException( BaseMessages.getString(
          PKG, "ExcelInput.Exception.RequiredFilesNotAccessible", message ) );
      }

    }
  }

  public Object[] getRowFromWorkbooks() throws ProcessStudioException {
    // This procedure outputs a single Excel data row on the destination
    // rowsets...	  
    Object[] retval = null;
    try {
      // First, see if a file has been opened?
      if ( data.workbook == null ) {
        // Open a new openFile..
        data.file = data.files.getFile( data.filenr );
        data.filename = KettleVFS.getFilename( data.file );
        // Add additional fields?
        if ( meta.getShortFileNameField() != null && meta.getShortFileNameField().length() > 0 ) {
          data.shortFilename = data.file.getName().getBaseName();
        }
        if ( meta.getPathField() != null && meta.getPathField().length() > 0 ) {
          data.path = KettleVFS.getFilename( data.file.getParent() );
        }
        if ( meta.isHiddenField() != null && meta.isHiddenField().length() > 0 ) {
          data.hidden = data.file.isHidden();
        }
        if ( meta.getExtensionField() != null && meta.getExtensionField().length() > 0 ) {
          data.extension = data.file.getName().getExtension();
        }
        if ( meta.getLastModificationDateField() != null && meta.getLastModificationDateField().length() > 0 ) {
          data.lastModificationDateTime = new Date( data.file.getContent().getLastModifiedTime() );
        }
        if ( meta.getUriField() != null && meta.getUriField().length() > 0 ) {
          data.uriName = data.file.getName().getURI();
        }
        if ( meta.getRootUriField() != null && meta.getRootUriField().length() > 0 ) {
          data.rootUriName = data.file.getName().getRootURI();
        }
        if ( meta.getSizeField() != null && meta.getSizeField().length() > 0 ) {
          data.size = new Long( data.file.getContent().getSize() );
        }

        if ( meta.isAddResultFile() ) {
          ResultFile resultFile =
            new ResultFile( ResultFile.FILE_TYPE_GENERAL, data.file, getWorkflowMeta().getName(), toString() );
          resultFile.setComment( BaseMessages.getString( PKG, "ExcelInput.Log.FileReadByStep" ) );
          addResultFile( resultFile );
        }

        if ( log.isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "ExcelInput.Log.OpeningFile", ""
            + data.filenr + " : " + data.filename ) );
        }

        data.workbook = WorkbookFactory.getWorkbook( meta.getSpreadSheetType(), data.filename, meta.getEncoding() );
        data.errorHandler.handleFile( data.file );
        // Start at the first sheet again...
        data.sheetnr = 0;

        // See if we have sheet names to retrieve, otherwise we'll have to get all sheets...
        //
        if ( meta.readAllSheets() ) {
          data.sheetNames = data.workbook.getSheetNames();
          data.startColumn = new int[data.sheetNames.length];
          data.startRow = new int[data.sheetNames.length];
          for ( int i = 0; i < data.sheetNames.length; i++ ) {
            data.startColumn[i] = data.defaultStartColumn;
            data.startRow[i] = data.defaultStartRow;
          }
        }
      }

      boolean nextsheet = false;

      // What sheet were we handling?
      if ( log.isDebug() ) {
        logDetailed( BaseMessages
          .getString( PKG, "ExcelInput.Log.GetSheet", "" + data.filenr + "." + data.sheetnr ) );
      }
      
      String sheetName = data.sheetNames[data.sheetnr];
      KSheet sheet = data.workbook.getSheet( sheetName );      
      if ( sheet != null ) {
        // at what row do we continue reading?
        if ( data.rownr < 0 ) {
          data.rownr = data.startRow[data.sheetnr];

          // Add an extra row if we have a header row to skip...
          if ( meta.startsWithHeader() ) {
            data.rownr++;            
          }
        }
        // Start at the specified column
        data.colnr = data.startColumn[data.sheetnr];

        // Build a new row and fill in the data from the sheet...
        try {
        	KCell[] line = sheet.getRow( data.rownr );
          // Already increase cursor 1 row
          int lineNr = ++data.rownr;
          // Excel starts counting at 0
          if ( !data.filePlayList.isProcessingNeeded( data.file, lineNr, sheetName ) ) {
            retval = null; // placeholder, was already null
          } else {
            if ( log.isRowLevel() ) {
              logRowlevel( BaseMessages.getString( PKG, "ExcelInput.Log.GetLine", "" + lineNr, data.filenr
                + "." + data.sheetnr ) );
            }

            if ( log.isRowLevel() ) {
              logRowlevel( BaseMessages.getString( PKG, "ExcelInput.Log.ReadLineWith", "" + line.length ) );
            }

            ExcelInputRow excelInputRow = new ExcelInputRow( sheet.getName(), lineNr, line );
            Object[] r = fillRow( data.colnr, excelInputRow );
            if ( log.isRowLevel() ) {
              logRowlevel( BaseMessages.getString(
                PKG, "ExcelInput.Log.ConvertedLinToRow", "" + lineNr, data.outputRowMeta.getString( r ) ) );
            }

            boolean isEmpty = isLineEmpty( line );
            if ( !isEmpty || !meta.ignoreEmptyRows() ) {
              // Put the row
              retval = r;
            } else {
              if ( data.rownr > sheet.getRows() ) {
                nextsheet = true;
              }
            }

            if ( meta.stopOnEmpty() && (isEmpty || checkForRowNumber(line) ) ) {
              nextsheet = true;
              retval= null;
            }
          }
        } catch ( ArrayIndexOutOfBoundsException e ) {
          if ( log.isRowLevel() ) {
            logRowlevel( BaseMessages.getString( PKG, "ExcelInput.Log.OutOfIndex" ) );
          }

          // We tried to read below the last line in the sheet.
          // Go to the next sheet...
          nextsheet = true;
        }
      } else {
        nextsheet = true;
      }

      if ( nextsheet ) {
        // Go to the next sheet
        data.sheetnr++;

        // Reset the start-row:
        data.rownr = -1;

        // no previous row yet, don't take it from the previous sheet!
        // (that whould be plain wrong!)
        data.previousRow = null;

        // Perhaps it was the last sheet?
        if ( data.sheetnr >= data.sheetNames.length ) {
          jumpToNextFile();
        }
      }
    } catch ( Exception e ) {
        logError( BaseMessages.getString( PKG, "ExcelInput.Error.ProcessRowFromExcel", data.filename + "", e
                .toString() ), e );
        throw new ProcessStudioException( BaseMessages.getString( PKG, "ExcelInput.Error.ProcessRowFromExcel", data.filename + "", e
                .toString() ), e );

    }
    return retval;
  }

  private boolean isLineEmpty( KCell[] line ) {
    if ( line.length == 0 ) {
      return true;
    }

    boolean isEmpty = true;
    for ( int i = 0; i < line.length && isEmpty; i++ ) {
      if ( line[i] != null && !Utils.isEmpty( line[i].getContents() ) ) {
        isEmpty = false;
      }
    }
    return isEmpty;
  }

	/**
	 * In case of streamer empty rows are skipped by Streamer Library So
	 * checking whether we missed any row by comparing fetched row number with
	 * passed row number
	 */
  boolean checkForRowNumber(KCell[] line)
  {
	  int lineLength=line.length;
	  
		for (int i = 0; i < lineLength; i++) {
			
			if (line[i] != null) {
				if (data.rownr < line[i].getRow()) {
					return true;
				}
			}
		}
	  return false;
  }
  
  private void jumpToNextFile() throws ProcessStudioException {
    data.sheetnr = 0;

    // Reset the start-row:
    data.rownr = -1;

    // no previous row yet, don't take it from the previous sheet! (that
    // whould be plain wrong!)
    data.previousRow = null;

    // Close the openFile!
    data.workbook.close();
    data.workbook = null; // marker to open again.
    data.errorHandler.close();

    // advance to the next file!
    data.filenr++;
  }

  private void initErrorHandling() {
    List<FileErrorHandler> errorHandlers = new ArrayList<FileErrorHandler>( 2 );

    if ( meta.getLineNumberFilesDestinationDirectory() != null ) {
      errorHandlers.add( new FileErrorHandlerContentLineNumber(
        getWorkflow().getCurrentDate(), environmentSubstitute( meta.getLineNumberFilesDestinationDirectory() ),
        meta.getLineNumberFilesExtension(), "Latin1", this ) );
    }
    if ( meta.getErrorFilesDestinationDirectory() != null ) {
      errorHandlers.add( new FileErrorHandlerMissingFiles(
        getWorkflow().getCurrentDate(), environmentSubstitute( meta.getErrorFilesDestinationDirectory() ), meta
          .getErrorFilesExtension(), "Latin1", this ) );
    }
    data.errorHandler = new CompositeFileErrorHandler( errorHandlers );
  }

  private void initReplayFactory() {
    Date replayDate = getWorkflow().getReplayDate();
    if ( replayDate == null ) {
      data.filePlayList = FilePlayListAll.INSTANCE;
    } else {
      data.filePlayList =
        new FilePlayListReplay(
          replayDate, environmentSubstitute( meta.getLineNumberFilesDestinationDirectory() ), meta
            .getLineNumberFilesExtension(),
          environmentSubstitute( meta.getErrorFilesDestinationDirectory() ), meta.getErrorFilesExtension(),
          "Latin1" );
    }
  }

  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (ExcelInputMeta) smi;
    data = (ExcelInputData) sdi;

    if ( super.init( smi, sdi ) ) {
      initErrorHandling();
      initReplayFactory();
      data.files = meta.getFileList( this );
      if ( data.files.nrOfFiles() == 0 && data.files.nrOfMissingFiles() > 0 && !meta.isAcceptingFilenames() ) {

        logError( BaseMessages.getString( PKG, "ExcelInput.Error.NoFileSpecified" ) );
        setErrorMessage( BaseMessages.getString( PKG, "ExcelInput.Error.NoFileSpecified" ), WorkflowFailureReason.UNKNOWN );
        return false;
      }

      if ( meta.getEmptyFields().size() > 0 ) {
        // Determine the maximum filename length...
        data.maxfilelength = -1;

        for ( FileObject file : data.files.getFiles() ) {
          String name = KettleVFS.getFilename( file );
          if ( name.length() > data.maxfilelength ) {
            data.maxfilelength = name.length();
          }
        }

        // Determine the maximum sheet name length...
        data.maxsheetlength = -1;
        if ( !meta.readAllSheets() ) {
          data.sheetNames = new String[meta.getSheetName().length];
          data.startColumn = new int[meta.getSheetName().length];
          data.startRow = new int[meta.getSheetName().length];
          for ( int i = 0; i < meta.getSheetName().length; i++ ) {
            data.sheetNames[i] = meta.getSheetName()[i];
            data.startColumn[i] = meta.getStartColumn()[i];
            data.startRow[i] = meta.getStartRow()[i];

            if ( meta.getSheetName()[i].length() > data.maxsheetlength ) {
              data.maxsheetlength = meta.getSheetName()[i].length();
            }
          }
        } else {
          // Allocated at open file time: we want ALL sheets.
          if ( meta.getStartRow().length == 1 ) {
            data.defaultStartRow = meta.getStartRow()[0];
          } else {
            data.defaultStartRow = 0;
          }
          if ( meta.getStartColumn().length == 1 ) {
            data.defaultStartColumn = meta.getStartColumn()[0];
          } else {
            data.defaultStartColumn = 0;
          }
        }

        return true;
      } else {
        logError( BaseMessages.getString( PKG, "ExcelInput.Error.NotInputFieldsDefined" ) );
        setErrorMessage( BaseMessages.getString( PKG, "ExcelInput.Error.NotInputFieldsDefined" ), WorkflowFailureReason.UNKNOWN );
      }

    }
    return false;
  }

  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (ExcelInputMeta) smi;
    data = (ExcelInputData) sdi;

    if ( data.workbook != null ) {
      data.workbook.close();
    }
    if ( data.file != null ) {
      try {
        data.file.close();
      } catch ( Exception e ) {
        // Ignore close errors
      }
    }
    try {
      data.errorHandler.close();
    } catch ( ProcessStudioException e ) {
      if ( log.isDebug() ) {
        logDebug( BaseMessages.getString( PKG, "ExcelInput.Error.CouldNotCloseErrorHandler", e.toString() ) );

        logDebug( Const.getStackTracker( e ) );
      }
    }

    super.dispose( smi, sdi );
  }

}
