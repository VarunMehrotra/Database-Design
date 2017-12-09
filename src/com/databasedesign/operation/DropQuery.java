package com.databasedesign.operation;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.*;
import java.io.IOException;

import com.databasedesign.main.*;
import com.databasedesign.dao.*;

public class DropQuery {

	public static void drop(String userInput) {

		String[] droptemp = userInput.replace(";"," ").split(" ");

		String tableName = droptemp[2].trim();

		if(!TableValidate.isTablePresent(tableName)){
			System.out.println("Table "+tableName+" is not present.");
			System.out.println();
		}
		else
		{
			dropTable(tableName);
			String path = PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + PropertiesFile.getSchema() + "/" + tableName + ".tbl";
			File file=new File(path);
			file.delete();
		}
	}

	public static void dropTable(String tableName) {
		try
		{
			String path = PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + "catalog/";
			
			RandomAccessFile vroombaseTables=new RandomAccessFile( path+"vroombase_tables.tbl", "rw");
			updateMetaOffset(vroombaseTables,"vroombase_tables",tableName);


			RandomAccessFile vroombaseColumns=new RandomAccessFile(path+"vroombase_columns.tbl", "rw");
			updateMetaOffset(vroombaseColumns,"vroombase_columns",tableName);

		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public static void updateMetaOffset(RandomAccessFile vroombaseTables,String metaTable,String tableName) throws IOException
	{
		int noOfPages = (int) (vroombaseTables.length() / PropertiesFile.getPageSize());

		Map<Integer, String> colNames = InsertQuery.getColumnNames(metaTable);
		
		for (int i = 0; i < noOfPages; i++) {
			vroombaseTables.seek(PropertiesFile.getPageSize() * i);
			byte pageType = vroombaseTables.readByte();
			if (pageType == 0x0D) {

				int noOfCells = vroombaseTables.readByte();
				short[] cellLocations = new short[noOfCells];
				vroombaseTables.seek((PropertiesFile.getPageSize() * i) + 8);
				for (int location = 0; location < noOfCells; location++) {
					cellLocations[location] = vroombaseTables.readShort();
				}
				Map<Integer, CellBean> recordCells = new LinkedHashMap<Integer, CellBean>();
				recordCells = InsertQuery.getRecords(vroombaseTables, cellLocations,i);
				
				
				String[] condition={"table_name","<>",tableName};
				String[] columnNames={"*"};
				
				Map<Integer,CellBean> filteredRecs = filterRecordsByData(colNames, recordCells, columnNames, condition);
				short[] offsets=new short[filteredRecs.size()];
				int l=0;
				for (Map.Entry<Integer, CellBean> entry : filteredRecs.entrySet()){
					CellBean cellBean = entry.getValue();
					offsets[l] = cellBean.getLocation();
					vroombaseTables.seek(i*PropertiesFile.getPageSize()+8+(2*l));
					vroombaseTables.writeShort(offsets[l]);
					l++;
				}
				
				vroombaseTables.seek((PropertiesFile.getPageSize() * i)+1);
				vroombaseTables.writeByte(offsets.length);
				vroombaseTables.writeShort(offsets[offsets.length-1]);
			}
		}
	}
	
	public static Map<Integer, CellBean> filterRecordsByData(Map<Integer, String> colNames, Map<Integer, CellBean> records,
			String[] resultColumnNames, String[] condition) {

		Set<String> resultColumnSet = new HashSet<String>(Arrays.asList(resultColumnNames));
		Map<Integer, CellBean> filteredRecords = new LinkedHashMap<Integer, CellBean>();
		
		int whereOrdinalPosition = 2;
		for (Map.Entry<Integer, String> entry : colNames.entrySet()) {
			String columnName = entry.getValue();
			if (columnName.equals(condition[0])) {
				whereOrdinalPosition = entry.getKey();
			}
		}
		Set<Integer> ordinalPositions = colNames.keySet();
		for (Map.Entry<Integer, CellBean> entry : records.entrySet()) {
			CellBean cellBean = entry.getValue();
			PayloadBean payloadBean = cellBean.getPayload();
			String[] data = payloadBean.getData();
			byte[] dataTypeCodes = payloadBean.getDataType();

			boolean result;
			if (whereOrdinalPosition == 1)
				result = InsertQuery.checkData((byte) 0x06, entry.getKey().toString(), condition);
			else
				result = InsertQuery.checkData(dataTypeCodes[whereOrdinalPosition - 2], data[whereOrdinalPosition - 2], condition);

			if (result)
				filteredRecords.put(entry.getKey(), entry.getValue());
		}

		return filteredRecords;

	}
}
