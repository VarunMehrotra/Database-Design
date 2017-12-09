package com.databasedesign.operation;

import com.databasedesign.dao.*;
import com.databasedesign.main.*;
import java.io.RandomAccessFile;
import java.util.*;
import java.time.*;
import java.io.IOException;
import java.io.FileNotFoundException;

public class DeleteQuery {

	public static void delete(String userInput) {
		String[] delete = userInput.split("where");

		String[] table = delete[0].trim().split(" ");

		String tableName = table[2].trim();
		
		String[] cond = UpdateQuery.parseCondition(delete[1]);
		
		for(int i=0; i<cond.length; i++)
		
		if(!TableValidate.isTablePresent(tableName))
		{
			System.out.println("Error : Table not present");
			return;
		}
		try {
			deleteOperation(tableName,cond);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void deleteOperation(String tableName, String[] cond) throws IOException {


		String path = PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + PropertiesFile.getSchema() + "/" + tableName + ".tbl";

		if(tableName.equalsIgnoreCase("vroombase_tables") || tableName.equalsIgnoreCase("vroombase_columns"))
			path = PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + "catalog/" + tableName + ".tbl";


		try {
			RandomAccessFile table=new RandomAccessFile(path,"rw");

			String condition[] = { "table_name", "=", tableName };
			String columnNames[] = { "*" };
			Map<Integer, CellBean> columnsMeta = InsertQuery.getColumnsMeta(tableName, columnNames, condition);
			String[] dataType = InsertQuery.getDataType(columnsMeta);
			String[] isNullable = InsertQuery.isNullable(columnsMeta);
			Map<Integer, String> colNames = InsertQuery.getColumnNames(tableName);

			condition = new String[0];

			//get page number on which data exist
			int pageNo=InsertQuery.findPage(tableName,Integer.parseInt(cond[2]));

			//check for duplicate value
			Map<Integer, CellBean> data = InsertQuery.getData(tableName, columnNames, condition);
			if (data.containsKey(Integer.parseInt(cond[2]))) {
				table.seek((PropertiesFile.getPageSize() * pageNo) + 1);
				int noOfCells = table.readByte();
				short[] cellLocations = new short[noOfCells];
				table.seek((PropertiesFile.getPageSize() * pageNo) + 8);
				for (int location = 0; location < noOfCells; location++) {
					cellLocations[location] = table.readShort();
				}
				Map<Integer, CellBean> recordCells = new LinkedHashMap<Integer, CellBean>();
				recordCells = InsertQuery.getRecords(table, cellLocations,pageNo);

				String[] condition1={cond[0],"<>",cond[2]};
				String[] columnNames1={"*"};

				Map<Integer,CellBean> filteredRecs=DropQuery.filterRecordsByData(colNames, recordCells, columnNames, condition1);
				short[] offsets=new short[filteredRecs.size()];
				int l=0;
				for (Map.Entry<Integer, CellBean> entry : filteredRecs.entrySet()){
					CellBean cellBean = entry.getValue();
					offsets[l]=cellBean.getLocation();
					table.seek(pageNo*PropertiesFile.getPageSize()+8+(2*l));
					table.writeShort(offsets[l]);
					l++;
				}

				table.seek((PropertiesFile.getPageSize()*pageNo)+1);
				table.writeByte(offsets.length);
				table.writeShort(offsets[offsets.length-1]);
				table.close();

			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}
}
