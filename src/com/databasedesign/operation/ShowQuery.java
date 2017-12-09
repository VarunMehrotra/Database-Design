package com.databasedesign.operation;

import com.databasedesign.main.*;
import com.databasedesign.dao.*;
import java.io.File;
import java.io.RandomAccessFile;
import java.util.*;

public class ShowQuery {
	
	public static void show(String tableName, String[] columnNames, String[] condition) {
		try {

			tableName=tableName.trim();
			String path = PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + PropertiesFile.getSchema() + "/" + tableName + ".tbl";

			if(tableName.equalsIgnoreCase("vroombase_tables") || tableName.equalsIgnoreCase("vroombase_columns"))
				path = PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + "catalog/" + tableName + ".tbl";
			
			
			RandomAccessFile table = new RandomAccessFile(path, "rw");
			int noOfPages = (int) (table.length() / PropertiesFile.getPageSize());

			Map<Integer, String> colNames = InsertQuery.getColumnNames(tableName);
			Map<Integer, CellBean> records = new LinkedHashMap<Integer, CellBean>();
			for (int i = 0; i < noOfPages; i++) {
				table.seek(PropertiesFile.getPageSize() * i);
				byte pageType = table.readByte();
				if (pageType == 0x0D) {

					int noOfCells = table.readByte();
					short[] cellLocations = new short[noOfCells];
					table.seek((PropertiesFile.getPageSize() * i) + 8);
					for (int location = 0; location < noOfCells; location++) {
						cellLocations[location] = table.readShort();
					}
					Map<Integer, CellBean> recordCells = new LinkedHashMap<Integer, CellBean>();
					recordCells = InsertQuery.getRecords(table, cellLocations,i);
					records.putAll(recordCells);
					//System.out.println(recordCells);
				}
			}

			if (condition.length > 0) {
				Map<Integer, CellBean> filteredRecords = InsertQuery.filterRecords(colNames, records, columnNames, condition);
				SelectQuery.printTable(colNames, filteredRecords);
			} else {
				SelectQuery.printTable(colNames, records);
			}

		} catch (Exception e) {
			e.printStackTrace();

			
		}

	}
}
