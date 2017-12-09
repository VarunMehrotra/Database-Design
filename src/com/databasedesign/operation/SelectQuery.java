package com.databasedesign.operation;

import com.databasedesign.main.*;
import com.databasedesign.dao.*;
import java.io.File;
import java.io.RandomAccessFile;
import java.util.*;

public class SelectQuery {
	
	public static void select(String userInput, String tableName) {
		
		String[] columnNames;
		String[] condition=new String[0];
		String temp[]=userInput.split("where");
		columnNames=temp[0].split("from")[0].replaceAll("select"," ").split(",");
		
		for(int i=0;i<columnNames.length;i++)
			columnNames[i]=columnNames[i].trim();
		
		if(temp.length>1)
			condition=parseCondition(temp[1]);
		
		Query(tableName,columnNames,condition);
	}
	
	public static String[] parseCondition(String whereCondition) {

		String condition[] = new String[3];
		String values[] = new String[2];
		if (whereCondition.contains("=")) {
			values = whereCondition.split("=");
			condition[0] = values[0].trim();
			condition[1] = "=";
			condition[2] = values[1].trim();
		}

		if (whereCondition.contains(">")) {
			values = whereCondition.split(">");
			condition[0] = values[0].trim();
			condition[1] = ">";
			condition[2] = values[1].trim();
		}

		if (whereCondition.contains("<")) {
			values = whereCondition.split("<");
			condition[0] = values[0].trim();
			condition[1] = "<";
			condition[2] = values[1].trim();
		}

		if (whereCondition.contains(">=")) {
			values = whereCondition.split(">=");
			condition[0] = values[0].trim();
			condition[1] = ">=";
			condition[2] = values[1].trim();
		}

		if (whereCondition.contains("<=")) {
			values = whereCondition.split("<=");
			condition[0] = values[0].trim();
			condition[1] = "<=";
			condition[2] = values[1].trim();
		}

		if (whereCondition.contains("<>")) {
			values = whereCondition.split("<>");
			condition[0] = values[0].trim();
			condition[1] = "<>";
			condition[2] = values[1].trim();
		}
		return condition;
	}
	
	public static void Query(String tableName, String[] columnNames, String[] condition) {

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
				}
			}

			if (condition.length > 0) {
				Map<Integer, CellBean> filteredRecords = InsertQuery.filterRecords(colNames, records, columnNames, condition);
				printTable(colNames, filteredRecords);
			} else {
				printTable(colNames, records);
			}
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void printTable(Map<Integer,String> colNames, Map<Integer,CellBean> records) {
		String colString = "";
		String recString = "";
		ArrayList<String> recList = new ArrayList<String>();

		for (Map.Entry<Integer, String> entry : colNames.entrySet()) {

			String colName = entry.getValue();
			colString += colName + " | ";
		}
		System.out.println(colString);
		for (Map.Entry<Integer, CellBean> entry : records.entrySet()) {

			CellBean cellBean = entry.getValue();
			recString += cellBean.getRow();
			String data[] = cellBean.getPayload().getData();
			for (String dataS : data) {
				recString = recString + " | " + dataS;
			}
			System.out.println(recString);
			recString = "";
		}

	}
}
