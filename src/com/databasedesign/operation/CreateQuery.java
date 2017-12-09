package com.databasedesign.operation;

import com.databasedesign.dao.*;
import com.databasedesign.main.*;

import main.com.Property;

import java.util.*;
import java.time.*;
import java.io.File;
import java.io.RandomAccessFile;

public class CreateQuery {
	
	public static void createDatabase(String createDatabaseQuery) {
		ArrayList<String> inputArray = new ArrayList<String>(Arrays.asList(createDatabaseQuery.split(" ")));
		String dbName = inputArray.get(2);

		if(dbName == null || dbName.isEmpty())
			System.out.println("Error : Invalid Query !");
		else {
			File dbFile = new File(PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + dbName);

			if(dbFile.isDirectory())
				System.out.println("Error : Database " + dbName + " is already present");
			else {
				dbFile.mkdirs();
				PropertiesFile.setSchema(dbName);
				System.out.println("Database is created Successfully");
				
			}
		}
	}
	
	public static void create(String userInput) {

		ArrayList<String> createArray = new ArrayList<String>(Arrays.asList(userInput.split(" ")));
		
		String tableName = createArray.get(2);
		
		if(createArray.get(1).equals("database"))
			createDatabase(userInput);
		else {
			String[] userArray = userInput.replaceAll("\\(", "").replaceAll("\\)", "").split(tableName);
			String[] columnName = userArray[1].trim().split(",");

			for(int i = 0; i < columnName.length; i++)
				columnName[i] = columnName[i].trim();

			if(TableValidate.isTablePresent(tableName)){
				System.out.println("Error : Table " + tableName + " is already present.");
			}
			else{
				RandomAccessFile createTableFile;
				try {
					String str = PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + PropertiesFile.getSchema() + "/" + tableName + ".tbl";
					createTableFile = new RandomAccessFile(str, "rw");
					createTable(createTableFile, tableName, columnName);
				}
				catch(Exception ex) {
					ex.printStackTrace();
				}
			}
		}
	}

	public static void createTable(RandomAccessFile createTableFile, String tableName, String[] columnName) {

		try{
			createTableFile.setLength(PropertiesFile.getPageSize());
			createTableFile.seek(0);
			createTableFile.writeByte(0x0D);
			createTableFile.seek(2);
			createTableFile.writeShort(PropertiesFile.getPageSize());
			createTableFile.writeInt(-1);
			createTableFile.close();

			//Update Vroombase_tables
			RandomAccessFile vroomBaseTable = new RandomAccessFile(PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + "/catalog/vroombase_tables.tbl", "rw");
			int noOfPages = (int) (vroomBaseTable.length()/PropertiesFile.getPageSize());
			int pageNo = 0;

			Map<Integer, CellBean> recordCells = new LinkedHashMap<Integer, CellBean>();
			for(int i=0;i<noOfPages;i++)
			{
				vroomBaseTable.seek((i*(PropertiesFile.getPageSize()))+4);
				int filePointer = vroomBaseTable.readInt();
				if(filePointer == -1){
					pageNo = i;
					vroomBaseTable.seek(i*(PropertiesFile.getPageSize())+1);
					int noOfCells = vroomBaseTable.readByte();
					short[] cellLocations = new short[noOfCells];
					vroomBaseTable.seek((PropertiesFile.getPageSize() * i) + 8);
					for (int location = 0; location < noOfCells; location++) {
						cellLocations[location] = vroomBaseTable.readShort();
					}
					recordCells = getRecords(vroomBaseTable, cellLocations,i);
				}
			}
			vroomBaseTable.close();

			Set<Integer> rowIds = recordCells.keySet();
			Set<Integer> sortedRowIds = new TreeSet<Integer>(rowIds);
			Integer rows[]=sortedRowIds.toArray(new Integer[sortedRowIds.size()]);
			int key=rows[rows.length-1]+1;

			String[] values = {String.valueOf(key),tableName.trim(),"8","10"};
			InsertQuery.insertOperation("vroombase_tables", values);

			//Update Vroombase_columns
			RandomAccessFile vroombaseColumns=new RandomAccessFile(PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + "/catalog/vroombase_columns.tbl", "rw");
			noOfPages=(int) (vroombaseColumns.length()/PropertiesFile.getPageSize());
			pageNo = 0;

			recordCells = new LinkedHashMap<Integer, CellBean>();
			for(int i=0;i<noOfPages;i++)
			{
				vroombaseColumns.seek((i*(PropertiesFile.getPageSize()))+4);
				int filePointer=vroombaseColumns.readInt();
				if(filePointer==-1){
					pageNo = i;
					vroombaseColumns.seek(i*(PropertiesFile.getPageSize())+1);
					int noOfCells = vroombaseColumns.readByte();
					short[] cellLocations = new short[noOfCells];
					vroombaseColumns.seek(((PropertiesFile.getPageSize()) * i) + 8);
					for (int location = 0; location < noOfCells; location++) {
						cellLocations[location] = vroombaseColumns.readShort();
					}
					recordCells = getRecords(vroombaseColumns, cellLocations,i);
				}
			}
			rowIds=recordCells.keySet();
			sortedRowIds = new TreeSet<Integer>(rowIds);
			rows=sortedRowIds.toArray(new Integer[sortedRowIds.size()]);
			key=rows[rows.length-1];

			for(int i = 0; i < columnName.length; i++){
				key = key + 1;

				String[] coltemp = columnName[i].split(" ");
				String isNullable="YES";

				if(coltemp.length==4)
				{
					if(coltemp[2].equalsIgnoreCase("NOT") && coltemp[3].equalsIgnoreCase("NULL"))
					{
						isNullable="NO";	
					}
					if(coltemp[2].equalsIgnoreCase("PRIMARY") && coltemp[3].equalsIgnoreCase("KEY"))
					{
						isNullable="NO";
					}

				}
				String colName = coltemp[0];
				String dataType = coltemp[1].toUpperCase();
				String ordinalPosition = String.valueOf(i+1);
				String[] val = {String.valueOf(key), tableName, colName, dataType, ordinalPosition, isNullable};
				InsertQuery.insertOperation("vroombase_columns", val);
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	private static Map<Integer, CellBean> getRecords(RandomAccessFile table, short[] cellLocations, int pageNo) {

		Map<Integer, CellBean> cells = new LinkedHashMap<Integer, CellBean>();
		for (int position = 0; position < cellLocations.length; position++) {
			try {
				CellBean cellBean = new CellBean();
				cellBean.setPageNo(pageNo);
				cellBean.setLocation(cellLocations[position]);

				table.seek(cellLocations[position]);

				short payLoadSize = table.readShort();
				cellBean.setPayLoadSize(payLoadSize);

				int rowId = table.readInt();
				cellBean.setRow(rowId);

				PayloadBean payloadBean = new PayloadBean();
				byte num_cols = table.readByte();
				payloadBean.setNoOfColumns(num_cols);

				byte[] dataType = new byte[num_cols];
				int colsRead = table.read(dataType);
				payloadBean.setDataType(dataType);

				String data[] = new String[num_cols];
				payloadBean.setData(data);

				for (int i = 0; i < num_cols; i++) {
					switch (dataType[i]) {
					case 0x00:
						data[i] = Integer.toString(table.readByte());
						data[i] = "null";
						break;

					case 0x01:
						data[i] = Integer.toString(table.readShort());
						data[i] = "null";
						break;

					case 0x02:
						data[i] = Integer.toString(table.readInt());
						data[i] = "null";
						break;

					case 0x03:
						data[i] = Long.toString(table.readLong());
						data[i] = "null";
						break;

					case 0x04:
						data[i] = Integer.toString(table.readByte());
						break;

					case 0x05:
						data[i] = Integer.toString(table.readShort());
						break;

					case 0x06:
						data[i] = Integer.toString(table.readInt());
						break;

					case 0x07:
						data[i] = Long.toString(table.readLong());
						break;

					case 0x08:
						data[i] = String.valueOf(table.readFloat());
						break;

					case 0x09:
						data[i] = String.valueOf(table.readDouble());
						break;

					case 0x0A:
						long tmp = table.readLong();
						Date dateTime = new Date(tmp);
						break;

					case 0x0B:
						long tmp1 = table.readLong();
						Date date = new Date(tmp1);
						break;

					default:
						int len = new Integer(dataType[i] - 0x0C);
						byte[] bytes = new byte[len];
						for (int j = 0; j < len; j++)
							bytes[j] = table.readByte();
						data[i] = new String(bytes);
						break;
					}

				}

				cellBean.setPayload(payloadBean);
				cells.put(rowId, cellBean);

			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		return cells;
	}
}
