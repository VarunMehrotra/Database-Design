package com.databasedesign.operation;

import com.databasedesign.dao.*;
import com.databasedesign.main.*;
import java.io.File;
import java.io.RandomAccessFile;
import java.util.*;

public class UpdateQuery {

	public static void update(String userInput) {
		String[]updates = userInput.toLowerCase().split("set");
		String[]table = updates[0].trim().split(" ");
		String tablename = table[1].trim();
		String set_value;
		String where=null;
		if(!TableValidate.isTablePresent(tablename))
		{
			System.out.println("Table not present");
			return;
		}
		if(updates[1].contains("where"))
		{
			String []findupdate = updates[1].split("where");
			set_value = findupdate[0].trim();
			where = findupdate[1].trim();
			UpdateOperation(tablename, parseCondition(set_value),  parseCondition((where)));
		}
		else{ 
			set_value=updates[1].trim();

			String[] no_where = new String[0];
			UpdateOperation(tablename,  parseCondition(set_value),no_where);
		}
	}

	public static void UpdateOperation(String tableName, String[] set, String[] cond) {

		String path = PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + PropertiesFile.getSchema() + "/" + tableName + ".tbl";

		if(tableName.equalsIgnoreCase("vroombase_tables") || tableName.equalsIgnoreCase("vroombase_columns"))
			path = PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + "catalog/" + tableName + ".tbl";


		try {
			RandomAccessFile file=new RandomAccessFile(path,"rw");

			String condition[] = { "table_name", "=", tableName};
			String columnNames[] = { "*" };
			Map<Integer, CellBean> columnsMeta = InsertQuery.getColumnsMeta(tableName, columnNames, condition);
			String[] dataType = InsertQuery.getDataType(columnsMeta);
			String[] isNullable = InsertQuery.isNullable(columnsMeta);
			Map<Integer, String> colNames = InsertQuery.getColumnNames(tableName);

			//ordinal position
			int k = -1;
			for (Map.Entry<Integer, String> entry : colNames.entrySet()) {
				String columnName = entry.getValue();
				if (columnName.equals(set[0])) {
					k = entry.getKey();
				}
			}

			if(cond.length>0){
				int key=Integer.parseInt(cond[2]);
				condition = new String[0];

				//get page number on which data exist
				int pageno=InsertQuery.findPage(tableName,Integer.parseInt(cond[2]));

				//check for duplicate value
				Map<Integer, CellBean> data = InsertQuery.getData(tableName, columnNames, condition);
				if (data.containsKey(Integer.parseInt(cond[2]))) {

					try {
						file.seek((pageno)*PropertiesFile.getPageSize()+1);
						int records = file.read();
						short[] offsetLocations = new short[records];
						//TreeMap<Integer, Short> offsets = new TreeMap<Integer, Short>();


						for(int j=0;j<records;j++){
							file.seek((pageno)*PropertiesFile.getPageSize()+8+2*j);
							offsetLocations[j]=file.readShort();
							file.seek(offsetLocations[j]+2);
							//int pay_size = file.readShort();
							int ky=file.readInt();
							if(key==ky){
								int no=file.read();
								byte[] sc = new byte[no];
								file.read(sc);
								int seek_positions=0;
								for(int i=0;i<k-2;i++){
									seek_positions+=InsertQuery.dataLength(sc[i]);
								}
								file.seek(offsetLocations[j]+6+no+1+seek_positions);


								byte sc_update = sc[k-2];
								switch (sc_update){

								case 0x00:	file.write(Integer.parseInt(set[2]));
								sc[k-2]=0x04;
								break;
								case 0x01:	file.writeShort(Integer.parseInt(set[2]));
								sc[k-2]=0x05;
								break;
								case 0x02:	file.writeInt(Integer.parseInt(set[2]));
								sc[k-2]=0x06;
								break;
								case 0x03:	file.writeDouble(Double.parseDouble(set[2]));
								sc[k-2]=0x09;
								break;
								case 0x04:	file.write(Integer.parseInt(set[2]));
								break;
								case 0x05:	file.writeShort(Integer.parseInt(set[2]));
								break;
								case 0x06:	file.writeInt(Integer.parseInt(set[2]));
								break;
								case 0x07:	file.writeLong(Long.parseLong(set[2]));
								break;

								case 0x08: 	file.writeFloat(Float.parseFloat(set[2]));
								break;

								case 0x09:	file.writeDouble(Double.parseDouble(set[2]));
								break;

								}

								file.seek(offsetLocations[j]+7);
								file.write(sc);

							}
						}

					}catch (Exception e) {
						e.printStackTrace(System.out);
					}
				}
			}
			else{

				try {
					int no_of_pages = (int) (file.length()/PropertiesFile.getPageSize());
					for(int l=0;l<no_of_pages;l++){
						file.seek(l*PropertiesFile.getPageSize());
						byte pageType=file.readByte();
						if(pageType==0x0D){

							file.seek((l)*PropertiesFile.getPageSize()+1);
							int records = file.read();
							short[] offsetLocations = new short[records];

							for(int j=0;j<records;j++){
								file.seek((l)*PropertiesFile.getPageSize()+8+2*j);
								offsetLocations[j]=file.readShort();
								file.seek(offsetLocations[j]+6);
								//int pay_size = file.readShort();


								int no=file.read();
								byte[] sc = new byte[no];
								file.read(sc);
								int seek_positions=0;
								for(int i=0;i<k-2;i++){
									seek_positions+=InsertQuery.dataLength(sc[i]);
								}
								file.seek(offsetLocations[j]+6+no+1+seek_positions);


								byte sc_update = sc[k-2];
								switch (sc_update){

								case 0x00:	file.write(Integer.parseInt(set[2]));
								sc[k-2]=0x04;
								break;
								case 0x01:	file.writeShort(Integer.parseInt(set[2]));
								sc[k-2]=0x05;
								break;
								case 0x02:	file.writeInt(Integer.parseInt(set[2]));
								sc[k-2]=0x06;
								break;
								case 0x03:	file.writeDouble(Double.parseDouble(set[2]));
								sc[k-2]=0x09;
								break;
								case 0x04:	file.write(Integer.parseInt(set[2]));
								break;
								case 0x05:	file.writeShort(Integer.parseInt(set[2]));
								break;
								case 0x06:	file.writeInt(Integer.parseInt(set[2]));
								break;
								case 0x07:	file.writeLong(Long.parseLong(set[2]));
								break;

								case 0x08: 	file.writeFloat(Float.parseFloat(set[2]));
								break;

								case 0x09:	file.writeDouble(Double.parseDouble(set[2]));
								break;

								}

								file.seek(offsetLocations[j]+7);
								file.write(sc);

							}
						}
					}
				}catch (Exception e) {
					e.printStackTrace(System.out);
				}
			}
		}catch (Exception e) {
			e.printStackTrace(System.out);
		}
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

}
