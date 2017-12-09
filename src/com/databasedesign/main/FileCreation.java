package com.databasedesign.main;

import java.io.File;
import java.io.RandomAccessFile;

public class FileCreation {
	
	public static void createFile() {
		File catalog = new File(PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + "/catalog");
		File userData = new File(PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + "/user_data");
		
		catalog.mkdirs();
		userData.mkdirs();
		
		if(catalog.isDirectory())
		{
			File vroomBaseTables = new File(PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + "/catalog/vroombase_tables.tbl");
			File vroomBaseColumns = new File(PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + "/catalog/vroombase_columns.tbl");
			
			if(!vroomBaseTables.exists())
			{
				createDatabase();
			}
			if(!vroomBaseColumns.exists())
			{
				createDatabase();
			}
		}
		else
		{
			createDatabase();
		}
	}
	
	public static void createDatabase() {

		File catalog = new File(PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + "/catalog");
		File userData = new File(PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + "/user_data");
		
		catalog.mkdir();
		userData.mkdir();

		createVroomBase_Tables();
		createVroomBase_Columns();

	}
	
	public static void createVroomBase_Tables() {

		try {
			String vroomBaseTable = PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + "/catalog/vroombase_tables.tbl";
			RandomAccessFile tablesMeta = new RandomAccessFile(vroomBaseTable, "rw");
			tablesMeta.setLength(PropertiesFile.getPageSize() * 1);
			tablesMeta.seek(0);
			tablesMeta.write(0x0D);
			tablesMeta.write(0x02);
			tablesMeta.writeShort(PropertiesFile.getPageSize() - 32 - 33);
			tablesMeta.writeInt(-1);// rightmost
			tablesMeta.writeShort(PropertiesFile.getPageSize() - 32);
			tablesMeta.writeShort(PropertiesFile.getPageSize() - 32 - 33);

			tablesMeta.seek(PropertiesFile.getPageSize() - 32);
			tablesMeta.writeShort(26);
			tablesMeta.writeInt(1);
			tablesMeta.writeByte(3);
			tablesMeta.writeByte(28);
			tablesMeta.write(0x06);
			tablesMeta.write(0x05);
			tablesMeta.writeBytes("vroombase_tables");
			tablesMeta.writeInt(2);
			tablesMeta.writeShort(34); // avg_length

			tablesMeta.seek(PropertiesFile.getPageSize() - 32 - 33);
			tablesMeta.writeShort(19);
			tablesMeta.writeInt(2);
			tablesMeta.writeByte(3);
			tablesMeta.writeByte(29);
			tablesMeta.write(0x06);
			tablesMeta.write(0x05);
			tablesMeta.writeBytes("vroombase_columns");
			tablesMeta.writeInt(10);
			tablesMeta.writeShort(34); // avg_lngth

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void createVroomBase_Columns() {

		int cellHeader = 6;
		try {
			String vroomColumnTable = PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + "/catalog/vroombase_columns.tbl";
			RandomAccessFile columnsMeta = new RandomAccessFile(vroomColumnTable, "rw");
			columnsMeta.setLength(PropertiesFile.getPageSize() * 1);
			columnsMeta.seek(0);
			columnsMeta.write(0x0D);
			columnsMeta.write(10);

			int recordSize[] = new int[] { 33, 39, 40, 43, 34, 40, 41, 39, 49, 41 };
			int offset[] = new int[10];

			offset[0] = PropertiesFile.getPageSize() - recordSize[0] - cellHeader;

			// error
			columnsMeta.seek(4);

			columnsMeta.writeInt(-1);

			//columnsMeta.writeShort(offset[0]);
			for (int i = 1; i < offset.length; i++) {
				offset[i] = offset[i - 1] - (recordSize[i] + cellHeader);

			}
			columnsMeta.seek(2);
			columnsMeta.writeShort(offset[9]);

			columnsMeta.seek(8);
			for (int i = 0; i < offset.length; i++) {
				columnsMeta.writeShort(offset[i]);
			}

			// 1
			columnsMeta.seek(offset[0]);
			columnsMeta.writeShort(recordSize[0]);
			columnsMeta.writeInt(1);
			columnsMeta.writeByte(5);
			columnsMeta.write(28);
			columnsMeta.write(17);
			columnsMeta.write(15);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("vroombase_tables");
			columnsMeta.writeBytes("rowid");
			columnsMeta.writeBytes("INT");
			columnsMeta.write(1);
			columnsMeta.writeBytes("NO");

			// 2
			columnsMeta.seek(offset[1]);
			columnsMeta.writeShort(recordSize[1]);
			columnsMeta.writeInt(2);
			columnsMeta.writeByte(5);
			columnsMeta.write(28);
			columnsMeta.write(22);
			columnsMeta.write(16);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("vroombase_tables");
			columnsMeta.writeBytes("table_name");
			columnsMeta.writeBytes("TEXT");
			columnsMeta.write(2);
			columnsMeta.writeBytes("NO");

			// 3
			columnsMeta.seek(offset[2]);
			columnsMeta.writeShort(recordSize[2]);
			columnsMeta.writeInt(3);
			columnsMeta.writeByte(5);
			columnsMeta.write(28);
			columnsMeta.write(24);
			columnsMeta.write(15);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("vroombase_tables");
			columnsMeta.writeBytes("record_count");
			columnsMeta.writeBytes("INT");
			columnsMeta.write(3);
			columnsMeta.writeBytes("NO");

			// 4
			columnsMeta.seek(offset[3]);
			columnsMeta.writeShort(recordSize[3]);
			columnsMeta.writeInt(4);
			columnsMeta.writeByte(5);
			columnsMeta.write(28);
			columnsMeta.write(22);
			columnsMeta.write(20);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("vroombase_tables");
			columnsMeta.writeBytes("avg_length");
			columnsMeta.writeBytes("SMALLINT");
			columnsMeta.write(4);
			columnsMeta.writeBytes("NO");

			// 5
			columnsMeta.seek(offset[4]);
			columnsMeta.writeShort(recordSize[4]);
			columnsMeta.writeInt(5);
			columnsMeta.writeByte(5);
			columnsMeta.write(29);
			columnsMeta.write(17);
			columnsMeta.write(15);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("vroombase_columns");
			columnsMeta.writeBytes("rowid");
			columnsMeta.writeBytes("INT");
			columnsMeta.write(1);
			columnsMeta.writeBytes("NO");

			// 6
			columnsMeta.seek(offset[5]);
			columnsMeta.writeShort(recordSize[5]);
			columnsMeta.writeInt(6);
			columnsMeta.writeByte(5);
			columnsMeta.write(29);
			columnsMeta.write(22);
			columnsMeta.write(16);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("vroombase_columns");
			columnsMeta.writeBytes("table_name");
			columnsMeta.writeBytes("TEXT");
			columnsMeta.write(2);
			columnsMeta.writeBytes("NO");

			// 7
			columnsMeta.seek(offset[6]);
			columnsMeta.writeShort(recordSize[6]);
			columnsMeta.writeInt(7);
			columnsMeta.writeByte(5);
			columnsMeta.write(29);
			columnsMeta.write(23);
			columnsMeta.write(16);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("vroombase_columns");
			columnsMeta.writeBytes("column_name");
			columnsMeta.writeBytes("TEXT");
			columnsMeta.write(3);
			columnsMeta.writeBytes("NO");

			// 8
			columnsMeta.seek(offset[7]);
			columnsMeta.writeShort(recordSize[7]);
			columnsMeta.writeInt(8);
			columnsMeta.writeByte(5);
			columnsMeta.write(29);
			columnsMeta.write(21);
			columnsMeta.write(16);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("vroombase_columns");
			columnsMeta.writeBytes("data_type");
			columnsMeta.writeBytes("TEXT");
			columnsMeta.write(4);
			columnsMeta.writeBytes("NO");

			// 9
			columnsMeta.seek(offset[8]);
			columnsMeta.writeShort(recordSize[8]);
			columnsMeta.writeInt(9);
			columnsMeta.writeByte(5);
			columnsMeta.write(29);
			columnsMeta.write(28);
			columnsMeta.write(19);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("vroombase_columns");
			columnsMeta.writeBytes("ordinal_position");
			columnsMeta.writeBytes("TINYINT");
			columnsMeta.write(5);
			columnsMeta.writeBytes("NO");

			// 10
			columnsMeta.seek(offset[9]);
			columnsMeta.writeShort(recordSize[9]);
			columnsMeta.writeInt(10);
			columnsMeta.writeByte(5);
			columnsMeta.write(29);
			columnsMeta.write(23);
			columnsMeta.write(16);
			columnsMeta.write(0x04);
			columnsMeta.write(14);
			columnsMeta.writeBytes("vroombase_columns");
			columnsMeta.writeBytes("is_nullable");
			columnsMeta.writeBytes("TEXT");
			columnsMeta.write(6);
			columnsMeta.writeBytes("NO");

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
