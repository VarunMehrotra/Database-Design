package com.databasedesign.operation;

import com.databasedesign.dao.*;
import com.databasedesign.main.*;
import java.io.RandomAccessFile;
import java.util.*;
import java.time.*;
import java.io.IOException;
import java.util.Map.Entry;

public class InsertQuery {

	public static void insert(String userInput) {
		
		String[] insert = userInput.split(" ");
		String tableName=insert[2].trim();
		String values = userInput.split("values")[1].replaceAll("\\(", "").replaceAll("\\)", "").trim();
		
		String[] insertValues = values.split(",");
		for(int i = 0; i < insertValues.length; i++) {
			insertValues[i] = insertValues[i].trim();
		}
		
		if(!TableValidate.isTablePresent(tableName)){
			System.out.println("Table "+tableName+" does not exist.");
			System.out.println();
			return;
		}
		else
			insertOperation(tableName, insertValues);
	}
	public static void insertOperation(String tableName, String[] values) {

		try {
			tableName = tableName.trim();

			String path = PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + PropertiesFile.getSchema() + "/" + tableName + ".tbl";

			if(tableName.equalsIgnoreCase("vroombase_tables") || tableName.equalsIgnoreCase("vroombase_columns"))
				path = PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + "catalog/" + tableName + ".tbl";


			RandomAccessFile table = new RandomAccessFile(path, "rw");

			String condition[] = { "table_name", "=", tableName };
			String columnNames[] = { "*" };
			Map<Integer, CellBean> columnsMeta = getColumnsMeta(tableName, columnNames, condition);
			String[] dataType = getDataType(columnsMeta);
			String[] isNullable = isNullable(columnsMeta);

			for (int i = 0; i < values.length; i++) {
				if (values[i].equalsIgnoreCase("null") && isNullable[i].equals("NO")) {
					System.out.println("Cannot insert NULL values in NOT NULL field");
					return;
				}
			}
			condition = new String[0];

			int pageNo=findPage(tableName,Integer.parseInt(values[0]));

			Map<Integer, CellBean> data = getData(tableName, columnNames, condition);
			if (data.containsKey(Integer.parseInt(values[0]))) {
				System.out.println("Duplicate value for primary key");
				return;
			}

			byte[] plDataType = new byte[dataType.length-1];
			int payLoadSize = getPayloadSize(tableName, values, plDataType, dataType);
			
			payLoadSize = payLoadSize+6;

			int address = checkPageOverFlow(table,pageNo,payLoadSize);
			
			if(address!= -1){
				CellBean cellBean = createCell(pageNo,Integer.parseInt(values[0]),(short)payLoadSize,plDataType,values);
				writePayload(table,cellBean,address);
			}
			else
			{
				splitLeafPage(table,pageNo);
				int pNo = findPage(tableName,Integer.parseInt(values[0]));
				int addr = checkPageOverFlow(table,pNo,payLoadSize);
				CellBean cellBean = createCell(pNo,Integer.parseInt(values[0]),(short)payLoadSize,plDataType,values);
				writePayload(table,cellBean,addr);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static Map<Integer, CellBean> getColumnsMeta(String tableName, String[] columnNames, String[] condition) {

		try {
			String str = PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + "catalog/vroombase_columns.tbl";
			RandomAccessFile table = new RandomAccessFile(str, "rw");
			int noOfPages = (int) (table.length() / PropertiesFile.getPageSize());

			Map<Integer, String> colNames = getColumnNames("vroombase_columns");
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
					recordCells = getRecords(table, cellLocations,i);
					records.putAll(recordCells);
				}
			}

			if (condition.length > 0) {
				Map<Integer, CellBean> filteredRecords = filterRecords(colNames, records, columnNames, condition);
				return filteredRecords;
			} else {
				return records;
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	public static String[] getDataType(Map<Integer, CellBean> columnsMeta) {
		int count = 0;
		String[] dataType = new String[columnsMeta.size()];
		for (Map.Entry<Integer, CellBean> entry : columnsMeta.entrySet()) {

			CellBean cellBean = entry.getValue();
			PayloadBean payloadBean = cellBean.getPayload();
			String[] data = payloadBean.getData();
			dataType[count] = data[2];
			count++;
		}
		return dataType;
	}

	public static String[] isNullable(Map<Integer, CellBean> columnsMeta) {
		int count = 0;
		String[] nullable = new String[columnsMeta.size()];
		for (Map.Entry<Integer, CellBean> entry : columnsMeta.entrySet()) {

			CellBean cellBean = entry.getValue();
			PayloadBean payloadBean = cellBean.getPayload();
			String[] data = payloadBean.getData();
			nullable[count] = data[4];
			count++;
		}
		return nullable;
	}

	public static int findPage(String tableName, int key) {
		try {

			tableName=tableName.trim();
			String path = PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + PropertiesFile.getSchema() + "/" + tableName + ".tbl";
			if(tableName.equalsIgnoreCase("vroombase_tables") || tableName.equalsIgnoreCase("vroombase_columns"))
				path = PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + "/catalog/"+tableName+".tbl";

			RandomAccessFile table = new RandomAccessFile(path, "rw");
			int noOfPages = (int) (table.length() / PropertiesFile.getPageSize());

			Map<Integer, String> colNames = getColumnNames(tableName);
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
					recordCells = getRecords(table, cellLocations,i);

					Set<Integer> rowIds=recordCells.keySet();

					Set<Integer> sortedRowIds = new TreeSet<Integer>(rowIds);

					Integer rows[]=sortedRowIds.toArray(new Integer[sortedRowIds.size()]);

					//last page
					table.seek((PropertiesFile.getPageSize() * i)+4);
					int filePointer = table.readInt();

					if(rowIds.size()==0)
						return 0;
					if(rows[0] <= key && key <= rows[rows.length - 1])
						return i;
					else if(filePointer== -1 && rows[rows.length-1]<key)
						return i;
				}
			}

		}

		catch (Exception e) {
			e.printStackTrace();
		}

		return -1;
	}

	public static Map<Integer, CellBean> getData(String tableName, String[] columnNames, String[] condition) {
		try {


			tableName=tableName.trim();

			String path = PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + PropertiesFile.getSchema() + "/" + tableName + ".tbl";

			if(tableName.equalsIgnoreCase("vroombase_tables") || tableName.equalsIgnoreCase("vroombase_columns"))
				path = PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + "catalog/" + tableName + ".tbl";

			RandomAccessFile table = new RandomAccessFile(path, "rw");
			int noOfPages = (int) (table.length() / PropertiesFile.getPageSize());

			Map<Integer,PageBean> pageInfo=new LinkedHashMap<Integer, PageBean>();


			Map<Integer, String> colNames = getColumnNames(tableName);
			Map<Integer, CellBean> records = new LinkedHashMap<Integer, CellBean>();
			for (int i = 0; i < noOfPages; i++) {
				table.seek(PropertiesFile.getPageSize() * i);
				byte pageType = table.readByte();
				if (pageType == 0x0D) {

					PageBean pageBean = new PageBean();
					pageBean.setPageNo(i);
					pageBean.setPageType(pageType);



					int noOfCells = table.readByte();
					short[] cellLocations = new short[noOfCells];
					table.seek((PropertiesFile.getPageSize() * i) + 8);
					for (int location = 0; location < noOfCells; location++) {
						cellLocations[location] = table.readShort();
					}
					Map<Integer, CellBean> recordCells = new LinkedHashMap<Integer, CellBean>();
					recordCells = getRecords(table, cellLocations,i);

					pageBean.setRecords(recordCells);
					pageInfo.put(i, pageBean);

					records.putAll(recordCells);
					//System.out.println(recordCells);
				}
			}

			if (condition.length > 0) {
				Map<Integer, CellBean> filteredRecords = filterRecords(colNames, records, columnNames, condition);
				return filteredRecords;
			} else {
				return records;
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static int getPayloadSize(String tableName, String[] values, byte[] plDataType, String[] dataType) {

		int size = 1 + dataType.length - 1;
		for (int i = 0; i < values.length-1; i++) {
			plDataType[i] = getDataTypeCode(values[i + 1], dataType[i + 1]);
			size = size + dataLength(plDataType[i]);
		}
	

		return size;
	}

	public static int checkPageOverFlow(RandomAccessFile file, int page, int payLoadsize){
		int val = -1;

		try{
			file.seek((page)*PropertiesFile.getPageSize()+2);
			int content = file.readShort();
			if(content == 0)
				return PropertiesFile.getPageSize() - payLoadsize;

			file.seek((page)*PropertiesFile.getPageSize()+1);
			int noOfCells=file.read();
			int pageHeaderSize=8+2*noOfCells+2;

			file.seek((page)*PropertiesFile.getPageSize()+2);
			short startArea =(short)((page+1)*PropertiesFile.getPageSize()- file.readShort());

			int space=startArea+pageHeaderSize;
			int spaceAvail = PropertiesFile.getPageSize()-space;

			if(spaceAvail>=payLoadsize){
				file.seek((page)*PropertiesFile.getPageSize()+2);
				short offset=file.readShort();
				return offset-payLoadsize;
			}

		}catch(Exception e){
			e.printStackTrace();
		}

		return val;
	}

	public static CellBean createCell(int pageNo,int primaryKey,short payLoadSize,byte[] dataType,String[] values)
	{
		CellBean cellBean = new CellBean();
		cellBean.setPageNo(pageNo);
		cellBean.setRow(primaryKey);
		cellBean.setPayLoadSize(payLoadSize);


		PayloadBean payloadBean = new PayloadBean();
		payloadBean.setNoOfColumns(Byte.parseByte(values.length-1+""));
		payloadBean.setDataType(dataType);
		payloadBean.setData(values);	

		cellBean.setPayload(payloadBean);
		
		return cellBean;
	}

	public static void writePayload(RandomAccessFile file, CellBean cellBean,int cellLocation)
	{

		try{
			file.seek(cellLocation);
			file.writeShort(cellBean.getPayLoadSize());
			file.writeInt(cellBean.getRow());

			PayloadBean payloadBean = cellBean.getPayload();
			file.writeByte(payloadBean.getNoOfColumns());

			byte[] dataTypes=payloadBean.getDataType();
			file.write(dataTypes);

			String data[]=payloadBean.getData();

			for(int i = 0; i < dataTypes.length; i++){
				switch(dataTypes[i]){
				case 0x00:
					file.writeByte(0);
					break;
				case 0x01:
					file.writeShort(0);
					break;
				case 0x02:
					file.writeInt(0);
					break;
				case 0x03:
					file.writeLong(0);
					break;
				case 0x04:
					file.writeByte(new Byte(data[i+1]));
					break;
				case 0x05:
					file.writeShort(new Short(data[i+1]));
					break;
				case 0x06:
					file.writeInt(new Integer(data[i+1]));
					break;
				case 0x07:
					file.writeLong(new Long(data[i+1]));
					break;
				case 0x08:
					file.writeFloat(new Float(data[i+1]));
					break;
				case 0x09:
					file.writeDouble(new Double(data[i + 1]));
					break;
				case 0x0A:
					long datetime = file.readLong();
					ZoneId zoneId = ZoneId.of("America/Chicago");
					Instant x = Instant.ofEpochSecond(datetime);
					ZonedDateTime zdt2 = ZonedDateTime.ofInstant(x, zoneId);
					zdt2.toLocalTime();
					break;
				case 0x0B:
					long date = file.readLong();
					ZoneId zoneId1 = ZoneId.of("America/Chicago");
					Instant x1 = Instant.ofEpochSecond(date);
					ZonedDateTime zdt3 = ZonedDateTime.ofInstant(x1, zoneId1);
					break;
				default:
					file.writeBytes(data[i + 1]);
					break;
				}
			}

			//update no of cells
			file.seek((PropertiesFile.getPageSize()*cellBean.getPageNo())+1);
			int noOfCells = file.readByte();

			file.seek((PropertiesFile.getPageSize()*cellBean.getPageNo())+1);
			file.writeByte((byte)(noOfCells+1));


			//update cell start offset

			//update cell arrays
			//getPositionMethod
			Map<Integer,Short> updateMap=new TreeMap<Integer,Short>();
			short[] cellLocations = new short[noOfCells];
			int[] keys=new int[noOfCells];

			for (int location = 0; location < noOfCells; location++) {

				file.seek((PropertiesFile.getPageSize() * cellBean.getPageNo())+8+(location*2));
				cellLocations[location] = file.readShort();
				file.seek(cellLocations[location]+2);
				keys[location]=file.readInt();
				updateMap.put(keys[location], cellLocations[location]);
			}
			updateMap.put(cellBean.getRow(), (short)cellLocation);

			//update Cell Array in ascending order
			file.seek((PropertiesFile.getPageSize() * cellBean.getPageNo()) + 8);
			for (Map.Entry<Integer, Short> entry : updateMap.entrySet()) {
				short offset=entry.getValue();
				file.writeShort(offset);
			}


			//update cell start area
			file.seek((PropertiesFile.getPageSize() * cellBean.getPageNo())+2);
			file.writeShort(cellLocation);
			file.close();

		}
		catch(Exception e)
		{
			e.printStackTrace();
		}	
	}

	private static void splitLeafPage(RandomAccessFile table,int currentPage) {
		// TODO Auto-generated method stub
		int newPage = createNewPage(table);
		int midKey = divideData(table,currentPage);
		moveRecords(table,currentPage,newPage,midKey);

	}

	public static Map<Integer, String> getColumnNames(String tableName) {
		Map<Integer, String> columns = new LinkedHashMap<Integer, String>();
		try {
			String str = PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + "catalog/vroombase_columns.tbl";
			RandomAccessFile table = new RandomAccessFile(str, "rw");
			int noOfPages = (int) (table.length() / PropertiesFile.getPageSize());

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
					recordCells = getRecords(table, cellLocations,i);
					for (Map.Entry<Integer, CellBean> entry : recordCells.entrySet()) {

						CellBean cellBean = entry.getValue();
						
						PayloadBean payloadBean = cellBean.getPayload();
						String[] data = payloadBean.getData();
						if (data[0].equalsIgnoreCase(tableName)) {
							
							columns.put(Integer.parseInt(data[3]), data[1]);
						}
					}
				}

			}
		} catch (Exception e) {

			e.printStackTrace();
		}

		return columns;
	}

	public static Map<Integer, CellBean> getRecords(RandomAccessFile table, short[] cellLocations, int pageNo) {

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

	public static Map<Integer, CellBean> filterRecords(Map<Integer, String> colNames, Map<Integer, CellBean> records,
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
				result = checkData((byte) 0x06, entry.getKey().toString(), condition);
			else
				result = checkData(dataTypeCodes[whereOrdinalPosition - 2], data[whereOrdinalPosition - 2], condition);

			if (result)
				filteredRecords.put(entry.getKey(), entry.getValue());
		}

		return filteredRecords;
	}

	public static byte getDataTypeCode(String value, String dataType) {
		if (value.equals("null")) {
			switch (dataType) {
			case "TINYINT":
				return 0x00;
			case "SMALLINT":
				return 0x01;
			case "INT":
				return 0x02;
			case "BIGINT":
				return 0x03;
			case "REAL":
				return 0x02;
			case "DOUBLE":
				return 0x03;
			case "DATETIME":
				return 0x03;
			case "DATE":
				return 0x03;
			case "TEXT":
				return 0x03;
			default:
				return 0x00;
			}
		} else {
			switch (dataType) {
			case "TINYINT":
				return 0x04;
			case "SMALLINT":
				return 0x05;
			case "INT":
				return 0x06;
			case "BIGINT":
				return 0x07;
			case "REAL":
				return 0x08;
			case "DOUBLE":
				return 0x09;
			case "DATETIME":
				return 0x0A;
			case "DATE":
				return 0x0B;
			case "TEXT":
				return (byte) (value.length() + 0x0C);
			default:
				return 0x00;
			}
		}
	}

	public static short dataLength(byte codes) {
		switch (codes) {
		case 0x00:
			return 1;
		case 0x01:
			return 2;
		case 0x02:
			return 4;
		case 0x03:
			return 8;
		case 0x04:
			return 1;
		case 0x05:
			return 2;
		case 0x06:
			return 4;
		case 0x07:
			return 8;
		case 0x08:
			return 4;
		case 0x09:
			return 8;
		case 0x0A:
			return 8;
		case 0x0B:
			return 8;
		default:
			return (short) (codes - 0x0C);
		}
	}

	public static int createNewPage(RandomAccessFile table) {

		try{
			int noOfPages = (int)table.length()/PropertiesFile.getPageSize();
			noOfPages = noOfPages + 1;
			table.setLength(noOfPages*PropertiesFile.getPageSize());
			table.seek((noOfPages-1)*PropertiesFile.getPageSize());
			table.writeByte(0x0D);
			return noOfPages;
		}catch(Exception e){
			e.printStackTrace();
		}

		return -1;
	}
	
	public static int divideData(RandomAccessFile table,int pageNo)
	{
		int midKey=0;
		try{
			table.seek((pageNo)*PropertiesFile.getPageSize());
			byte pageType = table.readByte();
			short numCells = table.readByte();
			// id of mid cell
			short mid = (short) Math.ceil(numCells/2);
			
			table.seek(pageNo*PropertiesFile.getPageSize()+8+(2*(mid-1)));
			short addr=table.readShort();
			table.seek(addr);
			
			if(pageType==0x0D)
				table.seek(addr+2);
			else 
				table.seek(addr+4);
		
			midKey=table.readInt();
			
			
		}catch(Exception e){
			e.printStackTrace();
		}

		return midKey;
	}
	
	public static void moveRecords(RandomAccessFile table,int currentPage,int newPage,int midKey) {
		// TODO Auto-generated method stub
		try{

			table.seek((currentPage)*PropertiesFile.getPageSize());
			byte pageType = table.readByte();
			int noOfCells = table.readByte();
			
			int mid = (int) Math.ceil(noOfCells/2);
			
			int lower = mid-1;
			int upper = noOfCells - lower;
			int content = 512;

			for(int i = mid; i <= noOfCells; i++){
				
				table.seek(currentPage*PropertiesFile.getPageSize()+8+(2*i)-2);
				short offset=table.readShort();
				table.seek(offset);
				
				int cellSize = table.readShort()+6;
				content = content - cellSize;
				
				table.seek(offset);
				byte[] cell = new byte[cellSize];
				table.read(cell);
			
				table.seek((newPage-1)*PropertiesFile.getPageSize()+content);
				table.write(cell);
				
				table.seek((newPage-1)*PropertiesFile.getPageSize()+8+(i-mid)*2);
				table.writeShort((newPage-1)*PropertiesFile.getPageSize()+content);
				
			}

			// cell start area
			table.seek((newPage-1)*PropertiesFile.getPageSize()+2);
			table.writeShort((newPage-1)*PropertiesFile.getPageSize()+content);

			
			//current page cell content area update
			table.seek((currentPage)*PropertiesFile.getPageSize()+8+(lower*2));
			short offset=table.readShort();
			table.seek((currentPage)*PropertiesFile.getPageSize()+2);
			table.writeShort(offset);

			
			
			//copy right pointer of current page to new page
			table.seek((currentPage)*PropertiesFile.getPageSize()+4);
			int rightpointer = table.readInt();
			table.seek((newPage-1)*PropertiesFile.getPageSize()+4);
			table.writeInt(rightpointer);
			//update current page
			table.seek((currentPage)*PropertiesFile.getPageSize()+4);
			table.writeInt(newPage); //CHECK HERE NP
			

			
			byte cells = (byte) lower;
			table.seek((currentPage)*PropertiesFile.getPageSize()+1);
			table.writeByte(cells);
			cells = (byte) upper;
			table.seek((newPage-1)*PropertiesFile.getPageSize()+1);
			table.writeByte(cells);
			
			//parent updation
			int parent = getParent(table,currentPage+1);
			if(parent==0){
				int parentpage = createInteriorPage(table);
				setParent(table,parentpage,currentPage,midKey);
				table.seek((parentpage-1)*PropertiesFile.getPageSize()+4);
				table.writeInt(newPage); // right child
			}
			else
			{
				if(checkforRightPointer(table,parent,currentPage+1))
				{
					setParent(table,parent,currentPage,midKey);
					table.seek((parent-1)*PropertiesFile.getPageSize()+4);
					table.writeInt(newPage); // right child
				}
				else{
					setParent(table,parent,newPage,midKey);
				}
			}
		}catch(Exception e){
			System.out.println("Error at splitLeafPage");
			e.printStackTrace();
		}
	}
	
	public static boolean checkData(byte code, String data, String[] condition) {

		if (code >= 0x04 && code <= 0x07) {
			Long dataLong = Long.parseLong(data);
			switch (condition[1]) {
			case "=":
				if (dataLong == Long.parseLong(condition[2]))
					return true;
				break;
			case ">":
				if (dataLong > Long.parseLong(condition[2]))
					return true;
				break;
			case "<":
				if (dataLong < Long.parseLong(condition[2]))
					return true;
				break;
			case "<=":
				if (dataLong <= Long.parseLong(condition[2]))
					return true;
				break;
			case ">=":
				if (dataLong >= Long.parseLong(condition[2]))
					return true;
				break;
			case "<>":
				if (dataLong != Long.parseLong(condition[2]))
					return true;
				break;
			default:
				System.out.println("undefined operator return false");
				return false;
			}

		} else if (code == 0x08 || code == 0x09) {
			Double doubleData = Double.parseDouble(data);
			switch (condition[1]) {
			case "=":
				if (doubleData == Double.parseDouble(condition[2]))
					return true;
				break;
			case ">":
				if (doubleData > Double.parseDouble(condition[2]))
					return true;
				break;
			case "<":
				if (doubleData < Double.parseDouble(condition[2]))
					return true;
				break;
			case "<=":
				if (doubleData <= Double.parseDouble(condition[2]))
					return true;
				break;
			case ">=":
				if (doubleData >= Double.parseDouble(condition[2]))
					return true;
				break;
			case "<>":
				if (doubleData != Double.parseDouble(condition[2]))
					return true;
				break;
			default:
				System.out.println("undefined operator return false");
				return false;
			}

		} else if (code >= 0x0C) {

			condition[2] = condition[2].replaceAll("'", "");
			condition[2] = condition[2].replaceAll("\"", "");
			switch (condition[1]) {
			case "=":
				if (data.equalsIgnoreCase(condition[2]))
					return true;
				break;
			case "<>":
				if (!data.equalsIgnoreCase(condition[2]))
					return true;
				break;
			default:
				System.out.println("undefined operator return false");
				return false;
			}
		}

		return false;

	}
	
	public static int getParent(RandomAccessFile table, int page) {

		try {
			int numpages = (int) (table.length() / PropertiesFile.getPageSize());
			for (int i = 0; i < numpages; i++) {

				table.seek(i * PropertiesFile.getPageSize());
				byte pageType = table.readByte();

				if (pageType == 0x05) {
					table.seek(i * PropertiesFile.getPageSize() + 4);
					int p = table.readInt();
					if (page == p)
						return i + 1;

					table.seek(i * PropertiesFile.getPageSize() + 1);
					int numrecords = table.read();
					short[] offsets = new short[numrecords];

					// insertFile.read(offsets);
					for (int j = 0; j < numrecords; j++) {
						table.seek(i * PropertiesFile.getPageSize() + 8 + 2 * j);
						offsets[i] = table.readShort();
						table.seek(offsets[i]);
						if (page == table.readInt())
							return j + 1;

					}

				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return 0;
	}
	
	public static void setParent(RandomAccessFile table, int parent,  int childPage, int midkey) {
		// TODO Auto-generated method stub
		try {
			table.seek((parent-1)*PropertiesFile.getPageSize()+1);
			int numrecords = table.read();
			if(checkInteriorRecordFit(table,parent))
			{
				
				int content=(parent)*PropertiesFile.getPageSize();
				TreeMap<Integer,Short> offsets = new TreeMap<Integer,Short>();
				if(numrecords==0){
					table.seek((parent-1)*PropertiesFile.getPageSize()+1);
					table.write(1);
					content = content-8;
					table.writeShort(content);  //cell content star
					table.writeInt(-1);		//right page pointer
					table.writeShort(content);	//offset arrays
					table.seek(content);
					table.writeInt(childPage+1);
					table.writeInt(midkey);

				}
				else{
					table.seek((parent-1)*PropertiesFile.getPageSize()+2);
					short cellContentArea = table.readShort();
					cellContentArea = (short) (cellContentArea-8);
					table.seek(cellContentArea);
					table.writeInt(childPage+1);
					table.writeInt(midkey);
					table.seek((parent-1)*PropertiesFile.getPageSize()+2);
					table.writeShort(cellContentArea);
					for(int i=0;i<numrecords;i++){
						table.seek((parent-1)*PropertiesFile.getPageSize()+8+2*i);
						short off = table.readShort();
						table.seek(off+4);
						int key = table.readInt();
						offsets.put(key, off);
					}
					offsets.put(midkey,cellContentArea);
					table.seek((parent-1)*PropertiesFile.getPageSize()+1);
					table.write(numrecords++);
					table.seek((parent-1)*PropertiesFile.getPageSize()+8);
					for(Entry<Integer, Short> entry : offsets.entrySet()) {
						table.writeShort(entry.getValue());
					}
				}
			}
			else{
				splitInteriorPage(table,parent);
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	
	public static void splitInteriorPage(RandomAccessFile table, int parent) {
		
		
		int newPage = createInteriorPage(table);
		int midKey = divideData(table, parent-1);
		writeContentInteriorPage(table,parent,newPage,midKey);
		
		
		try {
			table.seek((parent-1)*PropertiesFile.getPageSize()+4);
			int rightpage = table.readInt();
			table.seek((newPage-1)*PropertiesFile.getPageSize()+4);
			table.writeInt(rightpage);
			table.seek((parent-1)*PropertiesFile.getPageSize()+4);
			table.writeInt(newPage);
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	}
	
public static boolean checkforRightPointer(RandomAccessFile table, int parent, int rightPointer) {
		
		try {
			table.seek((parent-1)*PropertiesFile.getPageSize()+4);
			if(table.readInt()==rightPointer)
				return true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	public static boolean checkInteriorRecordFit(RandomAccessFile table, int parent) {
		
		try {
			table.seek((parent - 1) * PropertiesFile.getPageSize() + 1);
			int numrecords = table.read();
			short cellcontent = table.readShort();
			int size = 8 + numrecords * 2 + cellcontent;
			size = PropertiesFile.getPageSize() - size;
			if (size >= 8)
				return true;
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		return false;
	}
	
	
	public static int createInteriorPage(RandomAccessFile table) {
		
		int numpages =0;
		try {
			numpages= (int) (table.length()/PropertiesFile.getPageSize());
			numpages++;
			table.setLength(table.length()+PropertiesFile.getPageSize());
			table.seek((numpages-1)*PropertiesFile.getPageSize());
			table.write(0x05);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		return numpages;
	}
	
	public static void writeContentInteriorPage(RandomAccessFile table, int parent, int newPage, int midKey) {
		// TODO Auto-generated method stub
		try {
			table.seek((parent-1)*PropertiesFile.getPageSize()+1);
			int numrecords = table.read();
			int mid = (int) Math.ceil((double)numrecords/2);
			int numrecords1 = mid-1;
			int numrecords2 = numrecords-numrecords1;
			int size = PropertiesFile.getPageSize();
			for(int i=numrecords1;i<numrecords;i++)
			{
				table.seek((parent-1)*PropertiesFile.getPageSize()+8+2*i);
				short offset = table.readShort();
				table.seek(offset);
				byte[] data = new byte[8];
				table.read(data);
				size = size-8;
				table.seek((newPage-1)*PropertiesFile.getPageSize()+size);
				table.write(data);
				
				//setting offset
				table.seek((newPage-1)*PropertiesFile.getPageSize()+8+(i-numrecords1)*2);
				table.writeShort(size);
				
			}
			
			//setting number of records
			table.seek((parent-1)*PropertiesFile.getPageSize()+1);
			table.write(numrecords1);
			
			table.seek((newPage-1)*PropertiesFile.getPageSize()+1);
			table.write(numrecords2);
			
			int int_parent = getParent(table, parent);
			if(int_parent==0){
				int newParent = createInteriorPage(table);
				setParent(table, newParent, parent, midKey);
				table.seek((newParent-1)*PropertiesFile.getPageSize()+4);
				table.writeInt(newPage);
			}
			else{
				if(checkforRightPointer(table,int_parent,parent)){
					setParent(table, int_parent, parent, midKey);
					table.seek((int_parent-1)*PropertiesFile.getPageSize()+4);
					table.writeInt(newPage);
				}
				else
				setParent(table, int_parent, newPage, midKey);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
