package com.databasedesign.dao;

import com.databasedesign.main.*;
import com.databasedesign.operation.*;

public class PayloadBean {

	byte noOfColumns;
	byte[] dataType;
	String[] data;
	
	
	public byte getNoOfColumns() {
		return noOfColumns;
	}
	public void setNoOfColumns(byte noOfColumns) {
		this.noOfColumns = noOfColumns;
	}
	public byte[] getDataType() {
		return dataType;
	}
	public void setDataType(byte[] dataType) {
		this.dataType = dataType;
	}
	public String[] getData() {
		return data;
	}
	public void setData(String[] data) {
		this.data = data;
	}
	
}
