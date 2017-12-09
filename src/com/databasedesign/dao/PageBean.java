package com.databasedesign.dao;

import com.databasedesign.main.*;
import com.databasedesign.operation.*;
import java.util.*;

public class PageBean {
	
	public byte getPageType() {
		return pageType;
	}
	public void setPageType(byte pageType) {
		this.pageType = pageType;
	}
	int pageNo;
	byte pageType;
	
	Map<Integer,CellBean> records;
	
	public int getPageNo() {
		return pageNo;
	}
	public void setPageNo(int pageNo) {
		this.pageNo = pageNo;
	}
	public Map<Integer, CellBean> getRecords() {
		return records;
	}
	public void setRecords(Map<Integer, CellBean> records) {
		this.records = records;
	}
	
}
