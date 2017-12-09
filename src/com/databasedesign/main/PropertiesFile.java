package com.databasedesign.main;

public class PropertiesFile {

	static final String PROMPT = "VroomBase> ";
	static final String VERSION = "v1.0";
	static final String DB_NAME = "Vroom Database";
	static final int PAGESIZE = 512;
	static final String HOME = "/Users/varunmehrotra";
	static final String CUR_DIR = "/data/";
	static String schema = "user_data";
	
	public static String getSchema() {
		return schema;
	}
	
	public static void setSchema(String schemaName) {
		schema = schemaName;
	}
	
	public static String getPrompt() {
		return PROMPT;
	}
	
	public static String getDBName() {
		return DB_NAME;
	}
	
	public static String getVersion() {
		return VERSION;
	}
	
	public static int getPageSize() {
		return PAGESIZE;
	}
	
	public static String getHome() {
		return HOME;
	}
	
	public static String getCurrentDirectory() {
		return CUR_DIR;
	}
}
