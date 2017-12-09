package com.databasedesign.main;

public class Helper {
	
	public static void printInfo() {
		System.out.println(star(100));
		System.out.println("Welcome to " + PropertiesFile.getDBName());
		System.out.println("VroomBase Version : " + PropertiesFile.getVersion());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println("Type \"exit;\" to exit.");
		System.out.println(star(100));
	}
	
	public static String star(int num) {
		String str = "";
		
		for(int i=0; i<num; i++)
			str = str + "*";
		
		return str;
	}
	
	public static void help() {
		System.out.println(star(100));
		System.out.println("SUPPORTED COMMANDS");
		System.out.println("All commands below are case insensitive");
		System.out.println();
		System.out.println("\t SELECT * FROM table_name;                        						Display all records in the table.");
		System.out.println("\t SELECT * FROM table_name WHERE rowid = <value>;   						Display records whose rowid is <id>.");
		System.out.println("\t USE database_name;                               						Select any existing database");
		System.out.println("\t CREATE DATABASE database_name;                   						Create a new non-existing database");
		System.out.println("\t CREATE TABLE table_name (rowId int primary key,......);                	Create a new non-existing table");
		System.out.println("\t DROP TABLE table_name;                   						        Drop an existing table");
		System.out.println("\t SHOW tables;                   										Show existing tables");
		System.out.println("\t HELP;                                           						Show this help information");
		System.out.println("\t EXIT;                                            						Exit the program");
		System.out.println();
		System.out.println();
		System.out.println(star(100));
	}
}
