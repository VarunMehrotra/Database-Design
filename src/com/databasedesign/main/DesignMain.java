package com.databasedesign.main;

import java.util.Scanner;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import com.databasedesign.operation.*;
import com.databasedesign.dao.*;

public class DesignMain {
	
	static boolean quit = false;
	
	public static void main(String[] args) {
		Scanner scanner = new Scanner(System.in);
		String userInput = "";
		Helper.printInfo();
		FileCreation.createFile();
		
		while(!quit) {
			System.out.print(PropertiesFile.getPrompt());
			userInput = scanner.nextLine().replace("\n", "").replaceAll("\\(", " \\( ").replace(";","").replace("\r", "").trim().toLowerCase();
			parsingUserInput(userInput);
		}
		scanner.close();
	}
	
	public static void parsingUserInput(String userInput) {
		ArrayList<String> inputArray = new ArrayList<String>(Arrays.asList(userInput.split(" ")));
		
		switch(inputArray.get(0)) {
		case "show":
			String[] condition=new String[0];
			String[] columnNames={"*"};
			ShowQuery.show("vroombase_tables",columnNames,condition);
			break;
		case "select":
			SelectQuery.select(userInput, inputArray.get(3));
			break;
		case "insert":
			InsertQuery.insert(userInput);
			break;
		case "exit":
			quit = true;
			break;
		case "drop":
			DropQuery.drop(userInput);
			break;
		case "delete":
			DeleteQuery.delete(userInput);
			break;
		case "create":
			CreateQuery.create(userInput);
			break;
		case "update":
			UpdateQuery.update(userInput);
			break;
		case "help":
			Helper.help();
			break;
		case "use":
			currentSchema(userInput);
			break;
		default:
			System.out.println("Error : Invalid Query !");
		}
	}
	
	public static void currentSchema(String userInput) {
		ArrayList<String> inputArray = new ArrayList<String>(Arrays.asList(userInput.split(" ")));
		String dbName = inputArray.get(1);
		
		if(dbName == null || dbName.isEmpty())
			System.out.println("Error : Invalid Query !");
		else {
			File dbFile = new File(PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + dbName);

			if(dbFile.isDirectory())
				PropertiesFile.setSchema(dbName);
			else {
				System.out.println("Error : Database " + dbName + " is not present");
			}
		}
	}
}
