package com.databasedesign.main;

import com.databasedesign.dao.*;
import com.databasedesign.operation.*;
import java.io.File;

public class TableValidate {

	public static boolean isTablePresent(String tableName)
	{
		String filename = tableName + ".tbl";
		String userDataDir;
		
		if(PropertiesFile.getSchema().equals("user_data"))
			userDataDir = PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + "user_data/";
		else
			userDataDir = PropertiesFile.getHome() + PropertiesFile.getCurrentDirectory() + PropertiesFile.getSchema() + "/";
		
		try {
			File userdata = new File(userDataDir);
			String[] currentFiles = userdata.list();
			
			for (String file : currentFiles) {
				if(filename.equals(file))
					return true;
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return false;
	}
}
