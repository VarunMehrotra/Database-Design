CREATE database database_name;					 
	Create new database.

CREATE table table_name (id int, name varchar);   
	Create new table under respective database.

USE database_name;                               
	Switched to new database.

SHOW tables;                                     
	Display all tables under respective database.

SELECT * FROM table_name;                        
	Display all records in the table.

SELECT * FROM table_name WHERE rowid = <value>;  
	Display records whose rowid is <id>.

DROP TABLE table_name;                           
	Remove table data and its schema.

DROP Database database_name;                     
	Remove database and its table.

HELP;                                            
	Show this help information.

EXIT;                                      
	Exit the program.
