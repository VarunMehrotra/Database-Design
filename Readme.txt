Database Design
Implemented a rudimentary Database Design based on SQL, called VroomBase. VroomBase support actions on a single table at a time, no joins or nested queries. 
Upon launch, engine will present a prompt similar to the MySQL mysql> prompt or SQLite sqlite> prompt, where interactive commands may be entered :-
VroomBase>
Technical Dependency – 
System should have Eclipse or Netbeans installed.
a.	If Eclipse is not installed, please click here to download and then install –
http://www.eclipse.org/downloads/eclipse-packages/

Programming Languages Used –
a.	JAVA

Instructions to execute – Assuming user has installed Eclipse :-
a.	Import the project in Eclipse and hit “Run” choosing “DesignMain.java” file
b.	For help, execute “help;” – this will provide the list of supported commands.

Supported Commands –
All commands below are case insensitive

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
