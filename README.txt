// Kaushik Nadimpalli
// CS6360 DavisBase Project

Navigate to where the source files are located and follow commands below to compile/run
Compile and Run Instructions for Project
 - javac DavisBase.java
 - java DavisBase

(There are some functionality issues within DavisBase such as DateTime and Date reading are not properly read which leads them to becoming null values. Furthermore, the first column should be unique as it is designed with as the row_id primary key column in mind. Furthermore, update and delete only work when where condition uses primary key.
As such, the project can definitely be improved.) 

1) CREATE TABLE DOGS (id INT, name TEXT, weight DOUBLE, height FLOAT);
2) show tables; 
// Displays Dogs tables recently created

// Inserts as shown below (Please do not add quotation marks for any inputs even TEXT type strings)

INSERT INTO DOGS (id, name, weight, height) VALUES (101, timmy, 30.0, 12.21213);
INSERT INTO DOGS (id, name, weight, height) VALUES (102, wok, 65.0, 3.34323);
INSERT INTO DOGS (id, name, weight, height) VALUES (103, leslie, 20.0, 11.45213);
INSERT INTO DOGS (id, name, weight, height) VALUES (104, johnny, 10.0, 16.84759);
INSERT INTO DOGS (id, name, weight, height) VALUES (105, olaf, 50.0, 10.38273);
INSERT INTO DOGS (id, name, weight, height) VALUES (106, jerry, 35.0, 2.39284); 
INSERT INTO DOGS (id, name, weight, height) VALUES (107, millie, 35.0, 2.38284);
INSERT INTO DOGS (id, name, weight, height) VALUES (108, subs, 40.0, 16.38281);
INSERT INTO DOGS (id, name, weight, height) VALUES (109, rover, 25.0, 13.38483);
INSERT INTO DOGS (id, name, weight, height) VALUES (110, pichu, 40.0, 14.12345);
INSERT INTO DOGS (id, name, weight, height) VALUES (111, pichu, 60.0, 15.12345);
INSERT INTO DOGS (id, name, weight, height) VALUES (112, michu, 50.0, 10.12345);

Some Commands you can run to rest DavisBase

Select * from davisbase_tables;
Select * from davisbase_columns;

Select * from Dogs where weight>30; 
// shows 8 records
Select * from Dogs where height<11.55555; 
// shows 6 records
Select * from Dogs where id>=109;
// shows 4 records
Select * from Dogs where name=pichu; 
// shows 2 records

INSERT INTO DOGS (id, name, weight, height) VALUES (112, michu, 50.0, 10.12345);
// above insert will be rejected as id not not unique

Update DOGS set weight=70.0 where id=112;
// Updates weight to 70.0 for record 112. Make sure updates are done through primary key (id in this case)
Select * from DOGS where id=112;
// above will display new weight of 70.0 for record 112

Delete from table DOGS where id=112;
// Deletes last record (record 112) from Dogs table
// *Another issue here is delete is only done with primary key.*
Select * from dogs;
// to check that the record has been deleted

- Inserts of other formats may lead to issue if not exactly inserted like the way shown above (as how is how the reads are done in the program). As such try, inserting around 20-30 records with same format as above and then testing some of the suggested queries above.

DROP TABLE DOGS;
// drops table dogs
Select * from dogs
// Displays that table does not exist
