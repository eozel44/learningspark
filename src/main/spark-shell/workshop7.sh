//sparksql shell

./bin/spark-sql

 CREATE TABLE people (name STRING, age int);

 INSERT INTO people VALUES ("eren", NULL);
 INSERT INTO people VALUES ("ahmet", 20);
 INSERT INTO people VALUES ("mehmet", 25);

 show tables;

 SELECT name FROM people WHERE age IS NULL;



