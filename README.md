# scala-project

Team members
Hesham Hassan, Ibrahim El-sadek, Malak Magdy

Steps to run the code: 
You will need to download Oracle connector  we recommmend ojdbc8.jar 
Adjust the sourceDirPath and the logFilePath to your local device by choosing the desired paths.

Description: 
This is a simple application that automates the process of applying some business rule to datasets that is inserted to a certain path,
as the inserted file is processed then inserted into a database after applying the business rule on and finally deleted from the folder also 
it wries everything has been done on a seprate log file. 

code description :
- we have two parts: the first part which is responsible for processing the file and publish it to database, the seconed part which is related to loading files
from directory and delete it after processing 
- main logic is inside function called (processFile) 
- we have 6 business rules applied with 6 functions 
- then using top2 and calcPrice functions we got the final result
