# Encrypt File Bath

To run the batch you need maven to build the project.
This job needs 2 mandatory parameter:-
* sourceFile : path to the source text file to exit
* noOfThreads : Amount of parallel threads needed.

Perform following steps to build the application start it :

`mvn clean package && >java -jar target\batch-1.0-SNAPSHOT.jar "sourceFile=c:/dev/lines.txt" "noOfThreads=5"`
