all: compile-files run-DBApp

compile-files:
	javac src/kalabalaDB/*.java libs/BPTree/*.java libs/General/*.java libs/RTree/*.java -d classes/
	
run-DBApp:
	java -classpath classes/ kalabalaDB.DBAppTest
	
clean:
	rm -r classes/*
	rm -r data/*