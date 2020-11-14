This repository is used to add the missing asset data

* how to run it
``` 
mvn clean package
cd target
java -jar asset-data-completion-jar-with-dependencies.jar --bootstrap-server <bootstrap-server> --topic <topic> --file-dest <file-dest>
```