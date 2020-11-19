This repository is used to add the missing asset data

* how to run it
``` 
mvn clean package
cd target
java -jar asset-data-completion-jar-with-dependencies.jar --bootstrap-server <bootstrap-server> --topic <topic> [--zk-server <zk-server>] [--file-dest <file-dest>]
```

|Options|Description|
|---|---|
|--bootstrap-server|The address of any kafka broker |
|--topic|The kafka topic name, here is 'enos-iam.resource.operate.event'|
|--zk-server|(Optional) The address of zookeeper which is used as the dubbo register|
|--file-dest|(Optional) The path of file which contains neo4j data of the asset tree|
