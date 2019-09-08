# spark-ex

## Description

This program extracts texts from XML files in inputDirectory. Then, for each file, it sends request to CoreNLP to get entity types. Finally, the texts and the corresponding entity types will be posted to Elasticsearch for future query.
This program assumes Elasticsearch server is running on localhost:9200 and CoreNLP server is runnign on localhost:9000.

This program can be run by  
`spark-submit --class "CaseIndex" --packages org.scalaj:scalaj-http_2.11:2.4.2,com.typesafe.play:play-json_2.11:2.7.3 --master local[2] JAR_FILE FULL_PATH_OF_DIRECTORY_WITH_CASE_FILES`
