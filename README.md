# Exporter

The purpose of the program is to export tweets extracted by the Tweetracker from the mongodb database to a SQL database in real time. This program needs to run in pair with the NER program. This program communicates with the NER program to ensure that it exports the documents that have been processed by the NER program in real time, and not export the documents that have not been processed by the NER program.

## How to Compile

To compile the code, use gradle:

gradle build

## How to run

To run the code, make sure to put the config.properties file in the same directory as the runnable file. Then do ./ner.

## Keep track of the progress

While the program is running, the current progress can be shown in a http server, with host and port defined in the config.properties file.
* **[host]:[port]/status** - shows the current object id of the tweet that's been exported
* **[host]:[port]/progress** - shows the current progress of the program.

* check the log files in the /log folder