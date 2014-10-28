A simple Pig loader to parse TFiles and provide it in line by line format for distributed
processing of logs.

Currently we rely on "yarn logs -applicationId <appId> | grep blah" etc. Or we download the logs
locally and mine them.  It can be time consuming when we need to download huge volume of log files.

Build/Install:
==============
1. mvn clean package
2. "udf-1.0-SNAPSHOT.jar" would be created in ./target directory


Running pig with tez:
====================
1. Install pig
2. $PIG_HOME/bin/pig -x tez (to open grunt shell)


Sample pig script:
==================
set pig.splitCombination false;
set tez.grouping.min-size 52428800;
set tez.grouping.max-size 52428800;

register '/grid/0/pig/udf/udf-1.0-SNAPSHOT.jar';
raw = load '/app-logs/rajesh/logs/application_1411511669099_0591/*' using 
        org.pig.storage.TFileStorage() AS (machine:chararray, key:chararray, line:chararray);
filterByLine = FILTER raw BY (key MATCHES '.*container_1411511669099_0591_01_000001.*')
                   AND (line MATCHES '.*Shuffle.*');
dump filterByLine;

filterByMachine = FILTER raw BY (machine MATCHES '.*myMac104.*');
dump filterByMachine;

