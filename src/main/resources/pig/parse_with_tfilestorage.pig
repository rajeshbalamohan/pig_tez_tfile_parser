-- Using TFileStorage itself, so that we dont need to copy the logs
register '/grid/4/home/rajesh/tez-autobuild/pig-champlain/pig/datafu-1.2.0.jar';
register '/grid/4/home/rajesh/tez-autobuild/pig-champlain/pig/piggybank.jar';
register '/grid/4/home/rajesh/tez-autobuild/pig-champlain/udf/version_1/udf.jar'

define quantile datafu.pig.stats.Quantile('0.0','0.10','0.20','0.30', '0.40', '0.50','0.60', '0.70',
'0.80', '0.90','1.0');

-- TODO:  This can easily be optimized using SPLIT in Pig, so that we read the source only once.  But as of now, its fine.
DEFINE parseFetcherData() RETURNS void {
 -- Generate data sets by parsing logs (Generate rate.csv and store it for referencing it later)
	raw = LOAD '$INPUT_LOGS' USING org.pig.storage.TFileStorage() AS (line:chararray);
	completedLines = FOREACH raw GENERATE org.pig.udf.ProcessCompletedLines(line, '$MACHINE_MAPPING_FILE_IN_HDFS', 'completed') as (completed:chararray);
	completedLinesFiltered = FILTER completedLines BY completed is not null;
	STORE completedLinesFiltered INTO '$ATTEMPT_INFO';

	-- Generate srcToAttempt Information (store it for referencing it later)
	-- raw1= LOAD '$INPUT_LOGS' USING org.pig.storage.TFileStorage() AS (line:chararray);
  raw1 = FILTER raw BY line MATCHES '.*for url.*';
  httpLines = FOREACH raw GENERATE org.pig.udf.ProcessCompletedLines(line, '$MACHINE_MAPPING_FILE_IN_HDFS', 'http') as (http:chararray);
  httpLinesFiltered = FILTER httpLines BY http is not null;
  STORE httpLinesFiltered INTO '$SRC_TO_ATTEMPT_INFO';
}

-- Generate end to end shuffle time (i.e connect time till the last attempt fetch per source->destination)
DEFINE generateEndToEndShuffleTime() RETURNS void {
	raw = LOAD '$INPUT_LOGS' USING org.pig.storage.TFileStorage() AS (line:chararray);
	completedLines = FOREACH raw GENERATE org.pig.udf.ProcessCompletedLines(line, '$MACHINE_MAPPING_FILE_IN_HDFS', 'EndToEnd') as (completed:chararray);
	completedLinesFiltered = FILTER completedLines BY completed is not null;
	STORE completedLinesFiltered INTO '$END_TO_END_TIMINGS';
}

-- At a per thread and per attempt level, generate the source to destination mapping
DEFINE generateSourceToDestinationMappingPerAttempt() RETURNS void {
	rate1 = LOAD '$ATTEMPT_INFO' using PigStorage(',') as (millis:long, vertex:chararray, threadId:chararray, src:chararray, attempt:chararray, timeTaken:chararray, transferRate:chararray, compressedSize:chararray);
	rate = FOREACH rate1 GENERATE millis, vertex, threadId,src,attempt, timeTaken, transferRate, compressedSize;
	srcToAttempt1 = LOAD '$SRC_TO_ATTEMPT_INFO' using PigStorage(',') as (millis:long, vertex:chararray, threadId:chararray, src:chararray, dest:chararray, query:chararray);
	srcToAttempt = FOREACH srcToAttempt1 GENERATE millis, vertex, threadId, src, dest, query;
	j = JOIN rate BY (vertex, threadId, src),  srcToAttempt BY (vertex, threadId,src) PARALLEL 20;
	result = FOREACH j GENERATE rate::millis, rate::vertex, rate::threadId, rate::src, srcToAttempt::dest, rate::attempt, rate::timeTaken, rate::transferRate, rate::compressedSize;
	STORE result INTO '$ATTEMPT_TIMINGS' USING PigStorage(',');
}

-- Generate all state transition related information
DEFINE generateStateTransitions() RETURNS void {
	-- Generate data sets by parsing logs (Generate rate.csv and store it for referencing it later)
	raw = LOAD '$INPUT_LOGS' USING org.pig.storage.TFileStorage() AS (line:chararray);
  raw = FILTER raw BY line MATCHES '.*transitioned from.*';
  n = FOREACH raw GENERATE FLATTEN(org.pig.udf.logs.Log4jParser(line));
  parseMsg = FOREACH n GENERATE data::time as time, FLATTEN(org.pig.udf.statemachine.ParseStateTransition(data::message));
  -- state: {time: chararray,stateTransition::desc: chararray,stateTransition::from: chararray,stateTransition::to: chararray}
  state = FILTER parseMsg BY stateTransition::desc is not null;
	STORE state INTO '/tmp/delete/' USING PigStorage(',');
}

-- Parse state machine information
DEFINE parseStateMachineInfo(regEx, location) RETURNS void {
		raw = load '$INPUT_LOGS' using org.pig.storage.TFileStorage() AS (line:chararray);
		filtered = FILTER raw BY (line MATCHES '.*transitioned from.*') AND (line MATCHES '$regEx') AND
		 (line MATCHES '.*INFO.*');
		n = FOREACH filtered GENERATE FLATTEN(org.pig.udf.logs.Log4jParser(line));
		parseMsg = FOREACH n GENERATE org.pig.udf.logs.ParseTime(data::time) as time, FLATTEN(org.pig.udf.statemachine.ParseStateTransition(data::message));
		-- state: {time: chararray,stateTransition::desc: chararray,stateTransition::from: chararray,stateTransition::to: chararray}
		state = FILTER parseMsg BY stateTransition::desc is not null;
		g = GROUP state BY stateTransition::desc PARALLEL 100;
		ordered = FOREACH g {
				sorted = ORDER state BY time;
				computeTime = org.pig.udf.statemachine.ComputeStateTransitionTime(sorted);
				GENERATE FLATTEN(group), FLATTEN(computeTime);
			}
		cleanMsgs = FILTER ordered BY org.apache.pig.piggybank.evaluation.string.LENGTH(transition::errorMsg) == 0 ;
		groupForQuantiles = GROUP cleanMsgs BY (transition::from, transition::intermediate, transition::to) PARALLEL 50;
		quantiles = FOREACH groupForQuantiles {
			sorted = ORDER cleanMsgs by transition::timeTaken;
			GENERATE FLATTEN(group) as id, quantile(sorted.transition::timeTaken) as quartiles;
		}
		STORE quantiles INTO '$location' USING PigStorage(',');
}

set pig.splitCombination false;
set tez.grouping.min-size 52428800
set tez.grouping.max-size 52428800
-- parse the fetcher logs
parseFetcherData();

-- Generate end to end shuffle time (i.e connect time till the last attempt fetch per source->destination)
generateEndToEndShuffleTime();
exec;

-- Generate per attempt per thread information
generateSourceToDestinationMappingPerAttempt();
exec;

--parseStateMachineInfo('.*container.*', '$RESULTS_DIR/container_state_machine/');
--exec;

--Run using "$PIG_HOME/bin/pig -x tez -param_file params.txt -f parse_with_tfilestorage.pig"