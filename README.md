# log-aggregator
This program is intend to build useable log message for Talend Open Studio jobs.
If there is not log4j integrated or configured, tools like graylog will take every line as message which renders the logging as nearly unusable.
This program does:
* combines all input t one message if the input time between lines is <= 10ms.
* takes care the aggregation of lines will not last longer 2s to always have a current logging.
