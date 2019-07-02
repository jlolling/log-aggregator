# log-aggregator
This program is intend to build useable log message for Talend Open Studio jobs.
If there is not log4j integrated or configured, tools like graylog will take every line as message which renders the logging as nearly unusable.
This program does:
* combines all input t one message if the input time between lines is <= 10ms.
* takes care the aggregation of lines will not last longer 2s to always have a current logging.
* Sends the messages to Graylog if configured

```
java -jar log-aggregator.jar
Parameter jobname or application_name must be set.
usage: java -jar log-aggregator-<version>.jar
 -a,--add_start_stop <arg>           Add a message for start and stop
                                     (default = true)
 -c,--config_file <arg>              Log4j config file
 -f,--flush_message_signal <arg>     Text to force flush the message.
                                     (default=FLUSH_MESSAGE)
 -g,--graylog_host <arg>             Graylog host
                                     [host|host:port|tcp:host|tcp:host:por
                                     t]
 -j,--job_name <arg>                 Job name
 -l,--layer <arg>                    Layer or system stage:
                                     [test|ref|prod]
 -o,--graylog_timeout <arg>          Timeout for Graylog TCP sender
                                     (default=5000)
 -p,--pid <arg>                      Process identifier
 -q,--queue_size <arg>               Message queue size
 -r,--redirect_to_stndout <arg>      Redirect all input received via pipe
                                     also to standard out. (default=false)
 -s,--max_message_size <arg>         Max message size
 -t,--application_name <arg>         Job name (compatible to logger)
 -v,--application_version <arg>      Job Version
 -x,--max_time_between_lines <arg>   Max time between lines to get them as
                                     one message
 -y,--max_time_until_send <arg>      Max time to collect data until a new
                                     message will be send
```

This is a typical use case:
```
java -jar myapp.jar | java -jar log-aggregator-<version>.jar -t "myapp" -v "1.0" -l "production" -g "tcp:graylog.local"
```
Please take note, in a Linux pipe the last program in the pipe will be started first to establish the input channel.
