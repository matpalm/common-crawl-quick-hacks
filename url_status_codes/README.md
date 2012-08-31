# intro

another quick hack to demostrate extraction of the status codes for each url from the metadata.

rather than use streaming again like my last example this one is written in java. 
it took about x2 as long to write, is x100 times as many lines of boilerplate and probably runs about x100,000 times faster :P

# build

    mvn package

# run locally

    $ s3cmd get s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/metadata-00000
    $ hadoop jar target/url_status_codes-1.0.0-jar-with-dependencies.jar com.matpalm.ExtractUrlStatusCodes \
     metadata-00000 \
     foo_bar

or if you want sequencefile compressed output too...

    $ hadoop jar target/url_status_codes-1.0.0-jar-with-dependencies.jar com.matpalm.ExtractUrlStatusCodes \
     -DcompressOutput=true \
     metadata-00000 \
     foo_bar

    12/08/30 20:38:58 INFO mapred.JobClient: Counters: 7
    12/08/30 20:38:58 INFO mapred.JobClient:   error
    12/08/30 20:38:58 INFO mapred.JobClient:     java.lang.NullPointerException=11854  # <-- could be anything (?)
    12/08/30 20:38:58 INFO mapred.JobClient:   FileSystemCounters
    12/08/30 20:38:58 INFO mapred.JobClient:     FILE_BYTES_READ=105681289
    12/08/30 20:38:58 INFO mapred.JobClient:     FILE_BYTES_WRITTEN=33449044
    12/08/30 20:38:58 INFO mapred.JobClient:   Map-Reduce Framework
    12/08/30 20:38:58 INFO mapred.JobClient:     Map input records=35068
    12/08/30 20:38:58 INFO mapred.JobClient:     Spilled Records=0
    12/08/30 20:38:58 INFO mapred.JobClient:     Map input bytes=41936945
    12/08/30 20:38:58 INFO mapred.JobClient:     Map output records=23214


    $ hadoop fs -text part-00000 | head
    http://www.museo-cb.com/museo-cb/audio-y-video/frequency-vhs/	HTTP/1.1 200 OK
    http://www.appio.com.br/informativos/info10.10.06.htm		HTTP/1.1 200 OK
    http://www.modperlbook.org/code/chapters/ch13-perf_coding/factorial.c	 HTTP/1.1 200 OK
    ...

running on emr you can specify a glob pattern for a larger set for the input and specify the s3 path directly instead of having to pull the file down first
