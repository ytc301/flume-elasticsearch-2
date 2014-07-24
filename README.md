### build
    mvn package && mvn dependency:copy-dependencies -DincludeScope=runtime

### deploy
* cd $FLUME_HOME
* mkdir -p plugins.d/trshybase-source plugins.d/trsserver-sink

* copy file-source/target/trsserver-source.jar to $FLUME_HOME/plugins.d/trsserver-source/lib/
* copy file-source/target/dependency/*.jar to $FLUME_HOME/plugins.d/trsserver-source/libext/
* copy elasticsearch-sink/target/trsserver-sink.jar to $FLUME_HOME/plugins.d/trsserver-sink/lib/
* copy elasticsearch-sink/target/dependency/*.jar to $FLUME_HOME/plugins.d/trsserver-sink/libext/

### run demo

    bin/flume-ng agent --conf conf --conf-file example.conf --name a1 -Dflume.root.logger=INFO,console -Dflume.monitoring.type=http -Dflume.monitoring.port=34545

### run production

	nohup bin/flume-ng agent --conf conf --conf-file conf/flume-elasticsearch.conf --name agent -Dflume.log.file=elasticsearch.log -Dflume.monitoring.type=http -Dflume.monitoring.port=34545 &


### debug
	-Dflume.root.logger=DEBUG,LOGFILE
