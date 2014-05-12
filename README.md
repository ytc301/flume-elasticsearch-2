### build
    mvn package && mvn dependency:copy-dependencies -DincludeScope=runtime

### deploy
* cd $FLUME_HOME
* mkdir -p plugins.d/trshybase-source plugins.d/trsserver-sink
* copy trshybase-source/target/trshybase-source.jar to $FLUME_HOME/plugins.d/trshybase-source/lib/
* copy trshybase-source/target/dependency/*.jar to $FLUME_HOME/plugins.d/trshybase-source/libext/
* copy trsserver-source/target/trsserver-source.jar to $FLUME_HOME/plugins.d/trsserver-source/lib/
* copy trsserver-source/target/dependency/*.jar to $FLUME_HOME/plugins.d/trsserver-source/libext/
* copy trsserver-sink/target/trsserver-sink.jar to $FLUME_HOME/plugins.d/trsserver-sink/lib/
* copy trsserver-sink/target/dependency/*.jar to $FLUME_HOME/plugins.d/trsserver-sink/libext/
* copy libtrsbean.so to $FLUME_HOME/plugins.d/trsserver-sink/native/
* copy feed-sink/target/feed-sink.jar to $FLUME_HOME/plugins.d/feed-sink/lib/
* copy feed-sink/target/dependency/*.jar to $FLUME_HOME/plugins.d/feed-sink/libext/

### run demo

    bin/flume-ng agent --conf conf --conf-file example.conf --name a1 -Dflume.root.logger=INFO,console -Dflume.monitoring.type=http -Dflume.monitoring.port=34545

### run production
	nohup bin/flume-ng agent --conf conf --conf-file tasks/runtime/hotspot.conf --name agent -Dflume.log.file=hotspot.log -Dflume.monitoring.type=http -Dflume.monitoring.port=34545 &

	nohup bin/flume-ng agent --conf conf --conf-file tasks/runtime/warehouse.conf --name agent -Dflume.log.file=warehouse.log -Dflume.monitoring.type=http -Dflume.monitoring.port=34546 &

	nohup bin/flume-ng agent --conf conf --conf-file tasks/runtime/hybase.conf --name agent -Dflume.log.file=hybase.log -Dflume.monitoring.type=http -Dflume.monitoring.port=34547 &

	nohup bin/flume-ng agent --conf conf --conf-file tasks/runtime/hybase-to-feed.conf --name agent -Dflume.log.file=feed.log -Dflume.monitoring.type=http -Dflume.monitoring.port=34548 &

### debug
	-Dflume.root.logger=DEBUG,LOGFILE