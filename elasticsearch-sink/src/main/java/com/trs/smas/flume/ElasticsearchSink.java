package com.trs.smas.flume;

import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchSink extends AbstractSink implements Configurable {
	
	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSink.class);
	
	private SinkCounter sinkCounter;
	private int batchSize;
	
	private String clusterName;
	private String nodeName;
	private String clusterIP;
	private int clusterPort;
	
	private String indexName;
	
	public void configure(Context context) {
		this.clusterName = context.getString("clusterName");
		this.nodeName = context.getString("nodeName");
		this.clusterIP = context.getString("clusterIP");
		this.clusterPort = context.getInteger("clusterPort");
		this.indexName = context.getString("indexName", "elasticsearch");
		batchSize = context.getInteger("batchSize", 1000);
		
		if (sinkCounter == null) {
			sinkCounter = new SinkCounter(getName());
		}
	}

	public Status process() throws EventDeliveryException {
		Status status = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		Client client = SystemUtil.getInstance(clusterName, nodeName, clusterIP, clusterPort);
		
		BulkRequestBuilder brb =  client.prepareBulk();
		BulkResponse br = null;		
		try {
			transaction.begin();
			int i = 0;
			for (i = 0; i < batchSize; i++) {
				Event event = channel.take();
				if (event == null) {
					break;
				}
				Map<String, String> map = event.getHeaders();
				
				brb.add(client.prepareIndex(indexName.toLowerCase(),indexName.toLowerCase())
						 .setSource(SystemUtil.toJSON(map)).setId(map.get("IR_SID")));
				sinkCounter.incrementEventDrainAttemptCount();
			}

			if (i == 0) {
				sinkCounter.incrementBatchEmptyCount();
				status = Status.BACKOFF;
			} else if (i < batchSize) {
				sinkCounter.incrementBatchUnderflowCount();
			} else {
				sinkCounter.incrementBatchCompleteCount();
			}
			
			if(i > 0){
				if(brb != null && brb.numberOfActions() > 0) {
					br = brb.execute().actionGet();
					if (br.hasFailures())  	
				        throw new Exception("build index error " + br.buildFailureMessage());						
				}
			}		
			transaction.commit();
		} catch(NoNodeAvailableException e) {
			transaction.rollback();
			LOG.error("Unable to communite with es cluster, cluster ip and port is : " 
					+ this.clusterIP + ":" + this.clusterPort, e);			
			/* recreate connection to es cluster */
			SystemUtil.reBuild(clusterName, nodeName, clusterIP, clusterPort);
			return Status.BACKOFF;
		} catch(ChannelException e) {
			transaction.rollback();
			LOG.error("Unable to get event from" + " channel " + channel.getName(), e);
			return Status.BACKOFF;
		} catch(Exception e) {
			transaction.rollback();
			LOG.error("Failed to deliver event. Exception follows.", e);
			throw new EventDeliveryException("Failed to deliver event", e);
		} finally {
			transaction.close();
		}
		
		return status;
	}
}