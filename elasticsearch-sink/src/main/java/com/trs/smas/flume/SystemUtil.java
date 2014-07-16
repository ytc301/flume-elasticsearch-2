package com.trs.smas.flume;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SystemUtil {
	
	private static final Logger LOG = LoggerFactory.getLogger(SystemUtil.class);
	
	private static ObjectMapper mapper = new ObjectMapper();
	
	private static Client client = null;
		
	public static String toJSON(Object obj) {
		if(mapper == null)
			mapper = new ObjectMapper();
		try {
			return mapper.writeValueAsString(obj);
		} catch (JsonProcessingException e) {
			LOG.error(" convert object to json error : " + e.getMessage());
			return null;
		}
	}
	
	public static Client getInstance(String clusterName, String nodeName, String ip, int port) {
		if(client == null)
			client = getClient(clusterName, nodeName, ip, port);	
		return client;
	}
	
	public static void reBuild(String clusterName, String nodeName, String ip, int port) {
		while(true) {
			try {
				client.admin().cluster().prepareClusterStats().execute().actionGet();
				break;
			} catch(NoNodeAvailableException e) {
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e1) {
					LOG.error(" waiting rebuild connect to cluster failed , ", e1);
				}
				LOG.error(" connect to es cluster error, please check network or cluster basic info : ", e);
				client = getClient(clusterName, nodeName, ip, port);
				continue;
			} catch(Exception e) {
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e1) {
					LOG.error(" waiting rebuild connect to cluster failed , ", e1);
				}
				LOG.error(" connect to es cluster occur other exception : ", e);
				client = getClient(clusterName, nodeName, ip, port);
				continue;
			}
		}		
	}
	
	private static  Client getClient(String clusterName, String nodeName, String ip, int port) {
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName)
				.put("node.name", nodeName).build();
		return new TransportClient(settings)
			.addTransportAddress(new InetSocketTransportAddress(ip, port));
	}
}