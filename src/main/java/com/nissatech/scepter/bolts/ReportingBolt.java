package com.nissatech.scepter.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import noNamespace.ContextAttribute;
import noNamespace.ContextAttributeList;
import noNamespace.ContextElement;
import noNamespace.UpdateContextRequestDocument;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.LoggerFactory;

/**
 *
 * @author aleksandar
 */
public class ReportingBolt extends BaseRichBolt
{
    private static final long serialVersionUID = 5366589266515902196L;

    private transient HttpClient httpclient;
    private String hostname;
    private int port;
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd)
    {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc)
    {
        httpclient = HttpClients.createDefault();
        hostname = map.get("Output_host").toString();
        port = (int) map.get("Output_port");

    }
    @Override
    public void execute(Tuple tuple)
    {
        URIBuilder builder = new URIBuilder();
        try
        {
            UpdateContextRequestDocument contextRequestDocument = UpdateContextRequestDocument.Factory.newInstance();
            ContextElement contextElement = contextRequestDocument.addNewUpdateContextRequest().addNewContextElementList().addNewContextElement();
            ContextAttributeList attributeList = contextElement.addNewContextAttributeList();
            for(String field: tuple.getFields())
            {
                ContextAttribute contextAttribute = attributeList.addNewContextAttribute();
                contextAttribute.setName(field);
                contextAttribute.setType(field);
            }
            
            
            URI uri = builder.setHost(hostname).setPort(port).setPath("/NGSI10/updateContext").build();
            HttpPost httpost = new HttpPost(uri);
            httpost.setHeader("Content-Type", ContentType.APPLICATION_XML.getMimeType());
            HttpResponse response = httpclient.execute(httpost);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200)
            {
                LoggerFactory.getLogger(this.getClass()).info(EntityUtils.toString(response.getEntity()).toString());
            }
            
        }
        catch (URISyntaxException | IOException ex)
        {
            Logger.getLogger(ReportingBolt.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
