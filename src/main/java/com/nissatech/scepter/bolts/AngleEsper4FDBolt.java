package com.nissatech.scepter.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.nissatech.fitman.fitmaneventtypes.AngleSimpleEvent;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.LoggerFactory;

/**
 *
 * @author aleksandar
 */
public class AngleEsper4FDBolt extends BaseRichBolt
{

    private static final long serialVersionUID = 3350900988865977355L;
    private transient HttpClient httpclient;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd)
    {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc)
    {
        httpclient = HttpClients.createDefault();
        String host = map.get("E4FD_host").toString();
        int port = (int) map.get("E4FD_port");
        URIBuilder builder = new URIBuilder();
        try
        {
            URI uri = builder.setHost(host).setPort(port).setPath("/cep/instance").build();
            HttpPost httpost = new HttpPost(uri);
            HttpResponse response = httpclient.execute(httpost);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 409)
            {
                LoggerFactory.getLogger(this.getClass()).info(EntityUtils.toString(response.getEntity()).toString());
            }
            else if (statusCode == 200)
            {
                registerEvents(httpclient, builder);
                registerPatterns(httpclient, builder);
            }
        }
        catch (IOException | URISyntaxException ex)
        {
            LoggerFactory.getLogger(this.getClass()).error(ex.toString());
        }
    }

    @Override
    public void execute(Tuple tuple)
    {
        AngleSimpleEvent angle = (AngleSimpleEvent) tuple.getValueByField("angleEvent");
        HttpPost httpost = new HttpPost("http://localhost");

    }

    private void registerEvents(HttpClient httpclient, URIBuilder builder) throws URISyntaxException
    {
        URI uri = builder.setPath("/cep/instance").build();
        HttpPost httpost = new HttpPost(uri);
    }

    private void registerPatterns(HttpClient httpclient, URIBuilder builder)
    {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
