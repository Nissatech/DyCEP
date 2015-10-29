//package com.nissatech.scepter.bolts;
//
//import backtype.storm.task.TopologyContext;
//import backtype.storm.topology.BasicOutputCollector;
//import backtype.storm.topology.OutputFieldsDeclarer;
//import backtype.storm.topology.base.BaseBasicBolt;
//import backtype.storm.tuple.Tuple;
//import com.basho.riak.client.IRiakClient;
//import com.basho.riak.client.RiakException;
//import com.basho.riak.client.RiakFactory;
//import com.nissatech.gcmhelper.exceptions.GcmPostException;
//import com.nissatech.gcmhelper.messages.GcmPostMessage;
//import com.nissatech.gcmhelper.messages.GcmResponseMessage;
//import com.nissatech.gcmhelper.senders.GcmHttpSender;
//import com.nissatech.gcmriakpersistance.gcmriakpersistance.UserLibrary;
//
//import com.nissatech.scepter.StormObject;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.ExecutionException;
//import java.util.logging.Level;
//import java.util.logging.Logger;
//import org.slf4j.LoggerFactory;
//
///**
// *
// * @author aleksandar
// */
//public class GCMPusherBolt extends BaseBasicBolt implements StormObject
//{
//    private static final long serialVersionUID = -3150901567169931687L;
//
//    private String hostname;
//    private long port;
//    private final String id;
//    private transient IRiakClient riakProtoBufferClient;
//
//    public GCMPusherBolt(String id)
//    {
//        this.id = id;
//    }
//
//    
//    @Override
//    public void prepare(Map stormConf, TopologyContext context)
//    {
//        super.prepare(stormConf, context);          //To change body of generated methods, choose Tools | Templates.
//        this.hostname=stormConf.get("riakHostname").toString();
//        this.port=(Long)stormConf.get("riakPort");
//    }
//    
//    public void declareOutputFields(OutputFieldsDeclarer declarer)
//    {
//        //nothing to declare
//    }
//
//    public void execute(Tuple input, BasicOutputCollector collector)
//    {
//        try 
//        {
//            if(input.contains("patientId"))
//            {
//                String patientId = input.getStringByField("patientId");
//                riakProtoBufferClient = RiakFactory.pbcClient(this.hostname, (int) this.port);
//                UserLibrary library = new UserLibrary(riakProtoBufferClient);
//                List<String> gcmIds = library.getGcmIds(patientId);
//                if(gcmIds.isEmpty())
//                    return;
//
//                GcmHttpSender sender = new GcmHttpSender("AIzaSyB3hcawKhFJxTCXBNdC-3wtzwrcTWtsiDY");
//                GcmPostMessage message = new GcmPostMessage(gcmIds);
//                message.addData("payload", "Please slow down");
//                GcmResponseMessage response = sender.send(message);  
//                LoggerFactory.getLogger(this.getClass().getName()).info("GCM Response:"+response.getSuccess().toString());
//                
//            }
//        }
//        catch (RiakException ex) {
//            LoggerFactory.getLogger(this.getClass().getName()).error(ex.toString());
//        }
//        catch (GcmPostException ex) {
//            LoggerFactory.getLogger(this.getClass().getName()).error(ex.toString());
//        }
//        catch (InterruptedException ex)
//        {
//            Logger.getLogger(GCMPusherBolt.class.getName()).log(Level.SEVERE, null, ex);
//        }
//        catch (ExecutionException ex)
//        {
//            Logger.getLogger(GCMPusherBolt.class.getName()).log(Level.SEVERE, null, ex);
//        }
//        finally
//        {
//            if(this.riakProtoBufferClient != null)
//                this.riakProtoBufferClient.shutdown();
//        }
//    }
//
//    public String getId()
//    {
//        return this.id;
//    }
//    
//}
