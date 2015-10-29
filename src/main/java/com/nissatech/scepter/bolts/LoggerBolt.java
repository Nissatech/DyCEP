package com.nissatech.scepter.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.nissatech.scepter.StormObject;
import org.slf4j.LoggerFactory;

/**
 * A bolt subscribing to arbitrary streams and tuples and logs any of the subscribed fields if present. Used mainly for debugging purposes. 
 * @author aleksandar
 */
public class LoggerBolt extends BaseBasicBolt implements StormObject
{

    private static final long serialVersionUID = 6752411842782275219L;

    String[] fieldsToFollow;
    private String id;

    /**
     * Declares fields to be outputed. This function does not emit anything so this method is empty. 
     */
    public void declareOutputFields(OutputFieldsDeclarer ofd)
    {
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    /**
     * Constructor for the LoggerBolt
     * @param id String identifier of the bolt instance. 
     * @param field Array of Strings declaring fields that should be looked upon in the incoming tuples.
     */
    public LoggerBolt(String id, String... field)
    {
        super();
        this.fieldsToFollow = field;
        this.id = id;
    }

    public void execute(Tuple tuple, BasicOutputCollector boc)
    {
        for (String s : this.fieldsToFollow) {
            if (tuple.contains(s)) {
                Object o = tuple.getValueByField(s);
                if(o != null)
                    LoggerFactory.getLogger(this.getClass().getName()).info(o.toString());
            }
        }
    }

    /**
     * 
     * @return Returns the String identifier of this component. Can be used when building the Storm topology. 
     */
    public String getId()
    {
        return this.id;
    }

}
