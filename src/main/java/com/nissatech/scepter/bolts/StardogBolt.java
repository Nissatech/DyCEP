package com.nissatech.scepter.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.complexible.stardog.api.ConnectionConfiguration;
import com.complexible.stardog.sesame.StardogRepository;
import com.nissatech.scepter.StormObject;

import java.util.Map;
import org.openrdf.model.Model;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.slf4j.LoggerFactory;

/**
 * Bolt handling Stardog database writes.
 * Receives the tuple containing a {@link org.openrdf.model.Model} object under the field name "model" which is stored into the database
 * @author aleksandar
 */
public class StardogBolt extends BaseRichBolt implements StormObject
{
    private static final long serialVersionUID = 6949632091976366931L;

    protected transient Repository repo;
    private final String id;
    private transient RepositoryConnection connection = null;

    /**
     * 
     * @param id String identifier of the bolt instance
     */
    public StardogBolt(String id)
    {
        super();
        this.id = id;
    }

    /**
     * This function does not emit anything.
     * @param ofd 
     */
    public void declareOutputFields(OutputFieldsDeclarer ofd)
    {
        //  throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void prepare(Map map, TopologyContext tc, OutputCollector oc)
    {
         repo = new StardogRepository(ConnectionConfiguration
                    .from("snarl://192.168.0.114:5820/myDB")
                    .credentials("admin", "admin"));
         try {
             repo.initialize();
             connection = repo.getConnection();
         }
         catch (RepositoryException ex) {
             LoggerFactory.getLogger(this.getClass().getName()).error(ex.toString());
         }

    }

    public void execute(Tuple tuple)
    {
        Model m = (Model) tuple.getValueByField("model");
        if(m != null)
        {
            try {        
                
                connection.begin();
                connection.add(m);
                connection.commit();
            }
            catch (RepositoryException ex) {
                LoggerFactory.getLogger(this.getClass().getName()).error(ex.toString());
            }
        }
        
    }

    /**
     * 
     * @return Identifier of the Storm component
     */
    public String getId()
    {
        return this.id;
    }

    @Override
    public void cleanup()
    {
        super.cleanup(); 
        try {
            this.connection.close();
        }
        catch (RepositoryException ex) {
            LoggerFactory.getLogger(this.getClass().getName()).error(ex.toString());
        }
        
    }
    
    
}
