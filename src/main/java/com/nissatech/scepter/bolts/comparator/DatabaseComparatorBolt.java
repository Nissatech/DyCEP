package com.nissatech.scepter.bolts.comparator;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import static com.complexible.common.rdf.query.parser.sparql.BS.QueryLanguage;
import com.complexible.stardog.api.ConnectionConfiguration;
import com.complexible.stardog.sesame.StardogRepository;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.nissatech.scepter.StormObject;

import com.nissatech.scepter.bolts.comparator.executor.BindingTupleComparator;
import com.nissatech.scepter.bolts.comparator.util.BindingAdapter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.slf4j.LoggerFactory;

/**
 *
 * @author aleksandar
 */
public class DatabaseComparatorBolt extends BaseRichBolt implements StormObject
{

    private static final long serialVersionUID = 2556536714566638042L;

    private String id;
    private String query;
    protected transient Repository repo;
    private transient RepositoryConnection connection = null;
    Map<String, Object> literalSPARQLBinidings;
    Map<String, String> stormTupleSPARQLBindings;
    ListMultimap<BindingTupleComparator, DBComparatorOutputStream> emitions;
    transient OutputCollector output;

    public DatabaseComparatorBolt(String id)
    {
        this.id = id;
        this.literalSPARQLBinidings = new HashMap<String, Object>();
        this.stormTupleSPARQLBindings = new HashMap<String, String>();
        this.emitions = ArrayListMultimap.create();
    }

    protected static abstract class AbstractBuilder
    {

        protected final DatabaseComparatorBolt bolt;

        public AbstractBuilder(String id)
        {
            this(new DatabaseComparatorBolt(id));
        }

        protected AbstractBuilder(final DatabaseComparatorBolt bolt)
        {
            this.bolt = bolt;
        }

    }

    protected static abstract class AbstractBindingBuilder extends AbstractBuilder
    {

        public AbstractBindingBuilder(DatabaseComparatorBolt bolt)
        {
            super(bolt);
        }

        public ComparisonBuilder comparisons()
        {
            return new ComparisonBuilder(bolt);
        }

    }

    public static class Builder extends AbstractBuilder
    {

        public Builder(String id)
        {
            super(id);
        }

        public TupleQueryBuilder tupleQuery(String query)
        {
            bolt.setQuery(query);
            return new TupleQueryBuilder(bolt);
        }

    }

    public static class ComparisonBuilder extends AbstractBuilder
    {

        public ComparisonBuilder(DatabaseComparatorBolt bolt)
        {
            super(bolt);
        }

        public ComparisonResultBuilder comparison(BindingTupleComparator comparator)
        {
            return new ComparisonResultBuilder(bolt, comparator);
        }
        public DatabaseComparatorBolt build()
        {
            return bolt;
        }

    }

    public static class TupleQueryBuilder extends AbstractBindingBuilder
    {

        public TupleQueryBuilder(DatabaseComparatorBolt bolt)
        {
            super(bolt);
        }

        public TupleQueryBuilder bindLiteral(String key, Object literal)
        {
            bolt.literalSPARQLBinidings.put(key, literal);
            return new TupleQueryBuilder(bolt);
        }

        public TupleQueryBuilder bindFromTuple(String key, String tuple)
        {
            bolt.stormTupleSPARQLBindings.put(key, tuple);
            return new TupleQueryBuilder(bolt);
        }

    }

    public static class ComparisonResultTupleBuilder extends AbstractBuilder
    {

        String stream;
        BindingTupleComparator comparator;
        List<DBComparatorOutputLiteral> tupleList;
        List<String> tuplesForward;
        public ComparisonResultTupleBuilder(DatabaseComparatorBolt bolt, BindingTupleComparator comparator, String stream)
        {
            super(bolt);
            this.stream = stream;
            this.comparator=comparator;
            this.tupleList=new ArrayList<DBComparatorOutputLiteral>();
            this.tuplesForward = new ArrayList<String>();
        }
        public ComparisonResultTupleBuilder forwardTuple(String string)
        {
            this.tuplesForward.add(string);
            return this;
        }
        public ComparisonResultTupleBuilder emitLiteral(String tuple, Object value)
        {
            this.tupleList.add(new DBComparatorOutputLiteral(tuple, value));
            return this;
        }
        
        public ComparisonResultTupleBuilder emitToStream(String stream)
        {
            deployEmitions();
            return new ComparisonResultTupleBuilder(bolt,comparator,stream);
        }
        public DatabaseComparatorBolt build()
        {
            deployEmitions();
            return bolt;
        }
        private void deployEmitions()
        {
            bolt.emitions.put(comparator, new DBComparatorOutputStream(this.stream, tupleList,this.tuplesForward));
        }
          
    }
    public static class ComparisonResultBuilder extends AbstractBuilder
    {

        BindingTupleComparator comparator;

        public ComparisonResultBuilder(DatabaseComparatorBolt bolt, BindingTupleComparator comparator)
        {
            super(bolt);
            this.comparator = comparator;
        }

        public ComparisonResultTupleBuilder emitToStream(String stream)
        {
            //bolt.emitions.put(comparator, new DBComparatorOutputStream(stream));
            return new ComparisonResultTupleBuilder(bolt,comparator,stream);
        }

        public ComparisonResultTupleBuilder emitToDefaultStream()
        {
            return new ComparisonResultTupleBuilder(bolt,comparator,"default");
        }
        
        public DatabaseComparatorBolt build()
        {
            return bolt;
        }

    }
   
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        for (DBComparatorOutputStream stream : this.emitions.values()) {
            declarer.declareStream(stream.getStreamName(), new Fields(stream.getTupleNames()));
            
        }
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {

        repo = new StardogRepository(ConnectionConfiguration
                .from("snarl://192.168.0.114:5820/myDB")
                .credentials("admin", "admin"));
        try {
            repo.initialize();
            //connection = repo.getConnection();
            this.output = collector;
        }
        catch (RepositoryException ex) {
            LoggerFactory.getLogger(this.getClass().getName()).error(ex.toString());
        }
    }

    @Override
    public void cleanup()
    {
        try {
            super.cleanup();
            if(connection!= null && connection.isOpen()) this.connection.close();
            repo.shutDown();
            
        }
        catch (RepositoryException ex) {
            LoggerFactory.getLogger(this.getClass().getName()).error(ex.toString());
        }
    }

    
    public void execute(Tuple input)
    {
        try {
            this.connection=repo.getConnection();
            ValueFactory valueFactory = repo.getValueFactory();
            TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL, query);
            for (Entry entry : this.literalSPARQLBinidings.entrySet()) {
                BindingAdapter.SparqlBinding adapted = BindingAdapter.adapt(entry.getValue());
                tupleQuery.setBinding(entry.getKey().toString(), valueFactory.createLiteral(adapted.getValue(), adapted.getURI()));
            }
            for (Entry entry : this.stormTupleSPARQLBindings.entrySet()) {
                Object tupleValue = input.getValueByField(entry.getValue().toString());
                BindingAdapter.SparqlBinding adapted = BindingAdapter.adapt(tupleValue);
                tupleQuery.setBinding(entry.getKey().toString(), valueFactory.createLiteral(adapted.getValue(), adapted.getURI()));
            }

            TupleQueryResult result = tupleQuery.evaluate();

            while (result.hasNext()) {
                BindingSet bindingSet = result.next();
                for (BindingTupleComparator comparator : this.emitions.keySet()) {
                    boolean eval = comparator.eval(bindingSet, input);
                    if (eval == true) {
                        List<DBComparatorOutputStream> outputStreams = this.emitions.get(comparator);
                        for(DBComparatorOutputStream emitionStream : outputStreams)
                        {
                           List<Object> valuesToSend = emitionStream.getLiteralValues();
                           for(String field : emitionStream.getForwardingTuples())
                               valuesToSend.add(input.getValueByField(field));
                           
                           output.emit(emitionStream.getStreamName(),valuesToSend);
                        }
                            
                    }
                }
            }

        }
        catch (RepositoryException ex) {
            LoggerFactory.getLogger(this.getClass().getName()).error(ex.toString());
        }
        catch (MalformedQueryException ex) {
            LoggerFactory.getLogger(this.getClass().getName()).error(ex.toString());
        }
        catch (QueryEvaluationException ex) {
            LoggerFactory.getLogger(this.getClass().getName()).error(ex.toString());
        }
        finally
        {
            try {
                connection.close();
            }
            catch (RepositoryException ex) {
                LoggerFactory.getLogger(this.getClass().getName()).error(ex.toString());
            }
        }
    }

    public String getId()
    {
        return this.id;
    }

    public String getQuery()
    {
        return query;
    }

    public void setQuery(String query)
    {
        this.query = query;
    }
}
