package com.nissatech.scepter.bolts.comparator.util;

import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.XMLSchema;

/**
 *
 * @author aleksandar
 */
public class BindingAdapter
{
    public static SparqlBinding adapt(Object o)
    {
        if(o instanceof DateTime)
        {
            return new SparqlBinding(ISODateTimeFormat.dateTime().print((DateTime) o), XMLSchema.DATETIME);
        }
        if(o instanceof Integer)
        {
            return new SparqlBinding(String.valueOf(o), XMLSchema.INTEGER);
        }
        if(o instanceof Double)
        {
            return new SparqlBinding(String.valueOf(o), XMLSchema.DOUBLE);
        }
        if(o instanceof String)
        {
            return new SparqlBinding(o.toString(), XMLSchema.STRING);
        }
        if(o instanceof Float)
        {
            return new SparqlBinding(o.toString(), XMLSchema.FLOAT);
        }
        if(o instanceof Short)
        {
            return new SparqlBinding(o.toString(), XMLSchema.SHORT);
        }
        return null;
    }
    public static class SparqlBinding
    {
        private String value;
        private URI type;
        public SparqlBinding(String value, URI type)
        {
            this.value=value;
            this.type=type;
        }     

        public String getValue()
        {
            return value;
        }

        public URI getURI()
        {
            return type;
        }
        
    }
}
