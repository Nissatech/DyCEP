package com.nissatech.scepter.bolts.comparator.executor;

import backtype.storm.tuple.Tuple;
import com.nissatech.scepter.bolts.comparator.operators.CompareOperation;
import java.io.Serializable;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;

/**
 *
 * @author aleksandar
 */
public class BindingTupleComparator implements Serializable
{
    private static final long serialVersionUID = -870550835719724373L;

    private enum Link
    {
        AND, OR, NONE;
    }
    private BindingTupleComparator next;
    private final CompareOperation operation;
    private Link link;
    String dbBinding;
    String tupleBinding;
    public BindingTupleComparator(String dbBinding, String tupleBinding, CompareOperation operation)
    {
        this.operation = operation;
        this.link = Link.NONE;
        this.dbBinding = dbBinding;
        this.tupleBinding = tupleBinding; 

    }

    public BindingTupleComparator and(BindingTupleComparator and)
    {
        link = Link.AND;
        this.next = and;
        return this;
    }

    public BindingTupleComparator or(BindingTupleComparator or)
    {
        link = Link.OR;
        this.next = or;
        return this;
    }

    public boolean eval(BindingSet bindingSet, Tuple inputTuple)
    {
        Value first = bindingSet.getValue(this.dbBinding);
        Object second = inputTuple.getValueByField(this.tupleBinding);
        if(first == null || second == null) return false;
        switch (this.link) {
            case AND:
                return operation.execute(first, second) && next.eval(bindingSet, inputTuple);
            case OR:
                return operation.execute(first, second) || next.eval(bindingSet, inputTuple);
        }
        return operation.execute(first, second);
   
    }

}
