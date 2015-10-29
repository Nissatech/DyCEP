package com.nissatech.scepter.bolts.comparator.operators;

import java.io.Serializable;
import org.openrdf.model.Value;

/**
 *
 * @author aleksandar
 */
public interface CompareOperation extends Serializable{

    public boolean execute(Value x, Object y);
}
