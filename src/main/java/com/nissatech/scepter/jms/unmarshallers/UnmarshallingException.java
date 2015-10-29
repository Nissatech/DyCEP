/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.nissatech.scepter.jms.unmarshallers;

/**
 *
 * @author aleksandar
 * Thrown in case of an unmarshalling failure.
 */
public class UnmarshallingException extends Exception
{
    private static final long serialVersionUID = 256977922913299701L;

    public UnmarshallingException()
    {
        super();
    }

    public UnmarshallingException(final String string, final Throwable thrwbl)
    {
        super(string, thrwbl);
    }

    
    public UnmarshallingException(String string)
    {
        super(string);
    }
    
    
}
