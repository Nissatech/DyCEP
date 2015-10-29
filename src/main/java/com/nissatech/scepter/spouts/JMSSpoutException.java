package com.nissatech.scepter.spouts;

/**
 *
 * @author aleksandar
 */
public class JMSSpoutException extends Exception
{
    private static final long serialVersionUID = -764351880816083417L;

    public JMSSpoutException(String string)
    {
        super(string);
    }

    public JMSSpoutException(String string, Throwable thrwbl)
    {
        super(string, thrwbl);
    }

    public JMSSpoutException(Throwable thrwbl)
    {
        super(thrwbl);
    }
    
}
