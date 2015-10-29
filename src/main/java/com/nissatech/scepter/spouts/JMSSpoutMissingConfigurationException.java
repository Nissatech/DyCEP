/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.nissatech.scepter.spouts;

/**
 *
 * @author aleksandar
 */
public class JMSSpoutMissingConfigurationException extends JMSSpoutException
{

    public JMSSpoutMissingConfigurationException(String string)
    {
        super(string);
    }

    public JMSSpoutMissingConfigurationException(String string, Throwable thrwbl)
    {
        super(string, thrwbl);
    }

    public JMSSpoutMissingConfigurationException(Throwable thrwbl)
    {
        super(thrwbl);
    }
    
}
