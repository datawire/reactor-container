package org.example.amqp;

import java.io.IOException;

/**
 * The container will be initialized to listen on a host and port.
 * Created by jamesmartin on 5/5/15.
 */
public interface MessageContainer {

    /**
     * register a listener for a particular AMQP destination.
     * @param address
     * @return message listener for address
     */
    MessageListener createListener(String address);

    /**
     * The address here is a complete AMQP address of the form:
     * amqp://host:port/address
     * @param address
     * @return sender object for sending messages
     */
    MessageSender createSender(String address);

    void run() throws IOException;

}
