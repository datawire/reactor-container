package org.example.amqp;

/**
 * Created by jamesmartin on 5/5/15.
 */
public interface MessageListener {

    void setHandler(MessageHandler handler);

    MessageHandler getHandler();

}
