package org.example.amqp;

import org.apache.qpid.proton.message.Message;

/**
 * Created by jamesmartin on 5/5/15.
 */
public interface MessageSender {

    void send(Message message);

}
