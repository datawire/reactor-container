package org.example.test.disruptor;

import org.apache.qpid.proton.message.Message;

/**
 * Created by jamesmartin on 4/23/15.
 */
public class MessageEvent {
    Message message;

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }
}
