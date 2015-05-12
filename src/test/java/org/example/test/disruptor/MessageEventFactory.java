package org.example.test.disruptor;

import com.lmax.disruptor.EventFactory;

/**
 * Created by jamesmartin on 4/23/15.
 */
public class MessageEventFactory implements EventFactory<MessageEvent> {
    @Override
    public MessageEvent newInstance() {
        return new MessageEvent();
    }
}
