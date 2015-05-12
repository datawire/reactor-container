package org.example.test.disruptor;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.apache.qpid.proton.message.Message;
import org.example.amqp.MessageHandler;

/**
 * Created by jamesmartin on 4/23/15.
 */
public class LmaxMessageProcessor implements MessageHandler {

    RingBuffer<MessageEvent> ringBuffer;

    public LmaxMessageProcessor(Disruptor<MessageEvent> distruptor) {
        this.ringBuffer = distruptor.getRingBuffer();
    }
    @Override
    public void handleMessage (Message message) {
        long sequence = ringBuffer.next();  // Grab the next sequence
        try
        {
            MessageEvent event = ringBuffer.get(sequence); // Get the entry in the Disruptor
            // for the sequence
            event.setMessage(message);  // Fill with data
        }
        finally
        {
            ringBuffer.publish(sequence);
        }
    }
}
