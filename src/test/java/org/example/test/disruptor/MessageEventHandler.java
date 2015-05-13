package org.example.test.disruptor;

import com.lmax.disruptor.EventHandler;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.message.Message;
import org.example.amqp.MessageSender;

/**
 * Created by jamesmartin on 4/23/15.
 */
public class MessageEventHandler implements EventHandler<MessageEvent> {

    MessageSender sender;

    public MessageEventHandler(MessageSender sender) {
        this.sender = sender;
    }
    @Override
    public void onEvent(MessageEvent messageEvent, long l, boolean b) throws Exception {
        try {
            //System.out.println("MessageEventHandler.onEvent");
            Message message = messageEvent.getMessage();
            AmqpValue amqpValue = (AmqpValue)message.getBody();

            Message message1 = Message.Factory.create();

            message1.setBody(amqpValue);

            //System.out.println(String.format("Got message: %s, %s", amqpValue.getValue().toString(), l));

            if (sender != null)
                sender.send(message1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public MessageSender getSender() {
        return sender;
    }

    public void setSender(MessageSender sender) {
        this.sender = sender;
    }
}
