package org.example.amqp.impl;

import org.apache.qpid.proton.engine.*;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.reactor.Reactor;
import org.example.amqp.MessageSender;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Created by jamesmartin on 5/5/15.
 */
public class ProtonMessageSender extends BaseHandler implements MessageSender {

    private Deque<Message> messages = new ArrayDeque<>();

    private  int tag = 0;

    public static int MAX_SIZE = 10240;

    private int sent = 0;

    private String address;
    private String hostname = null;
    private Reactor reactor;
    private Sender sender ;

    ProtonMessageSender(String address, Reactor reactor) {
        this.address = address;
        this.reactor = reactor;
    }

    ProtonMessageSender(String hostname, String address, Reactor reactor) {
        this.hostname = hostname;
        this.address = address;
        this.reactor = reactor;
    }

    private byte[] nextTag() {
        return String.format("%s", tag++).getBytes();
    }

    @Override
    public void send(Message message) {
        synchronized (messages) {
            this.messages.add(message);
        }
        try {
            reactor.wakeup();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void _sendInternal(Sender sender) {
        // not sure why 1024 is used for queued messages
        while (sender.getCredit() > 0 && sender.getQueued() < 1024) {
            Message message = null;
            synchronized (messages) {
                if (messages.peek() == null) {
                    sender.drained();
                    return;
                }
                message = this.messages.remove();
            }

            Delivery dlv = sender.delivery(nextTag());

            // ok this is really crappy. we need to interrogate the message to get its actual size
            byte[] bytes = new byte[MAX_SIZE];
            int actualSize = message.encode(bytes, 0, MAX_SIZE);
            sender.send(bytes, 0, actualSize);
            sender.advance();
            dlv.settle();
            sent++;

            //System.out.println(String.format("Sent message(%s): %s", sender.getTarget().getAddress(), message));
        }
    }

//    public void onLinkLocalOpen(Event e) {
//        if (e.getLink() instanceof Sender) {
//            Sender sender = (Sender) e.getLink();
//            this.senders.add(sender);
//        }
//    }

//    @Override
//    public void onLinkFinal(Event e) {
//        if (e.getLink() instanceof Sender) {
//            Sender sender = (Sender) e.getLink();
//            this.senders.remove(sender);
//            System.out.println(String.format("Total sent: %s", sent));
//        }
//    }

    @Override
    public void onReactorQuiesced(Event e) {
        if (this.sender != null)
            _sendInternal(sender);
    }

    @Override
    public void onLinkFlow(Event e) {
        if (e.getLink() instanceof Sender) {
            this._sendInternal((Sender) e.getLink());
        }
    }


    @Override
    public void onDelivery(Event evt) {
        Delivery dlv = evt.getDelivery();
        if (dlv.remotelySettled()) {
            //System.out.println(String.format("Settled %s: %s", new String(dlv.getTag()), dlv.getRemoteState()));
            dlv.settle();
        }

    }

    @Override
    public void onConnectionUnbound(Event evt) {
        evt.getConnection().free();
    }

    @Override
    public void onConnectionInit(Event e) {
        if (hostname != null) {
            try {
                Connection conn = e.getConnection();
                conn.setHostname(hostname);

                // Every session or link could have their own handler(s) if we
                // wanted simply by adding the handler to the given session
                // or link
                Session ssn = conn.session();

                // If a link doesn't have an event handler, the events go to
                // its parent session. If the session doesn't have a handler
                // the events go to its parent connection. If the connection
                // doesn't have a handler, the events go to the reactor.
                Sender snd = ssn.sender(this.address);
                conn.open();
                ssn.open();
                snd.open();
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
    }

    @Override
    public void onReactorInit(Event e) {
        if (hostname != null) {
            e.getReactor().connection(this);
        }
    }

    @Override
    public void onTransportError(Event e) {
        System.out.println("transport error");
    }

    public void onLinkLocalOpen(Event evt) {
        if (evt.getLink() instanceof Sender) {

            Sender sender = (Sender) evt.getLink();
            System.out.println("SENDER OPEN: "+sender.getTarget().getAddress());
            if (sender.getTarget().getAddress().equals(this.address)) {
                this.sender = sender;
            }
        }
    }

    @Override
    public void onLinkLocalClose(Event e) {
        if (e.getLink() instanceof Sender) {

            if (this.sender != null && e.getLink() == this.sender)
                this.sender = null;
        }
    }
}
