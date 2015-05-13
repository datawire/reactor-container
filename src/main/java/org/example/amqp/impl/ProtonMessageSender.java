package org.example.amqp.impl;

import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.engine.*;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.reactor.Reactor;
import org.example.amqp.MessageSender;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

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
    private Set<Sender> senders = new HashSet<Sender>();

    ProtonMessageSender(String address, Reactor reactor) {
        this.address = address;
        this.reactor = reactor;
    }

    ProtonMessageSender(String hostname, String address, Reactor reactor) {
        this.hostname = hostname;
        this.address = address;
        this.reactor = reactor;
        //this.add(new Handshaker());
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
        while (sender.getCredit() > 0) {
            Message message = null;
            synchronized (messages) {
                if (messages.peek() == null) {
                    //sender.drained();
                    return;
                }
                message = this.messages.remove();
            }

            System.out.println("sendInternal");

            Delivery dlv = sender.delivery(nextTag());

            // ok this is really crappy. we need to interrogate the message to get its actual size
            byte[] bytes = new byte[MAX_SIZE];
            int actualSize = message.encode(bytes, 0, MAX_SIZE);

            sender.send(bytes, 0, actualSize);
            sender.advance();
            //dlv.settle();
            sent++;

            //System.out.println(String.format("Sent message(%s): %s", sender.getTarget().getAddress(), message));
        }
    }


    @Override
    public void onLinkFinal(Event e) {
        if (e.getLink() instanceof Sender) {
            Sender sender = (Sender) e.getLink();
            this.senders.remove(sender);
            System.out.println(String.format("Total sent: %s", sent));
        }
    }

    @Override
    public void onReactorQuiesced(Event e) {
        for (Sender sender : senders) {
            _sendInternal(sender);
        }
    }

    @Override
    public void onLinkFlow(Event e) {
        if (e.getLink() instanceof Sender && senders.contains(e.getLink())) {
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

                Session ssn = conn.session();

                Sender snd = ssn.sender(this.address+"-//"+this.hostname);
                Source src = new Source();
                src.setAddress("//"+this.hostname);
                snd.setSource(src);

                Target tgt = new Target();
                tgt.setAddress(this.address);
                snd.setTarget(tgt);

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
        System.out.println("transport error: "+e.getTransport().getCondition());
    }

    @Override
    public void onLinkLocalOpen(Event e) {
        if (e.getLink() instanceof Sender) {
            Sender sender = (Sender) e.getLink();

            if (sender.getTarget().getAddress().equals(this.address)) {
                System.out.println("SENDER OPEN: "+sender.getTarget().getAddress());
                this.senders.add(sender);
            }

        }
    }

    @Override
    public void onLinkLocalClose(Event e) {
        if (e.getLink() instanceof Sender) {

            if (this.senders.contains(e.getLink())) {
                this.senders.remove(e.getLink());
            }
        }
    }
}
