package org.example.amqp.impl;

import org.apache.qpid.proton.reactor.Reactor;
import org.example.amqp.AmqpDestination;
import org.example.amqp.MessageContainer;
import org.example.amqp.MessageListener;
import org.example.amqp.MessageSender;

import java.io.IOException;

/**
 * Created by jamesmartin on 5/5/15.
 */
public class ReactorContainer implements MessageContainer {

    public static String DEFAULT_HOST = "0.0.0.0";
    public static int DEFAULT_PORT = 5672;

    Reactor reactor;
    String host;
    int port;

    public ReactorContainer(String host, int port) {
        try {
            reactor = Reactor.Factory.create();
            reactor.getHandler().add(new FlowController(1024));
            reactor.getHandler().add(new Handshaker());
            this.host = host;
            this.port = port;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MessageListener createListener(String address) {
        ProtonMessageListener listener ;

        if (host == null) {
            AmqpDestination destination = AmqpDestination.parse(address);
            // if the address is not a full amqp address
            if (destination == null) {
                destination.setHost("localhost");
                destination.setPort(5672);
                destination.setAddress(address);
            }
            listener = new ProtonMessageListener(destination.getHost()+":"+destination.getPort(), destination.getAddress());
        } else {
            listener = new ProtonMessageListener(address);
        }
        reactor.getHandler().add(listener);

        return listener;
    }

    @Override
    public MessageSender createSender(String address) {
        ProtonMessageSender messageSender = null;

        if (host == null) {
            AmqpDestination destination = AmqpDestination.parse(address);
            // if the address is not a full amqp address
            if (destination == null) {
                destination.setHost("localhost");
                destination.setPort(5672);
                destination.setAddress(address);
            }
            messageSender = new ProtonMessageSender(destination.getHost()+":"+destination.getPort(), address, reactor);
        } else {
            messageSender = new ProtonMessageSender(address, reactor);
            reactor.getHandler().add(messageSender);
        }
        return messageSender;
    }

    public void init() throws IOException {
        if (this.host != null) {
            reactor.acceptor(host, port);
        }
    }

    public void run() {
        reactor.run();
    }
}
