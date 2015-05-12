/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.example.amqp.impl;

import org.apache.qpid.proton.engine.*;
import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.message.MessageFormat;
import org.example.amqp.MessageHandler;
import org.example.amqp.MessageListener;



public class ProtonMessageListener extends BaseHandler implements MessageListener {

    int received;
    private MessageHandler handler;
    private String address;
    private String hostname = null;
    private Receiver receiver = null;

    ProtonMessageListener(String address) {
        //this.receiver = receiver;
        this.address = address;
    }

    ProtonMessageListener(String hostname, String address) {
        //this.receiver = receiver;
        this.address = address;
        this.hostname = hostname;
    }

    @Override
    public void onDelivery(Event evt) {
        Delivery dlv = evt.getDelivery();
        if (dlv.getLink() instanceof Receiver) {
            Receiver receiver = (Receiver) dlv.getLink();
            if (!dlv.isPartial()) {
                byte[] bytes = new byte[dlv.pending()];
                receiver.recv(bytes, 0, bytes.length);

                Message message = Message.Factory.create();
                message.setMessageFormat(MessageFormat.AMQP);
                message.decode(bytes, 0, bytes.length);

                this.handler.handleMessage(message);

                received++;
                dlv.settle();
            }

        }
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
    public void onConnectionUnbound(Event evt) {
        evt.getConnection().free();
    }

    @Override
    public void onLinkFinal(Event evt) {
        if (evt.getLink() instanceof Receiver) {
            System.out.println(String.format("Total received: %s", received));
        }
    }

    @Override
    public void onLinkLocalOpen(Event e) {
        if (e.getLink() instanceof Receiver) {
            Receiver receiver = (Receiver) e.getLink();
            System.out.println("RECEIVER OPEN: "+receiver.getSource().getAddress());
            if (receiver.getSource().getAddress().equals(this.address)) {
                this.receiver = receiver;

            }
        }

    }

    @Override
    public void onLinkLocalClose(Event e) {
        if (this.receiver != null && e.getLink() == this.receiver) {
            this.receiver = null;
            System.out.println(">>>> nulling receiver");
        }
    }
    public MessageHandler getHandler() {
        return handler;
    }

    public void setHandler(MessageHandler handler) {
        this.handler = handler;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }
}
