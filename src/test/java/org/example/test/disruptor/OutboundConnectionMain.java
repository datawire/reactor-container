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
package org.example.test.disruptor;

import com.lmax.disruptor.dsl.Disruptor;
import org.example.amqp.MessageListener;
import org.example.amqp.MessageSender;
import org.example.amqp.impl.ReactorContainer;

import java.util.concurrent.Executors;

public class OutboundConnectionMain {


    public static void main(String[] argv) throws Exception {
        Disruptor<MessageEvent> disruptor = new Disruptor<MessageEvent>(new MessageEventFactory(), 1024, Executors.newCachedThreadPool());

        ReactorContainer container = new ReactorContainer(null, 0);
        container.init();

        MessageListener listener = container.createListener("amqp://0.0.0.0:5672/foo");
        listener.setHandler(new LmaxMessageProcessor(disruptor));

        MessageSender sender = container.createSender("amqp://0.0.0.0:5672/bar");

        disruptor.handleEventsWith(new MessageEventHandler(sender));
        disruptor.start();

        // this blocks for now
        container.run();
    }
}
