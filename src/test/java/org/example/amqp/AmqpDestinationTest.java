package org.example.amqp;

import junit.framework.Assert;
import org.junit.Test;

/**
 * Created by jamesmartin on 2/13/15.
 */
public class AmqpDestinationTest {

    @Test
    public void testDestination() {

        String url1 = "amqp://12.22.33.445:11111/whatever";

        AmqpDestination d1 = AmqpDestination.parse(url1);

        Assert.assertEquals("12.22.33.445", d1.getHost());
        Assert.assertEquals(11111, d1.getPort());
        Assert.assertEquals("whatever", d1.getAddress());

        String url2 = "amqp://12.22.33.445/whatever";
        AmqpDestination d2 = AmqpDestination.parse(url2);

        Assert.assertEquals("12.22.33.445", d2.getHost());
        Assert.assertEquals(5672, d2.getPort());
        Assert.assertEquals("whatever", d2.getAddress());

        String url3 = "amqp://12.22.33.445:5997";
        AmqpDestination d3 = AmqpDestination.parse(url3);

        Assert.assertEquals("12.22.33.445", d3.getHost());
        Assert.assertEquals(5997, d3.getPort());
        Assert.assertNull(d3.getAddress());

        String url4 = "amqp://my.silly_host.com:12345/bar";
        AmqpDestination d4 = AmqpDestination.parse(url4);
        Assert.assertEquals("my.silly_host.com", d4.getHost());
        Assert.assertEquals(12345, d4.getPort());
        Assert.assertEquals("bar", d4.getAddress());

        String url5 = "amqp://localhost:12345/help-me";
        AmqpDestination d5 = AmqpDestination.parse(url5);
        Assert.assertEquals("localhost", d5.getHost());
        Assert.assertEquals(12345, d5.getPort());
        Assert.assertEquals("help-me", d5.getAddress());
    }
}
