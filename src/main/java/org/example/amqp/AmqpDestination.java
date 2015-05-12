package org.example.amqp;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jamesmartin on 2/13/15.
 */
public class AmqpDestination {

    static Pattern ip_pattern = Pattern.compile("^amqp\\:\\/\\/(\\d+\\.\\d+\\.\\d+\\.\\d+)(\\:\\d+)?(\\/\\S+)?$");
    static Pattern hostname_pattern = Pattern.compile("^amqp\\:\\/\\/([\\w\\.\\-_]+)(\\:\\d+)?(\\/\\S+)?$");
    private String host;
    private int port;
    private String address;

    public AmqpDestination(String host, int port, String address) {
        this.host = host;
        this.port = port;
        this.address = address;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public static AmqpDestination parse(String url) {

        Matcher m = ip_pattern.matcher(url);
        if (!m.matches()) {
            m = hostname_pattern.matcher(url);
        }
        if (m.matches()) {
            String host = m.group(1);

            int port = 5672;
            int ngroups = m.groupCount();
            String destination = null;

            String g2 = m.group(2);
            if (g2 != null && g2.startsWith(":")) {
                port = Integer.parseInt(g2.substring(1));
            }

            String g3 = m.group(3);
            if (g3 != null && g3.startsWith("/")) {
                destination = g3.substring(1);
            }

            return new AmqpDestination(host, port, destination);
        } else {
            System.out.println("URL pattern match failed: "+url);
            return null;
        }
    }
}
