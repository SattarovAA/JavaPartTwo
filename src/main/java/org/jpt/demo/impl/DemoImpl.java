package org.jpt.demo.impl;

import lombok.extern.slf4j.Slf4j;
import org.jpt.client.Client;
import org.jpt.demo.Demo;
import org.jpt.server.Server;

@Slf4j
public class DemoImpl implements Demo {
    private static final int PORT = 7788;
    private static final String HOST = "localhost";

    @Override
    public void run() {
        log.info("Demo starts!");
        Thread serverThread = new Thread(new Server(PORT));
        Thread clientThread = new Thread(new Client(HOST, PORT));

        serverThread.start();
        clientThread.start();
    }
}
