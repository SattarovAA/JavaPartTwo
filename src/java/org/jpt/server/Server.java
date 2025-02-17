package org.jpt.server;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Простой многопоточный TCP Echo Server.
 */
@Slf4j
public class Server implements Runnable {
    /**
     * Количество потоков в пуле по умолчанию.
     */
    private static final int DEFAULT_THREAD_AMOUNT = 2;
    /**
     * Порт сервера.
     */
    private final int port;
    /**
     * Пул для многопоточной обработки клиентов.
     */
    private final ExecutorService pool;

    public Server(int port) {
        this.port = port;
        this.pool = Executors.newFixedThreadPool(DEFAULT_THREAD_AMOUNT);
    }

    /**
     * Обрабатывает подключения в пуле потоков через {@link ClientEcho}.
     */
    public void run() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {

            log.info("Server is running on port: {}", port);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                log.info("New client with address: {}", clientSocket.getInetAddress());

                ClientEcho clientEcho = new ClientEcho(clientSocket);
                pool.execute(clientEcho);
            }
        } catch (IOException e) {
            log.error("IOException in Server.", e);
        }
    }
}
