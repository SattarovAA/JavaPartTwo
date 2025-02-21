package org.jpt.client;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Клиент для обмена сообщениями с сервером.
 * Работает с вводом сообщений из {@code System.in}.
 * Завершает работу при вводе команды {@link #STOP_MESSAGE}.
 */
@Slf4j
public class Client implements Runnable {
    /**
     * Сообщение для завершения работы.
     */
    private static final String STOP_MESSAGE = "!STOP";
    /**
     * Хост сервера.
     */
    private final String host;
    /**
     * Порт сервера.
     */
    private final int port;

    public Client(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Обмен сообщений с сервером.
     * Работает с вводом сообщений из {@code System.in}.
     * Завершает работу при вводе команды {@link #STOP_MESSAGE}.
     */
    public void run() {
        try (Socket socket = new Socket(host, port);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader input = new BufferedReader(new InputStreamReader(System.in))) {

            String message;
            log.info("Connected to server! Write your message:");

            while ((message = input.readLine()) != null) {
                out.println(message);
                if (message.equals(STOP_MESSAGE)) {
                    break;
                }
                log.info(in.readLine());
            }

            log.info("Client disconnected.");
        } catch (IOException e) {
            log.error("IOException in Client", e);
        }
    }
}
