package org.jpt.server;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Получает данные и отправляет обратно.
 * Завершает работу при вводе команды {@link #STOP_MESSAGE}.
 */
@Slf4j
public class ClientEcho implements Runnable {
    /**
     * Сообщение для завершения работы.
     */
    private static final String STOP_MESSAGE = "!STOP";
    /**
     * Сокет для связи с клиентом.
     */
    private final Socket socket;

    public ClientEcho(Socket socket) {
        this.socket = socket;
    }

    /**
     * Получает данные и отправляет обратно.
     * Завершает работу при вводе команды {@link #STOP_MESSAGE}.
     */
    @Override
    public void run() {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            String message;
            while ((message = in.readLine()) != null) {
                log.info("Received message from client({}): {}",
                        socket.getInetAddress(),
                        message);
                if (message.equals(STOP_MESSAGE)) {
                    break;
                }
                out.println(message);
            }
            log.info("Client disconnected: {}", socket.getInetAddress());
        } catch (IOException e) {
            log.error("IOException in Echo.", e);
        }
    }
}
