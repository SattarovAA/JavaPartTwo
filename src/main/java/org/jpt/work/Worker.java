package org.jpt.work;

import org.jpt.demo.Demo;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Создает два потока, которые по очереди будут выводить числа.
 * Первый поток чётные числа, второй потом нечётные.
 */
public class Worker implements Demo {
    @Override
    public void run() {
        AtomicInteger counter = new AtomicInteger(0);
        Thread threadOne = new Thread(new Printer(counter, 0), "Thread-Even");
        Thread threadTwo = new Thread(new Printer(counter, 1), "Thread-Odd");

        threadOne.start();
        threadTwo.start();
    }
}
