package org.jpt;

import org.jpt.demo.Demo;
import org.jpt.demo.impl.DemoImpl;

public class Main {
    public static void main(String[] args) {
        Demo demo = new DemoImpl();
        demo.run();
    }
}