package com.distributed.gui;

/**
 * Launcher class to work around JavaFX module restrictions when using fat JARs.
 * JavaFX requires its modules to be on the module path, but with a shaded JAR
 * we need a plain non-Application main class to bootstrap.
 */
public class Launcher {
    public static void main(String[] args) {
        DashboardApp.main(args);
    }
}
