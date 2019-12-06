package ru.dmzadorin.demo.model;

public class DbTemporaryUnavailable extends RuntimeException {
    public DbTemporaryUnavailable(Throwable throwable) {
        super("Database temporary unavailable", throwable, false, false);
    }
}
