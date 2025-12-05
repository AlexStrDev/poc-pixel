package com.axonkafka.starter.lock;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@Service
public class DistributedLockService {

    private final Map<String, Lock> locks;

    public DistributedLockService() {
        this.locks = new ConcurrentHashMap<>();
    }

    public boolean tryLock(String key, long timeout, TimeUnit unit) {
        Lock lock = locks.computeIfAbsent(key, k -> new ReentrantLock());

        try {
            boolean acquired = lock.tryLock(timeout, unit);

            if (acquired) {
                log.debug("Lock adquirido: {}", key);
            } else {
                log.warn("Timeout esperando lock: {}", key);
            }

            return acquired;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupción esperando lock: {}", key, e);
            return false;
        }
    }

    public void unlock(String key) {
        Lock lock = locks.get(key);

        if (lock != null) {
            try {
                lock.unlock();
                log.debug("Lock liberado: {}", key);
            } catch (IllegalMonitorStateException e) {
                log.warn("Intento de liberar lock no adquirido: {}", key);
            }
        }
    }

    public boolean executeWithLock(String key, long timeout, TimeUnit unit, Runnable operation) {
        if (tryLock(key, timeout, unit)) {
            try {
                operation.run();
                return true;
            } finally {
                unlock(key);
            }
        }
        return false;
    }

    public <T> T executeWithLock(String key, long timeout, TimeUnit unit,
                                 java.util.function.Supplier<T> operation) {
        if (tryLock(key, timeout, unit)) {
            try {
                return operation.get();
            } finally {
                unlock(key);
            }
        }
        throw new LockAcquisitionException("No se pudo adquirir lock: " + key);
    }

    public Map<String, Object> getStats() {
        return Map.of(
                "activeLocks", locks.size()
        );
    }

    public void cleanup() {
        locks.clear();
    }

    public static class LockAcquisitionException extends RuntimeException {
        public LockAcquisitionException(String message) {
            super(message);
        }
    }
}