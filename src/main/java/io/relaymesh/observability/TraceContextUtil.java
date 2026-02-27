package io.relaymesh.observability;

import java.security.SecureRandom;

public final class TraceContextUtil {
    private static final SecureRandom RANDOM = new SecureRandom();

    private TraceContextUtil() {
    }

    public static String newTraceId() {
        return randomHex(16); // 16 bytes => 32 hex chars
    }

    public static String newSpanId() {
        return randomHex(8); // 8 bytes => 16 hex chars
    }

    public static String toTraceParent(String traceId, String spanId) {
        return "00-" + traceId + "-" + spanId + "-01";
    }

    private static String randomHex(int bytes) {
        byte[] value = new byte[bytes];
        RANDOM.nextBytes(value);
        StringBuilder sb = new StringBuilder(bytes * 2);
        for (byte b : value) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}

