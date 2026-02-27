package io.relaymesh.config;

import java.nio.file.Path;
import java.nio.file.Paths;

public final class RelayMeshConfig {
    public static final String DEFAULT_NAMESPACE = "default";
    public static final String DEFAULT_NAMESPACES_DIR = "namespaces";
    public static final long DEFAULT_PAYLOAD_MAX_BYTES = 10L * 1024L * 1024L;
    public static final int DEFAULT_MAX_ATTEMPTS = 3;
    public static final long DEFAULT_BASE_BACKOFF_MS = 1_000L;
    public static final long DEFAULT_MAX_BACKOFF_MS = 60_000L;
    public static final long DEFAULT_LEASE_TIMEOUT_MS = 15_000L;
    public static final long DEFAULT_RECLAIM_SCAN_INTERVAL_MS = 5_000L;
    public static final int DEFAULT_RETRY_DISPATCH_LIMIT = 32;
    public static final int DEFAULT_THREAD_POOL_CORE = 4;
    public static final int DEFAULT_THREAD_POOL_MAX = 8;
    public static final int DEFAULT_QUEUE_CAPACITY = 100;
    public static final int DEFAULT_LLM_PARALLEL_LIMIT = 4;

    private final Path rootDir;
    private final Path rootBaseDir;
    private final String namespace;

    public RelayMeshConfig(Path rootDir, Path rootBaseDir, String namespace) {
        this.rootDir = rootDir;
        this.rootBaseDir = rootBaseDir;
        this.namespace = namespace;
    }

    public static RelayMeshConfig fromRoot(String root) {
        return fromRoot(root, DEFAULT_NAMESPACE);
    }

    public static RelayMeshConfig fromRoot(String root, String namespace) {
        Path resolved = root == null || root.isBlank()
                ? Paths.get("data")
                : Paths.get(root);
        Path base = resolved.toAbsolutePath().normalize();
        String safeNamespace = sanitizeNamespace(namespace);
        Path scoped = DEFAULT_NAMESPACE.equals(safeNamespace)
                ? base
                : base.resolve(DEFAULT_NAMESPACES_DIR).resolve(safeNamespace);
        return new RelayMeshConfig(scoped, base, safeNamespace);
    }

    private static String sanitizeNamespace(String raw) {
        String normalized = raw == null || raw.isBlank() ? DEFAULT_NAMESPACE : raw.trim().toLowerCase();
        StringBuilder sb = new StringBuilder(normalized.length());
        for (int i = 0; i < normalized.length(); i++) {
            char ch = normalized.charAt(i);
            boolean ok = (ch >= 'a' && ch <= 'z')
                    || (ch >= '0' && ch <= '9')
                    || ch == '_' || ch == '-' || ch == '.';
            sb.append(ok ? ch : '-');
        }
        String value = sb.toString();
        if (value.isBlank()) {
            return DEFAULT_NAMESPACE;
        }
        while (value.contains("--")) {
            value = value.replace("--", "-");
        }
        if (value.startsWith(".")) {
            value = "ns" + value;
        }
        return value;
    }

    public Path rootDir() {
        return rootDir;
    }

    public Path rootBaseDir() {
        return rootBaseDir;
    }

    public String namespace() {
        return namespace;
    }

    public Path dbFile() {
        return rootDir.resolve("relaymesh.db");
    }

    public Path metaDir() {
        return rootDir.resolve("meta");
    }

    public Path payloadDir() {
        return rootDir.resolve("payload");
    }

    public Path processingDir() {
        return rootDir.resolve("processing");
    }

    public Path inboxRoot() {
        return rootDir.resolve("inbox");
    }

    public Path inboxHigh() {
        return inboxRoot().resolve("high");
    }

    public Path inboxNormal() {
        return inboxRoot().resolve("normal");
    }

    public Path inboxLow() {
        return inboxRoot().resolve("low");
    }

    public Path doneRoot() {
        return rootDir.resolve("done");
    }

    public Path deadRoot() {
        return rootDir.resolve("dead");
    }

    public Path retryRoot() {
        return rootDir.resolve("retry");
    }

    public Path auditRoot() {
        return rootDir.resolve("audit");
    }

    public Path securityRoot() {
        return rootDir.resolve("security");
    }

    public Path traceRoot() {
        return rootDir.resolve("trace");
    }

    public Path replicationRoot() {
        return rootDir.resolve("replication");
    }

    public Path reportsRoot() {
        return rootDir.resolve("reports");
    }

    public Path alertsRoot() {
        return rootDir.resolve("alerts");
    }

    public Path drRoot() {
        return rootDir.resolve("dr");
    }
}
