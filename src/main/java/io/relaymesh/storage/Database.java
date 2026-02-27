package io.relaymesh.storage;

import io.relaymesh.config.RelayMeshConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class Database {
    private static final String MIGRATION_SCHEMA_VERSION = "relaymesh.schema.migration.v1";
    private final RelayMeshConfig config;
    private final String jdbcUrl;

    public Database(RelayMeshConfig config) {
        this.config = config;
        this.jdbcUrl = "jdbc:sqlite:" + config.dbFile().toString();
    }

    public String namespace() {
        return config.namespace();
    }

    public void init() {
        initDirectories();
        initSchema();
        applyAndValidatePragmas();
    }

    public Connection openConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl);
    }

    private void initDirectories() {
        try {
            Files.createDirectories(config.rootDir());
            Files.createDirectories(config.metaDir());
            Files.createDirectories(config.payloadDir());
            Files.createDirectories(config.processingDir());
            Files.createDirectories(config.inboxHigh());
            Files.createDirectories(config.inboxNormal());
            Files.createDirectories(config.inboxLow());
            Files.createDirectories(config.doneRoot());
            Files.createDirectories(config.deadRoot());
            Files.createDirectories(config.retryRoot());
            Files.createDirectories(config.auditRoot());
            Files.createDirectories(config.securityRoot());
            Files.createDirectories(config.traceRoot());
            Files.createDirectories(config.replicationRoot());
            Files.createDirectories(config.reportsRoot());
            Files.createDirectories(config.alertsRoot());
            Files.createDirectories(config.drRoot());
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize directories", e);
        }
    }

    private void initSchema() {
        try (Connection conn = openConnection(); Statement st = conn.createStatement()) {
            st.execute("""
                    CREATE TABLE IF NOT EXISTS tasks (
                        task_id TEXT PRIMARY KEY,
                        namespace TEXT NOT NULL DEFAULT 'default',
                        status TEXT NOT NULL,
                        idempotency_key TEXT NOT NULL UNIQUE,
                        principal_id TEXT NOT NULL DEFAULT '',
                        trace_id TEXT NOT NULL,
                        result_payload TEXT,
                        last_error TEXT,
                        created_at_ms INTEGER NOT NULL,
                        updated_at_ms INTEGER NOT NULL
                    )
                    """);

            st.execute("""
                    CREATE TABLE IF NOT EXISTS steps (
                        step_id TEXT PRIMARY KEY,
                        namespace TEXT NOT NULL DEFAULT 'default',
                        task_id TEXT NOT NULL,
                        agent_id TEXT NOT NULL,
                        priority TEXT NOT NULL DEFAULT 'NORMAL',
                        payload_path TEXT NOT NULL DEFAULT '',
                        is_enqueued INTEGER NOT NULL DEFAULT 0,
                        result_payload TEXT,
                        status TEXT NOT NULL,
                        attempt INTEGER NOT NULL,
                        next_retry_at_ms INTEGER,
                        lease_owner TEXT,
                        lease_token TEXT,
                        lease_epoch INTEGER NOT NULL DEFAULT 0,
                        created_at_ms INTEGER NOT NULL,
                        updated_at_ms INTEGER NOT NULL,
                        FOREIGN KEY(task_id) REFERENCES tasks(task_id)
                    )
                    """);
            st.execute("""
                    CREATE TABLE IF NOT EXISTS step_dependencies (
                        namespace TEXT NOT NULL DEFAULT 'default',
                        task_id TEXT NOT NULL,
                        step_id TEXT NOT NULL,
                        depends_on_step_id TEXT NOT NULL,
                        created_at_ms INTEGER NOT NULL,
                        PRIMARY KEY(namespace, task_id, step_id, depends_on_step_id),
                        FOREIGN KEY(task_id) REFERENCES tasks(task_id)
                    )
                    """);
            ensureStepColumns(conn);
            ensureTaskColumns(conn);
            ensureStepDependencyColumns(conn);

            st.execute("""
                    CREATE TABLE IF NOT EXISTS messages (
                        msg_id TEXT PRIMARY KEY,
                        namespace TEXT NOT NULL DEFAULT 'default',
                        task_id TEXT NOT NULL,
                        step_id TEXT NOT NULL,
                        from_agent TEXT NOT NULL,
                        to_agent TEXT NOT NULL,
                        priority TEXT NOT NULL,
                        state TEXT NOT NULL,
                        meta_path TEXT NOT NULL,
                        payload_path TEXT NOT NULL,
                        created_at_ms INTEGER NOT NULL,
                        updated_at_ms INTEGER NOT NULL,
                        FOREIGN KEY(task_id) REFERENCES tasks(task_id),
                        FOREIGN KEY(step_id) REFERENCES steps(step_id)
                    )
                    """);

            st.execute("""
                    CREATE TABLE IF NOT EXISTS idempotency (
                        namespace TEXT NOT NULL DEFAULT 'default',
                        idempotency_key TEXT PRIMARY KEY,
                        status TEXT NOT NULL,
                        result_ref TEXT NOT NULL,
                        result_hash TEXT,
                        result_hash_algo TEXT,
                        processed_at_ms INTEGER,
                        created_at_ms INTEGER NOT NULL,
                        updated_at_ms INTEGER NOT NULL
                    )
                    """);
            ensureMessageColumns(conn);
            ensureIdempotencyColumns(conn);
            st.execute("""
                    CREATE TABLE IF NOT EXISTS mesh_nodes (
                        node_id TEXT PRIMARY KEY,
                        status TEXT NOT NULL,
                        last_heartbeat_ms INTEGER NOT NULL,
                        updated_at_ms INTEGER NOT NULL,
                        status_changed_at_ms INTEGER NOT NULL
                    )
                    """);
            ensureMeshNodeColumns(conn);
            st.execute("""
                    CREATE TABLE IF NOT EXISTS lease_conflicts (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        event_type TEXT NOT NULL,
                        task_id TEXT,
                        step_id TEXT,
                        worker_id TEXT,
                        expected_status TEXT,
                        expected_lease_token TEXT,
                        expected_lease_epoch INTEGER,
                        actual_status TEXT,
                        actual_lease_owner TEXT,
                        actual_lease_token TEXT,
                        actual_lease_epoch INTEGER,
                        occurred_at_ms INTEGER NOT NULL
                    )
                    """);
            ensureLeaseConflictColumns(conn);
            st.execute("""
                    CREATE TABLE IF NOT EXISTS cluster_state (
                        state_key TEXT PRIMARY KEY,
                        state_value TEXT NOT NULL,
                        version INTEGER NOT NULL,
                        updated_at_ms INTEGER NOT NULL
                    )
                    """);
            ensureClusterStateDefaults(conn);
            ensureSchemaMigrationsTable(conn);
            applyVersionedMigrations(conn);

            st.execute("CREATE INDEX IF NOT EXISTS idx_steps_status_next_retry ON steps(status, next_retry_at_ms)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_steps_lease_owner_updated ON steps(lease_owner, updated_at_ms)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_steps_task_status ON steps(task_id, status)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_steps_task_enqueued ON steps(task_id, is_enqueued)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_steps_namespace_status ON steps(namespace, status)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_step_dep_task_step ON step_dependencies(task_id, step_id)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_messages_state_priority_created ON messages(state, priority, created_at_ms)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_messages_namespace_state ON messages(namespace, state)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_idempotency_processed_at ON idempotency(processed_at_ms)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_tasks_namespace_status ON tasks(namespace, status)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_mesh_nodes_status_heartbeat ON mesh_nodes(status, last_heartbeat_ms)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_lease_conflicts_time ON lease_conflicts(occurred_at_ms)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_lease_conflicts_step_time ON lease_conflicts(step_id, occurred_at_ms)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_cluster_state_updated ON cluster_state(updated_at_ms)");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize SQLite schema", e);
        }
    }

    private void ensureStepColumns(Connection conn) throws SQLException {
        Set<String> columns = new HashSet<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("PRAGMA table_info(steps)")) {
            while (rs.next()) {
                columns.add(rs.getString("name").toLowerCase());
            }
        }

        try (Statement st = conn.createStatement()) {
            if (!columns.contains("priority")) {
                st.execute("ALTER TABLE steps ADD COLUMN priority TEXT NOT NULL DEFAULT 'NORMAL'");
            }
            if (!columns.contains("namespace")) {
                st.execute("ALTER TABLE steps ADD COLUMN namespace TEXT NOT NULL DEFAULT 'default'");
            }
            if (!columns.contains("payload_path")) {
                st.execute("ALTER TABLE steps ADD COLUMN payload_path TEXT NOT NULL DEFAULT ''");
            }
            if (!columns.contains("is_enqueued")) {
                st.execute("ALTER TABLE steps ADD COLUMN is_enqueued INTEGER NOT NULL DEFAULT 0");
            }
            if (!columns.contains("result_payload")) {
                st.execute("ALTER TABLE steps ADD COLUMN result_payload TEXT");
            }
            if (!columns.contains("lease_epoch")) {
                st.execute("ALTER TABLE steps ADD COLUMN lease_epoch INTEGER NOT NULL DEFAULT 0");
            }
            st.execute("UPDATE steps SET lease_epoch=attempt WHERE lease_epoch=0 AND attempt>0");
            st.execute("UPDATE steps SET namespace='" + safeSql(namespace()) + "' WHERE namespace IS NULL OR namespace=''");
        }
    }

    private void ensureTaskColumns(Connection conn) throws SQLException {
        Set<String> columns = new HashSet<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("PRAGMA table_info(tasks)")) {
            while (rs.next()) {
                columns.add(rs.getString("name").toLowerCase());
            }
        }
        try (Statement st = conn.createStatement()) {
            if (!columns.contains("namespace")) {
                st.execute("ALTER TABLE tasks ADD COLUMN namespace TEXT NOT NULL DEFAULT 'default'");
            }
            if (!columns.contains("principal_id")) {
                st.execute("ALTER TABLE tasks ADD COLUMN principal_id TEXT NOT NULL DEFAULT ''");
            }
            st.execute("UPDATE tasks SET namespace='" + safeSql(namespace()) + "' WHERE namespace IS NULL OR namespace=''");
        }
    }

    private void ensureStepDependencyColumns(Connection conn) throws SQLException {
        Set<String> columns = new HashSet<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("PRAGMA table_info(step_dependencies)")) {
            while (rs.next()) {
                columns.add(rs.getString("name").toLowerCase());
            }
        }
        try (Statement st = conn.createStatement()) {
            if (!columns.contains("namespace")) {
                st.execute("ALTER TABLE step_dependencies ADD COLUMN namespace TEXT NOT NULL DEFAULT 'default'");
            }
            st.execute("UPDATE step_dependencies SET namespace='" + safeSql(namespace()) + "' WHERE namespace IS NULL OR namespace=''");
        }
    }

    private void ensureMessageColumns(Connection conn) throws SQLException {
        Set<String> columns = new HashSet<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("PRAGMA table_info(messages)")) {
            while (rs.next()) {
                columns.add(rs.getString("name").toLowerCase());
            }
        }
        try (Statement st = conn.createStatement()) {
            if (!columns.contains("namespace")) {
                st.execute("ALTER TABLE messages ADD COLUMN namespace TEXT NOT NULL DEFAULT 'default'");
            }
            st.execute("UPDATE messages SET namespace='" + safeSql(namespace()) + "' WHERE namespace IS NULL OR namespace=''");
        }
    }

    private void ensureIdempotencyColumns(Connection conn) throws SQLException {
        Set<String> columns = new HashSet<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("PRAGMA table_info(idempotency)")) {
            while (rs.next()) {
                columns.add(rs.getString("name").toLowerCase());
            }
        }
        try (Statement st = conn.createStatement()) {
            if (!columns.contains("namespace")) {
                st.execute("ALTER TABLE idempotency ADD COLUMN namespace TEXT NOT NULL DEFAULT 'default'");
            }
            st.execute("UPDATE idempotency SET namespace='" + safeSql(namespace()) + "' WHERE namespace IS NULL OR namespace=''");
        }
    }

    private void ensureMeshNodeColumns(Connection conn) throws SQLException {
        Set<String> columns = new HashSet<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("PRAGMA table_info(mesh_nodes)")) {
            while (rs.next()) {
                columns.add(rs.getString("name").toLowerCase());
            }
        }
        try (Statement st = conn.createStatement()) {
            if (!columns.contains("status_changed_at_ms")) {
                st.execute("ALTER TABLE mesh_nodes ADD COLUMN status_changed_at_ms INTEGER NOT NULL DEFAULT 0");
            }
            st.execute("UPDATE mesh_nodes SET status_changed_at_ms=updated_at_ms WHERE status_changed_at_ms=0");
        }
    }

    private void ensureLeaseConflictColumns(Connection conn) throws SQLException {
        Set<String> columns = new HashSet<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("PRAGMA table_info(lease_conflicts)")) {
            while (rs.next()) {
                columns.add(rs.getString("name").toLowerCase());
            }
        }
        try (Statement st = conn.createStatement()) {
            if (!columns.contains("expected_lease_epoch")) {
                st.execute("ALTER TABLE lease_conflicts ADD COLUMN expected_lease_epoch INTEGER");
            }
            if (!columns.contains("actual_lease_epoch")) {
                st.execute("ALTER TABLE lease_conflicts ADD COLUMN actual_lease_epoch INTEGER");
            }
        }
    }

    private void ensureClusterStateDefaults(Connection conn) throws SQLException {
        long nowMs = System.currentTimeMillis();
        try (Statement st = conn.createStatement()) {
            st.execute("""
                    INSERT OR IGNORE INTO cluster_state(state_key,state_value,version,updated_at_ms)
                    VALUES('cluster_epoch','1',1,%d)
                    """.formatted(nowMs));
        }
    }

    private void ensureSchemaMigrationsTable(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("""
                    CREATE TABLE IF NOT EXISTS schema_migrations (
                        version TEXT PRIMARY KEY,
                        description TEXT NOT NULL,
                        checksum TEXT NOT NULL,
                        applied_at_ms INTEGER NOT NULL,
                        success INTEGER NOT NULL
                    )
                    """);
            st.execute("CREATE INDEX IF NOT EXISTS idx_schema_migrations_applied ON schema_migrations(applied_at_ms)");
        }
    }

    private void applyVersionedMigrations(Connection conn) throws SQLException {
        List<MigrationStep> steps = new ArrayList<>();
        steps.add(new MigrationStep(
                "20260226_001_namespace_and_principal",
                "Add namespace/principal columns and namespace indexes",
                List.of(
                        "CREATE INDEX IF NOT EXISTS idx_tasks_namespace_updated ON tasks(namespace, updated_at_ms)",
                        "CREATE INDEX IF NOT EXISTS idx_steps_namespace_updated ON steps(namespace, updated_at_ms)",
                        "CREATE INDEX IF NOT EXISTS idx_messages_namespace_updated ON messages(namespace, updated_at_ms)"
                )
        ));
        steps.add(new MigrationStep(
                "20260226_002_audit_integrity_cursor",
                "Add migration marker for audit integrity chain rollout",
                List.of("SELECT 1")
        ));
        for (MigrationStep step : steps) {
            if (isMigrationApplied(conn, step.version())) {
                continue;
            }
            applyMigration(conn, step);
        }
    }

    private boolean isMigrationApplied(Connection conn, String version) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT 1 FROM schema_migrations WHERE version=? AND success=1 LIMIT 1")) {
            ps.setString(1, version);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private void applyMigration(Connection conn, MigrationStep step) throws SQLException {
        String checksum = checksum(step);
        try (Statement st = conn.createStatement()) {
            for (String sql : step.sql()) {
                st.execute(sql);
            }
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT OR REPLACE INTO schema_migrations(version,description,checksum,applied_at_ms,success) VALUES(?,?,?,?,1)")) {
            ps.setString(1, step.version());
            ps.setString(2, step.description());
            ps.setString(3, checksum);
            ps.setLong(4, Instant.now().toEpochMilli());
            ps.executeUpdate();
        }
    }

    private String checksum(MigrationStep step) {
        StringBuilder sb = new StringBuilder();
        sb.append(MIGRATION_SCHEMA_VERSION).append('|')
                .append(step.version()).append('|')
                .append(step.description()).append('|');
        for (String sql : step.sql()) {
            sb.append(sql).append(';');
        }
        return Integer.toHexString(sb.toString().hashCode());
    }

    private String safeSql(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("'", "''");
    }

    private record MigrationStep(String version, String description, List<String> sql) {
    }

    private void applyAndValidatePragmas() {
        try (Connection conn = openConnection(); Statement st = conn.createStatement()) {
            st.execute("PRAGMA journal_mode=WAL");
            st.execute("PRAGMA synchronous=NORMAL");
            st.execute("PRAGMA foreign_keys=ON");
            st.execute("PRAGMA busy_timeout=5000");

            validatePragma(st, "journal_mode", "wal");
            validatePragma(st, "synchronous", "1");
            validatePragma(st, "foreign_keys", "1");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to apply SQLite pragmas", e);
        }
    }

    private void validatePragma(Statement st, String pragma, String expected) throws SQLException {
        try (ResultSet rs = st.executeQuery("PRAGMA " + pragma)) {
            if (!rs.next()) {
                throw new IllegalStateException("PRAGMA " + pragma + " did not return a value");
            }
            String actual = rs.getString(1);
            if (actual == null || !actual.equalsIgnoreCase(expected)) {
                throw new IllegalStateException(
                        "PRAGMA " + pragma + " mismatch, expected=" + expected + ", actual=" + actual
                );
            }
        }
    }

    public List<SchemaMigrationRow> listSchemaMigrations(int limit) {
        String sql = """
                SELECT version,description,checksum,applied_at_ms,success
                FROM schema_migrations
                ORDER BY applied_at_ms DESC, version DESC
                LIMIT ?
                """;
        int safeLimit = Math.max(1, limit);
        List<SchemaMigrationRow> out = new ArrayList<>();
        try (Connection c = openConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setInt(1, safeLimit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new SchemaMigrationRow(
                            rs.getString("version"),
                            rs.getString("description"),
                            rs.getString("checksum"),
                            rs.getLong("applied_at_ms"),
                            rs.getInt("success") == 1
                    ));
                }
            }
            return out;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to list schema migrations", e);
        }
    }

    public RollbackOutcome rollbackMigration(String version) {
        if (version == null || version.isBlank()) {
            throw new IllegalArgumentException("version must not be blank");
        }
        String v = version.trim();
        List<String> rollbackSql;
        if ("20260226_002_audit_integrity_cursor".equals(v)) {
            rollbackSql = List.of("SELECT 1");
        } else if ("20260226_001_namespace_and_principal".equals(v)) {
            rollbackSql = List.of(
                    "DROP INDEX IF EXISTS idx_tasks_namespace_updated",
                    "DROP INDEX IF EXISTS idx_steps_namespace_updated",
                    "DROP INDEX IF EXISTS idx_messages_namespace_updated"
            );
        } else {
            throw new IllegalArgumentException("Unsupported rollback migration version: " + version);
        }
        try (Connection c = openConnection()) {
            c.setAutoCommit(false);
            try (Statement st = c.createStatement();
                 PreparedStatement del = c.prepareStatement("DELETE FROM schema_migrations WHERE version=?")) {
                for (String sql : rollbackSql) {
                    st.execute(sql);
                }
                del.setString(1, v);
                int removed = del.executeUpdate();
                c.commit();
                return new RollbackOutcome(v, rollbackSql.size(), removed > 0);
            } catch (Exception e) {
                c.rollback();
                throw e;
            } finally {
                c.setAutoCommit(true);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to rollback migration: " + version, e);
        }
    }

    public record SchemaMigrationRow(
            String version,
            String description,
            String checksum,
            long appliedAtMs,
            boolean success
    ) {
    }

    public record RollbackOutcome(String version, int statements, boolean removedMigrationRow) {
    }
}
