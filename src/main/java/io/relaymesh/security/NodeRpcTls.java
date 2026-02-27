package io.relaymesh.security;

import com.fasterxml.jackson.databind.JsonNode;
import io.relaymesh.util.Jsons;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.HexFormat;

public final class NodeRpcTls {
    private NodeRpcTls() {
    }

    // Build SSL material and attach revocation-aware trust managers when truststore is provided.
    public static SslBundle build(
            String keystorePath,
            String keystorePass,
            String keystoreType,
            String truststorePath,
            String truststorePass,
            String truststoreType,
            String revocationFilePath
    ) throws Exception {
        if (keystorePath == null || keystorePath.isBlank()) {
            throw new IllegalArgumentException("keystore path is required");
        }
        if (keystorePass == null) {
            throw new IllegalArgumentException("keystore password is required");
        }
        String keyType = blankToDefault(keystoreType, "PKCS12");
        String trustType = blankToDefault(truststoreType, "PKCS12");
        Path keyPath = Path.of(keystorePath);
        KeyStore keyStore = loadKeyStore(keyPath, keystorePass, keyType);
        X509Certificate serverCert = findServerCertificate(keyStore);
        if (serverCert == null) {
            throw new IllegalArgumentException("No X509 certificate found in keystore: " + keystorePath);
        }

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, keystorePass.toCharArray());

        RevocationPolicy revocationPolicy = loadRevocationPolicy(revocationFilePath);
        TrustManager[] trustManagers = null;
        if (truststorePath != null && !truststorePath.isBlank()) {
            Path trustPath = Path.of(truststorePath);
            KeyStore trustStore = loadKeyStore(trustPath, truststorePass == null ? "" : truststorePass, trustType);
            trustManagers = buildTrustManagers(trustStore, revocationPolicy);
        }

        SSLContext ssl = SSLContext.getInstance("TLS");
        ssl.init(kmf.getKeyManagers(), trustManagers, null);
        long nowMs = Instant.now().toEpochMilli();
        return new SslBundle(
                ssl,
                fingerprintSha256(serverCert),
                normalizeSerialToken(serverCert.getSerialNumber().toString(16)),
                serverCert.getSubjectX500Principal().getName(),
                serverCert.getNotAfter().toInstant().toString(),
                revocationPolicy,
                nowMs,
                fileMtimeMs(keystorePath),
                fileMtimeMs(truststorePath),
                revocationPolicy.sourceMtimeMs()
        );
    }

    // Accept both JSON and line-oriented revocation formats and normalize values for deterministic matching.
    public static RevocationPolicy loadRevocationPolicy(String revocationFilePath) throws IOException {
        if (revocationFilePath == null || revocationFilePath.isBlank()) {
            return RevocationPolicy.empty(null, -1L);
        }
        Path path = Path.of(revocationFilePath);
        if (!Files.exists(path)) {
            return RevocationPolicy.empty(path.toString(), -1L);
        }
        long mtimeMs = Files.getLastModifiedTime(path).toMillis();
        byte[] bytes = Files.readAllBytes(path);
        String raw = decodeTextWithBom(bytes);
        String trimmed = raw == null ? "" : raw.trim();
        if (trimmed.isEmpty()) {
            return RevocationPolicy.empty(path.toString(), mtimeMs);
        }
        LinkedHashSet<String> serials = new LinkedHashSet<>();
        LinkedHashSet<String> fingerprints = new LinkedHashSet<>();

        if (trimmed.startsWith("{")) {
            parseJsonPolicy(trimmed, serials, fingerprints);
        } else {
            parseLinePolicy(trimmed, serials, fingerprints);
        }
        return new RevocationPolicy(
                Collections.unmodifiableSet(serials),
                Collections.unmodifiableSet(fingerprints),
                path.toString(),
                Instant.now().toEpochMilli(),
                mtimeMs
        );
    }

    public static long fileMtimeMs(String path) {
        if (path == null || path.isBlank()) {
            return -1L;
        }
        try {
            Path p = Path.of(path);
            if (!Files.exists(p)) {
                return -1L;
            }
            return Files.getLastModifiedTime(p).toMillis();
        } catch (Exception ignored) {
            return -1L;
        }
    }

    public static boolean isCertificateRevoked(X509Certificate certificate, RevocationPolicy policy) {
        if (certificate == null || policy == null) {
            return false;
        }
        String serial = normalizeSerialToken(certificate.getSerialNumber().toString(16));
        if (policy.revokedSerialNumbers().contains(serial)) {
            return true;
        }
        String fingerprint = fingerprintSha256(certificate);
        return policy.revokedSha256Fingerprints().contains(fingerprint);
    }

    public static String normalizeSerialToken(String raw) {
        String cleaned = compactHex(raw);
        if (cleaned.isEmpty()) {
            throw new IllegalArgumentException("revocation serial token is empty");
        }
        validateHex(cleaned, "serial");
        return cleaned;
    }

    public static String normalizeFingerprintToken(String raw) {
        String cleaned = compactHex(raw);
        if (cleaned.length() != 64) {
            throw new IllegalArgumentException("fingerprint must be 64 hex chars (sha256)");
        }
        validateHex(cleaned, "fingerprint");
        return cleaned;
    }

    public static String fingerprintSha256(X509Certificate certificate) {
        try {
            byte[] encoded = certificate.getEncoded();
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(encoded);
            return HexFormat.of().withUpperCase().formatHex(digest);
        } catch (Exception e) {
            throw new RuntimeException("failed to compute certificate fingerprint", e);
        }
    }

    public static void writeRevocationTemplate(Path output, List<String> serials, List<String> fingerprints) throws IOException {
        LinkedHashSet<String> serialSet = new LinkedHashSet<>();
        LinkedHashSet<String> fingerprintSet = new LinkedHashSet<>();
        if (serials != null) {
            for (String serial : serials) {
                if (serial == null || serial.isBlank()) {
                    continue;
                }
                serialSet.add(normalizeSerialToken(serial));
            }
        }
        if (fingerprints != null) {
            for (String fp : fingerprints) {
                if (fp == null || fp.isBlank()) {
                    continue;
                }
                fingerprintSet.add(normalizeFingerprintToken(fp));
            }
        }
        StringBuilder body = new StringBuilder();
        body.append("# RelayMesh Node RPC revocation list\n");
        body.append("# Supported forms:\n");
        body.append("# serial:<HEX_SERIAL>\n");
        body.append("# sha256:<HEX_FINGERPRINT_64>\n");
        for (String serial : serialSet) {
            body.append("serial:").append(serial).append("\n");
        }
        for (String fp : fingerprintSet) {
            body.append("sha256:").append(fp).append("\n");
        }
        Files.writeString(
                output,
                body.toString(),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
        );
    }

    private static String blankToDefault(String raw, String fallback) {
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        return raw;
    }

    private static KeyStore loadKeyStore(Path path, String password, String type) throws Exception {
        KeyStore keyStore = KeyStore.getInstance(type);
        try (var in = Files.newInputStream(path)) {
            keyStore.load(in, password == null ? new char[0] : password.toCharArray());
        }
        return keyStore;
    }

    private static X509Certificate findServerCertificate(KeyStore keyStore) throws Exception {
        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            if (!keyStore.isKeyEntry(alias) && !keyStore.isCertificateEntry(alias)) {
                continue;
            }
            var cert = keyStore.getCertificate(alias);
            if (cert instanceof X509Certificate x509) {
                return x509;
            }
        }
        return null;
    }

    private static TrustManager[] buildTrustManagers(KeyStore trustStore, RevocationPolicy revocationPolicy) throws Exception {
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);
        TrustManager[] base = tmf.getTrustManagers();
        TrustManager[] wrapped = new TrustManager[base.length];
        for (int i = 0; i < base.length; i++) {
            TrustManager manager = base[i];
            // Wrap X509 trust managers so certificate chain validation remains standard, then apply revocation policy.
            if (manager instanceof X509ExtendedTrustManager ext) {
                wrapped[i] = new RevocationTrustManager(ext, revocationPolicy);
            } else if (manager instanceof X509TrustManager x509) {
                wrapped[i] = new RevocationTrustManager(new X509TrustManagerAdapter(x509), revocationPolicy);
            } else {
                wrapped[i] = manager;
            }
        }
        return wrapped;
    }

    private static void parseJsonPolicy(
            String trimmed,
            Set<String> serials,
            Set<String> fingerprints
    ) throws IOException {
        JsonNode root = Jsons.mapper().readTree(trimmed);
        JsonNode serialNode = root.path("serialNumbers");
        if (serialNode.isArray()) {
            for (JsonNode n : serialNode) {
                String token = n.asText("");
                if (!token.isBlank()) {
                    serials.add(normalizeSerialToken(token));
                }
            }
        }
        JsonNode fpNode = root.path("sha256Fingerprints");
        if (fpNode.isArray()) {
            for (JsonNode n : fpNode) {
                String token = n.asText("");
                if (!token.isBlank()) {
                    fingerprints.add(normalizeFingerprintToken(token));
                }
            }
        }
    }

    private static void parseLinePolicy(
            String trimmed,
            Set<String> serials,
            Set<String> fingerprints
    ) {
        String[] lines = trimmed.split("\r?\n");
        for (String line : lines) {
            String raw = line == null ? "" : stripBom(line).trim();
            if (raw.isEmpty() || raw.startsWith("#")) {
                continue;
            }
            int sep = raw.indexOf(':');
            if (sep > 0) {
                String type = raw.substring(0, sep).trim().toLowerCase(Locale.ROOT);
                String value = raw.substring(sep + 1).trim();
                if (value.isEmpty()) {
                    continue;
                }
                if (Objects.equals(type, "serial")) {
                    serials.add(normalizeSerialToken(value));
                    continue;
                }
                if (Objects.equals(type, "sha256")
                        || Objects.equals(type, "fingerprint")
                        || Objects.equals(type, "sha256fingerprint")) {
                    fingerprints.add(normalizeFingerprintToken(value));
                    continue;
                }
                throw new IllegalArgumentException("unsupported revocation token prefix: " + type);
            }
            // Bare tokens are treated as fingerprint if length=64 (sha256), otherwise as serial.
            String compact = compactHex(raw);
            if (compact.length() == 64) {
                fingerprints.add(normalizeFingerprintToken(compact));
            } else {
                serials.add(normalizeSerialToken(compact));
            }
        }
    }

    private static String compactHex(String raw) {
        if (raw == null) {
            return "";
        }
        String token = stripBom(raw).trim();
        if (token.toLowerCase(Locale.ROOT).startsWith("0x")) {
            token = token.substring(2);
        }
        token = token.replace(":", "")
                .replace("-", "")
                .replace(" ", "")
                .trim()
                .toUpperCase(Locale.ROOT);
        return token;
    }

    private static void validateHex(String value, String fieldName) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            boolean ok = (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F');
            if (!ok) {
                throw new IllegalArgumentException(fieldName + " must be hex: " + value);
            }
        }
    }

    private static String stripBom(String raw) {
        if (raw == null || raw.isEmpty()) {
            return raw == null ? "" : raw;
        }
        if (raw.charAt(0) == '\uFEFF') {
            return raw.substring(1);
        }
        return raw;
    }

    private static String decodeTextWithBom(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }
        // Revocation files can be edited on different platforms; accept common BOM variants explicitly.
        if (bytes.length >= 3
                && (bytes[0] & 0xFF) == 0xEF
                && (bytes[1] & 0xFF) == 0xBB
                && (bytes[2] & 0xFF) == 0xBF) {
            return new String(bytes, 3, bytes.length - 3, StandardCharsets.UTF_8);
        }
        if (bytes.length >= 2
                && (bytes[0] & 0xFF) == 0xFF
                && (bytes[1] & 0xFF) == 0xFE) {
            return new String(bytes, 2, bytes.length - 2, StandardCharsets.UTF_16LE);
        }
        if (bytes.length >= 2
                && (bytes[0] & 0xFF) == 0xFE
                && (bytes[1] & 0xFF) == 0xFF) {
            return new String(bytes, 2, bytes.length - 2, StandardCharsets.UTF_16BE);
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public record RevocationPolicy(
            Set<String> revokedSerialNumbers,
            Set<String> revokedSha256Fingerprints,
            String sourcePath,
            long loadedAtMs,
            long sourceMtimeMs
    ) {
        static RevocationPolicy empty(String sourcePath, long sourceMtimeMs) {
            return new RevocationPolicy(
                    Collections.emptySet(),
                    Collections.emptySet(),
                    sourcePath,
                    Instant.now().toEpochMilli(),
                    sourceMtimeMs
            );
        }

        public int entryCount() {
            return revokedSerialNumbers.size() + revokedSha256Fingerprints.size();
        }
    }

    public record SslBundle(
            SSLContext sslContext,
            String serverCertFingerprintSha256,
            String serverCertSerialHex,
            String serverCertSubject,
            String serverCertNotAfterIso,
            RevocationPolicy revocationPolicy,
            long loadedAtMs,
            long keystoreMtimeMs,
            long truststoreMtimeMs,
            long revocationMtimeMs
    ) {
    }

    private static final class RevocationTrustManager extends X509ExtendedTrustManager {
        private final X509ExtendedTrustManager delegate;
        private final RevocationPolicy revocationPolicy;

        private RevocationTrustManager(X509ExtendedTrustManager delegate, RevocationPolicy revocationPolicy) {
            this.delegate = Objects.requireNonNull(delegate);
            this.revocationPolicy = revocationPolicy == null
                    ? RevocationPolicy.empty(null, -1L)
                    : revocationPolicy;
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            delegate.checkClientTrusted(chain, authType);
            enforce(chain);
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            delegate.checkServerTrusted(chain, authType);
            enforce(chain);
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return delegate.getAcceptedIssuers();
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
            delegate.checkClientTrusted(chain, authType, socket);
            enforce(chain);
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
            delegate.checkServerTrusted(chain, authType, socket);
            enforce(chain);
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, javax.net.ssl.SSLEngine engine) throws CertificateException {
            delegate.checkClientTrusted(chain, authType, engine);
            enforce(chain);
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, javax.net.ssl.SSLEngine engine) throws CertificateException {
            delegate.checkServerTrusted(chain, authType, engine);
            enforce(chain);
        }

        private void enforce(X509Certificate[] chain) throws CertificateException {
            if (chain == null || chain.length == 0 || chain[0] == null) {
                return;
            }
            X509Certificate leaf = chain[0];
            if (isCertificateRevoked(leaf, revocationPolicy)) {
                throw new CertificateException(
                        "certificate revoked by node rpc policy: serial=" +
                                normalizeSerialToken(leaf.getSerialNumber().toString(16)) +
                                ", sha256=" + fingerprintSha256(leaf)
                );
            }
        }
    }

    private static final class X509TrustManagerAdapter extends X509ExtendedTrustManager {
        private final X509TrustManager delegate;

        private X509TrustManagerAdapter(X509TrustManager delegate) {
            this.delegate = Objects.requireNonNull(delegate);
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            delegate.checkClientTrusted(chain, authType);
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            delegate.checkServerTrusted(chain, authType);
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return delegate.getAcceptedIssuers();
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
            delegate.checkClientTrusted(chain, authType);
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
            delegate.checkServerTrusted(chain, authType);
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, javax.net.ssl.SSLEngine engine) throws CertificateException {
            delegate.checkClientTrusted(chain, authType);
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, javax.net.ssl.SSLEngine engine) throws CertificateException {
            delegate.checkServerTrusted(chain, authType);
        }
    }
}
