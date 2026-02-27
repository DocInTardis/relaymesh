package io.relaymesh.security;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Principal;
import java.security.PublicKey;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Date;
import java.util.Set;

final class NodeRpcTlsTest {

    @Test
    void parseRevocationPolicyFromLineAndJsonFormats() throws Exception {
        Path root = Files.createTempDirectory("relaymesh-node-rpc-tls-test-");
        try {
            String linePolicy = """
                    # sample
                    serial:0x1a2b
                    sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
                    """;
            Path policy = root.resolve("revocations.txt");
            Files.writeString(policy, linePolicy, StandardCharsets.UTF_8);
            NodeRpcTls.RevocationPolicy parsedLine = NodeRpcTls.loadRevocationPolicy(policy.toString());
            Assertions.assertTrue(parsedLine.revokedSerialNumbers().contains("1A2B"));
            Assertions.assertTrue(parsedLine.revokedSha256Fingerprints().contains(
                    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            ));

            String jsonPolicy = """
                    {
                      "serialNumbers": ["00ff", "0x10"],
                      "sha256Fingerprints": ["BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"]
                    }
                    """;
            Files.writeString(policy, jsonPolicy, StandardCharsets.UTF_8);
            NodeRpcTls.RevocationPolicy parsedJson = NodeRpcTls.loadRevocationPolicy(policy.toString());
            Assertions.assertTrue(parsedJson.revokedSerialNumbers().contains("00FF"));
            Assertions.assertTrue(parsedJson.revokedSerialNumbers().contains("10"));
            Assertions.assertTrue(parsedJson.revokedSha256Fingerprints().contains(
                    "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
            ));
        } finally {
            deleteRecursively(root);
        }
    }

    @Test
    void revokedMatchChecksSerialAndFingerprint() {
        FakeX509Certificate cert = new FakeX509Certificate(
                new BigInteger("1A2B", 16),
                new byte[]{1, 2, 3, 4}
        );
        String fingerprint = NodeRpcTls.fingerprintSha256(cert);

        NodeRpcTls.RevocationPolicy bySerial = new NodeRpcTls.RevocationPolicy(
                Set.of("1A2B"),
                Set.of(),
                "inline",
                Instant.now().toEpochMilli(),
                -1L
        );
        Assertions.assertTrue(NodeRpcTls.isCertificateRevoked(cert, bySerial));

        NodeRpcTls.RevocationPolicy byFingerprint = new NodeRpcTls.RevocationPolicy(
                Set.of(),
                Set.of(fingerprint),
                "inline",
                Instant.now().toEpochMilli(),
                -1L
        );
        Assertions.assertTrue(NodeRpcTls.isCertificateRevoked(cert, byFingerprint));

        NodeRpcTls.RevocationPolicy none = new NodeRpcTls.RevocationPolicy(
                Set.of(),
                Set.of(),
                "inline",
                Instant.now().toEpochMilli(),
                -1L
        );
        Assertions.assertFalse(NodeRpcTls.isCertificateRevoked(cert, none));
    }

    @Test
    void invalidFingerprintLengthRejected() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> NodeRpcTls.normalizeFingerprintToken("ABCDEF")
        );
    }

    private static void deleteRecursively(Path root) throws Exception {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try (var walk = Files.walk(root)) {
            for (Path p : walk.sorted((a, b) -> Integer.compare(b.getNameCount(), a.getNameCount())).toList()) {
                Files.deleteIfExists(p);
            }
        }
    }

    private static final class FakeX509Certificate extends X509Certificate {
        private final BigInteger serial;
        private final byte[] encoded;

        private FakeX509Certificate(BigInteger serial, byte[] encoded) {
            this.serial = serial;
            this.encoded = encoded.clone();
        }

        @Override
        public BigInteger getSerialNumber() {
            return serial;
        }

        @Override
        public byte[] getEncoded() throws CertificateEncodingException {
            return encoded.clone();
        }

        @Override
        public void checkValidity() {
        }

        @Override
        public void checkValidity(Date date) {
        }

        @Override
        public int getVersion() {
            return 3;
        }

        @Override
        public Principal getIssuerDN() {
            throw unsupported();
        }

        @Override
        public Principal getSubjectDN() {
            throw unsupported();
        }

        @Override
        public Date getNotBefore() {
            return new Date(0L);
        }

        @Override
        public Date getNotAfter() {
            return new Date(Long.MAX_VALUE);
        }

        @Override
        public byte[] getTBSCertificate() {
            throw unsupported();
        }

        @Override
        public byte[] getSignature() {
            throw unsupported();
        }

        @Override
        public String getSigAlgName() {
            throw unsupported();
        }

        @Override
        public String getSigAlgOID() {
            throw unsupported();
        }

        @Override
        public byte[] getSigAlgParams() {
            throw unsupported();
        }

        @Override
        public boolean[] getIssuerUniqueID() {
            throw unsupported();
        }

        @Override
        public boolean[] getSubjectUniqueID() {
            throw unsupported();
        }

        @Override
        public boolean[] getKeyUsage() {
            throw unsupported();
        }

        @Override
        public int getBasicConstraints() {
            return -1;
        }

        @Override
        public void verify(PublicKey key) {
            throw unsupported();
        }

        @Override
        public void verify(PublicKey key, String sigProvider) {
            throw unsupported();
        }

        @Override
        public String toString() {
            return "FakeX509Certificate(" + serial.toString(16) + ")";
        }

        @Override
        public PublicKey getPublicKey() {
            throw unsupported();
        }

        @Override
        public boolean hasUnsupportedCriticalExtension() {
            return false;
        }

        @Override
        public Set<String> getCriticalExtensionOIDs() {
            return null;
        }

        @Override
        public Set<String> getNonCriticalExtensionOIDs() {
            return null;
        }

        @Override
        public byte[] getExtensionValue(String oid) {
            return null;
        }

        private UnsupportedOperationException unsupported() {
            return new UnsupportedOperationException("not required for this test");
        }
    }
}
