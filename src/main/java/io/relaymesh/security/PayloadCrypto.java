package io.relaymesh.security;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.relaymesh.util.Jsons;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

public final class PayloadCrypto {
    private static final String SCHEMA = "relaymesh.aesgcm.v1";
    private static final int GCM_TAG_BITS = 128;
    private static final int GCM_IV_BYTES = 12;
    private static final int KEY_BYTES = 32;

    private final Path keyFile;
    private final SecureRandom secureRandom;
    private volatile Keyring keyring;

    public PayloadCrypto(Path keyFile) {
        this.keyFile = keyFile;
        this.secureRandom = new SecureRandom();
        this.keyring = loadOrCreateKeyring();
    }

    public String encrypt(String plaintext) {
        if (plaintext == null) {
            return null;
        }
        Keyring ring = keyring;
        if (ring == null || ring.activeKid == null || ring.activeKid.isBlank() || ring.keys.isEmpty()) {
            return plaintext;
        }
        SecretKeySpec key = ring.keys.get(ring.activeKid);
        if (key == null) {
            return plaintext;
        }
        byte[] iv = new byte[GCM_IV_BYTES];
        secureRandom.nextBytes(iv);
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_BITS, iv));
            byte[] cipherText = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));
            ObjectNode row = Jsons.mapper().createObjectNode();
            row.put("enc", SCHEMA);
            row.put("kid", ring.activeKid);
            row.put("iv", Base64.getEncoder().encodeToString(iv));
            row.put("ct", Base64.getEncoder().encodeToString(cipherText));
            return Jsons.mapper().writeValueAsString(row);
        } catch (Exception e) {
            throw new RuntimeException("Failed to encrypt payload", e);
        }
    }

    public String decryptIfNeeded(String maybeCipherText) {
        if (maybeCipherText == null || maybeCipherText.isBlank()) {
            return maybeCipherText;
        }
        String raw = maybeCipherText.trim();
        if (!raw.startsWith("{") || !raw.contains("\"enc\"")) {
            return maybeCipherText;
        }
        JsonNode node;
        try {
            node = Jsons.mapper().readTree(raw);
        } catch (Exception ignored) {
            return maybeCipherText;
        }
        String enc = node.path("enc").asText("");
        if (!SCHEMA.equals(enc)) {
            return maybeCipherText;
        }
        String ivBase64 = node.path("iv").asText("");
        String ctBase64 = node.path("ct").asText("");
        String kid = node.path("kid").asText("");
        if (ivBase64.isBlank() || ctBase64.isBlank()) {
            throw new RuntimeException("Invalid encrypted payload format: missing iv/ct");
        }
        byte[] iv = Base64.getDecoder().decode(ivBase64);
        byte[] cipherText = Base64.getDecoder().decode(ctBase64);
        Keyring ring = keyring;
        if (ring == null) {
            throw new RuntimeException("Payload keyring is not loaded");
        }
        if (!kid.isBlank()) {
            SecretKeySpec exact = ring.keys.get(kid);
            if (exact != null) {
                return decrypt(cipherText, iv, exact);
            }
        }
        for (SecretKeySpec key : ring.keys.values()) {
            try {
                return decrypt(cipherText, iv, key);
            } catch (RuntimeException ignored) {
                // Try next key in rotation.
            }
        }
        throw new RuntimeException("Unable to decrypt payload with current keyring");
    }

    public synchronized RotationOutcome rotate() {
        Keyring current = keyring == null ? new Keyring("", new LinkedHashMap<>()) : keyring;
        LinkedHashMap<String, SecretKeySpec> next = new LinkedHashMap<>(current.keys);
        String kid = "k" + Instant.now().toEpochMilli();
        byte[] raw = new byte[KEY_BYTES];
        secureRandom.nextBytes(raw);
        next.put(kid, new SecretKeySpec(raw, "AES"));
        Keyring rotated = new Keyring(kid, next);
        persistKeyring(rotated);
        keyring = rotated;
        return new RotationOutcome(kid, next.size(), keyFile.toString());
    }

    public KeyringStatus status() {
        Keyring ring = keyring;
        int total = ring == null ? 0 : ring.keys.size();
        String active = ring == null ? "" : ring.activeKid;
        return new KeyringStatus(active, total, keyFile.toString());
    }

    private String decrypt(byte[] cipherText, byte[] iv, SecretKeySpec key) {
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_BITS, iv));
            byte[] plain = cipher.doFinal(cipherText);
            return new String(plain, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("Failed to decrypt payload", e);
        }
    }

    private synchronized Keyring loadOrCreateKeyring() {
        if (!Files.exists(keyFile)) {
            Keyring created = bootstrapKeyring();
            persistKeyring(created);
            return created;
        }
        try {
            JsonNode node = Jsons.mapper().readTree(Files.readString(keyFile, StandardCharsets.UTF_8));
            String active = node.path("active_kid").asText("");
            JsonNode keysNode = node.path("keys");
            LinkedHashMap<String, SecretKeySpec> keys = new LinkedHashMap<>();
            if (keysNode.isObject()) {
                keysNode.fieldNames().forEachRemaining(kid -> {
                    String rawBase64 = keysNode.path(kid).asText("");
                    if (kid == null || kid.isBlank() || rawBase64.isBlank()) {
                        return;
                    }
                    byte[] raw = Base64.getDecoder().decode(rawBase64);
                    keys.put(kid, new SecretKeySpec(raw, "AES"));
                });
            }
            if (keys.isEmpty()) {
                Keyring created = bootstrapKeyring();
                persistKeyring(created);
                return created;
            }
            // Keep service availability when active key id is missing after partial/manual key edits.
            if (!keys.containsKey(active)) {
                active = keys.keySet().iterator().next();
            }
            return new Keyring(active, keys);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load payload keyring: " + keyFile, e);
        }
    }

    private Keyring bootstrapKeyring() {
        byte[] raw = new byte[KEY_BYTES];
        secureRandom.nextBytes(raw);
        String kid = "k" + Instant.now().toEpochMilli();
        LinkedHashMap<String, SecretKeySpec> keys = new LinkedHashMap<>();
        keys.put(kid, new SecretKeySpec(raw, "AES"));
        return new Keyring(kid, keys);
    }

    private void persistKeyring(Keyring ring) {
        try {
            if (keyFile.getParent() != null) {
                Files.createDirectories(keyFile.getParent());
            }
            LinkedHashMap<String, String> keys = new LinkedHashMap<>();
            for (Map.Entry<String, SecretKeySpec> entry : ring.keys.entrySet()) {
                keys.put(entry.getKey(), Base64.getEncoder().encodeToString(entry.getValue().getEncoded()));
            }
            ObjectNode root = Jsons.mapper().createObjectNode();
            root.put("schema", "relaymesh.payload.keys.v1");
            root.put("active_kid", ring.activeKid);
            root.set("keys", Jsons.mapper().valueToTree(keys));
            Files.writeString(keyFile, Jsons.toJson(root), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to persist payload keyring: " + keyFile, e);
        }
    }

    private record Keyring(String activeKid, LinkedHashMap<String, SecretKeySpec> keys) {
    }

    public record RotationOutcome(String activeKid, int totalKeys, String keyFile) {
    }

    public record KeyringStatus(String activeKid, int totalKeys, String keyFile) {
    }
}
