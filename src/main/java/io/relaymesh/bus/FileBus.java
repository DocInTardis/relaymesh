package io.relaymesh.bus;

import io.relaymesh.config.RelayMeshConfig;
import io.relaymesh.model.MessageEnvelope;
import io.relaymesh.model.Priority;
import io.relaymesh.security.PayloadCrypto;
import io.relaymesh.util.Jsons;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public final class FileBus {
    private static final int DEFAULT_MAX_CONSECUTIVE_HIGH = 100;

    private final RelayMeshConfig config;
    private final PayloadCrypto payloadCrypto;
    private final List<Priority> weightedSequence;
    private final int maxConsecutiveHigh;
    private final AtomicLong lowStarvationCount;
    private int sequenceCursor = 0;
    private int consecutiveHighClaims = 0;

    public FileBus(RelayMeshConfig config) {
        this(config, DEFAULT_MAX_CONSECUTIVE_HIGH);
    }

    FileBus(RelayMeshConfig config, int maxConsecutiveHigh) {
        this(config, maxConsecutiveHigh, new PayloadCrypto(config.securityRoot().resolve("payload-keys.json")));
    }

    public FileBus(RelayMeshConfig config, int maxConsecutiveHigh, PayloadCrypto payloadCrypto) {
        this.config = config;
        this.payloadCrypto = payloadCrypto;
        this.weightedSequence = defaultWeightedSequence();
        this.maxConsecutiveHigh = Math.max(1, maxConsecutiveHigh);
        this.lowStarvationCount = new AtomicLong(0L);
    }

    public void enqueue(MessageEnvelope envelope, String payload) {
        Path payloadPath = Path.of(envelope.payloadPath());
        Path metaPath = config.metaDir().resolve(envelope.msgId() + ".meta.json");
        Path inboxPath = inboxPath(Priority.fromString(envelope.priority()))
                .resolve(envelope.createdAtMs() + "_" + envelope.msgId() + ".meta.json");
        try {
            String encrypted = payloadCrypto.encrypt(payload);
            Files.writeString(payloadPath, encrypted, StandardCharsets.UTF_8);
            String metaJson = Jsons.toJson(envelope);
            Files.writeString(metaPath, metaJson, StandardCharsets.UTF_8);
            Files.writeString(inboxPath, metaJson, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to enqueue message: " + envelope.msgId(), e);
        }
    }

    public void enqueueMetaOnly(MessageEnvelope envelope) {
        Path metaPath = config.metaDir().resolve(envelope.msgId() + ".meta.json");
        Path inboxPath = inboxPath(Priority.fromString(envelope.priority()))
                .resolve(envelope.createdAtMs() + "_" + envelope.msgId() + ".meta.json");
        try {
            String metaJson = Jsons.toJson(envelope);
            Files.writeString(metaPath, metaJson, StandardCharsets.UTF_8);
            Files.writeString(inboxPath, metaJson, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to enqueue retry message: " + envelope.msgId(), e);
        }
    }

    public synchronized Optional<ClaimedMessage> claimNext(String workerId) {
        if (consecutiveHighClaims >= maxConsecutiveHigh) {
            Optional<ClaimedMessage> forcedLow = claimFromPriority(Priority.LOW, workerId);
            if (forcedLow.isPresent()) {
                consecutiveHighClaims = 0;
                lowStarvationCount.incrementAndGet();
                return forcedLow;
            }
        }
        for (int i = 0; i < weightedSequence.size(); i++) {
            Priority nextPriority = weightedSequence.get(sequenceCursor % weightedSequence.size());
            sequenceCursor++;
            Optional<ClaimedMessage> claimed = claimFromPriority(nextPriority, workerId);
            if (claimed.isPresent()) {
                if (nextPriority == Priority.HIGH) {
                    consecutiveHighClaims++;
                } else {
                    consecutiveHighClaims = 0;
                }
                return claimed;
            }
        }
        return Optional.empty();
    }

    public long lowStarvationCount() {
        return lowStarvationCount.get();
    }

    public String readPayload(String payloadPath) {
        try {
            String raw = Files.readString(Path.of(payloadPath), StandardCharsets.UTF_8);
            return payloadCrypto.decryptIfNeeded(raw);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read payload: " + payloadPath, e);
        }
    }

    public PayloadCrypto.KeyringStatus payloadKeyStatus() {
        return payloadCrypto.status();
    }

    public PayloadCrypto.RotationOutcome rotatePayloadKey() {
        return payloadCrypto.rotate();
    }

    public void completeSuccess(ClaimedMessage message) {
        Path dailyDoneDir = config.doneRoot().resolve(LocalDate.now().toString());
        try {
            Files.createDirectories(dailyDoneDir);
            Files.move(
                    message.processingFile(),
                    dailyDoneDir.resolve(message.processingFile().getFileName().toString()),
                    StandardCopyOption.REPLACE_EXISTING
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to move message to done directory", e);
        }
    }

    public void completeFailure(ClaimedMessage message) {
        try {
            Files.move(
                    message.processingFile(),
                    config.deadRoot().resolve(message.processingFile().getFileName().toString()),
                    StandardCopyOption.REPLACE_EXISTING
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to move message to dead directory", e);
        }
    }

    public void completeRetry(ClaimedMessage message) {
        try {
            Files.move(
                    message.processingFile(),
                    config.retryRoot().resolve(message.processingFile().getFileName().toString()),
                    StandardCopyOption.REPLACE_EXISTING
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to move message to retry directory", e);
        }
    }

    private Optional<ClaimedMessage> claimFromPriority(Priority priority, String workerId) {
        Path inbox = inboxPath(priority);
        List<Path> files = listMetaFiles(inbox);
        if (files.isEmpty()) {
            return Optional.empty();
        }

        Path candidate = files.get(0);
        Path workerDir = config.processingDir().resolve(workerId);
        try {
            Files.createDirectories(workerDir);
            Path claimed = workerDir.resolve(candidate.getFileName().toString());
            try {
                Files.move(candidate, claimed, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException ignored) {
                Files.move(candidate, claimed, StandardCopyOption.REPLACE_EXISTING);
            }
            MessageEnvelope envelope = Jsons.mapper().readValue(claimed.toFile(), MessageEnvelope.class);
            return Optional.of(new ClaimedMessage(envelope, claimed));
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    private List<Path> listMetaFiles(Path dir) {
        List<Path> files = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.meta.json")) {
            for (Path path : stream) {
                files.add(path);
            }
        } catch (IOException ignored) {
            return files;
        }
        files.sort(Comparator.comparing(path -> path.getFileName().toString()));
        return files;
    }

    private Path inboxPath(Priority priority) {
        return switch (priority) {
            case HIGH -> config.inboxHigh();
            case NORMAL -> config.inboxNormal();
            case LOW -> config.inboxLow();
        };
    }

    private List<Priority> defaultWeightedSequence() {
        List<Priority> sequence = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            sequence.add(Priority.HIGH);
        }
        for (int i = 0; i < 3; i++) {
            sequence.add(Priority.NORMAL);
        }
        sequence.add(Priority.LOW);
        return sequence;
    }

    public record ClaimedMessage(MessageEnvelope envelope, Path processingFile) {
    }
}
