package io.relaymesh.cli;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

final class ControlRoomPage {
    private static final String RESOURCE_PATH = "/web/control-room.html";
    private static final String HTML = load(RESOURCE_PATH);

    private ControlRoomPage() {
    }

    static String html() {
        return HTML;
    }

    private static String load(String resourcePath) {
        try (InputStream in = ControlRoomPage.class.getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new IllegalStateException("Missing control-room resource: " + resourcePath);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load control-room resource: " + resourcePath, e);
        }
    }
}

