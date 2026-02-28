package io.relaymesh.cli;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControlRoomPageTest {
    @Test
    void shouldLoadEmbeddedControlRoomPage() {
        String html = ControlRoomPage.html();
        assertFalse(html.isBlank(), "control-room page should not be blank");
        assertTrue(html.contains("RelayMesh Control Room"), "control-room title should be present");
        assertTrue(html.contains("control-room/layouts"), "profile API hooks should remain present");
    }
}

