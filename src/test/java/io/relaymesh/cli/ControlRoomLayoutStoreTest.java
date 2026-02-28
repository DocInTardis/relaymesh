package io.relaymesh.cli;

import com.fasterxml.jackson.databind.JsonNode;
import io.relaymesh.util.Jsons;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControlRoomLayoutStoreTest {
    @Test
    void normalizeProfileNameShouldValidateCharacters() {
        assertEquals("ops-main", ControlRoomLayoutStore.normalizeProfileName(" ops-main "));
        assertEquals("profile_1", ControlRoomLayoutStore.normalizeProfileName("profile_1"));
        assertEquals("", ControlRoomLayoutStore.normalizeProfileName("bad/name"));
        assertEquals("", ControlRoomLayoutStore.normalizeProfileName(""));
    }

    @Test
    void readWriteProfilesShouldRoundTrip() throws Exception {
        Path root = Files.createTempDirectory("relaymesh-layout-test-");
        LinkedHashMap<String, JsonNode> profiles = new LinkedHashMap<>();
        profiles.put("ops", Jsons.mapper().readTree("{\"panes\":[{\"view\":\"tasks\"}]}"));
        profiles.put("incident", Jsons.mapper().readTree("{\"panes\":[{\"view\":\"dead\"}]}"));

        ControlRoomLayoutStore.writeProfiles(root, profiles);
        LinkedHashMap<String, JsonNode> loaded = ControlRoomLayoutStore.readProfiles(root);

        assertEquals(2, loaded.size());
        assertTrue(loaded.containsKey("ops"));
        assertTrue(loaded.containsKey("incident"));
        assertEquals("tasks", loaded.get("ops").path("panes").get(0).path("view").asText());
        assertEquals("dead", loaded.get("incident").path("panes").get(0).path("view").asText());
    }

    @Test
    void writeProfilesShouldDropInvalidProfileName() throws Exception {
        Path root = Files.createTempDirectory("relaymesh-layout-invalid-test-");
        LinkedHashMap<String, JsonNode> profiles = new LinkedHashMap<>();
        profiles.put("valid_profile", Jsons.mapper().readTree("{\"panes\":[]}"));
        profiles.put("invalid/profile", Jsons.mapper().readTree("{\"panes\":[]}"));

        ControlRoomLayoutStore.writeProfiles(root, profiles);
        LinkedHashMap<String, JsonNode> loaded = ControlRoomLayoutStore.readProfiles(root);

        assertTrue(loaded.containsKey("valid_profile"));
        assertFalse(loaded.containsKey("invalid/profile"));
    }
}

