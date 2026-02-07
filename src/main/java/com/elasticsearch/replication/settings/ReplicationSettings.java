package com.elasticsearch.replication.settings;

import com.elasticsearch.replication.transport.TransportConfig;
import org.elasticsearch.common.settings.Setting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * All cluster-level settings for the replication plugin.
 */
public class ReplicationSettings {

    private static final String PREFIX = "replication.";

    // Plugin role: "producer" (active cluster) or "consumer" (passive cluster) or "disabled"
    public static final Setting<String> ROLE = Setting.simpleString(
        PREFIX + "role", "disabled", Setting.Property.NodeScope, Setting.Property.Dynamic);

    // CDC settings (producer side)
    public static final Setting<Integer> BATCH_SIZE = Setting.intSetting(
        PREFIX + "batch.size", 1000, 1, 100000,
        Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<Long> BATCH_AGE_MS = Setting.longSetting(
        PREFIX + "batch.age_ms", 5000L, 100L,
        Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<Long> BATCH_BYTES = Setting.longSetting(
        PREFIX + "batch.max_bytes", 5 * 1024 * 1024L, 1024L,
        Setting.Property.NodeScope, Setting.Property.Dynamic);

    // Index include/exclude patterns (comma-separated)
    public static final Setting<String> INCLUDE_INDICES = Setting.simpleString(
        PREFIX + "indices.include", "", Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<String> EXCLUDE_INDICES = Setting.simpleString(
        PREFIX + "indices.exclude", "", Setting.Property.NodeScope, Setting.Property.Dynamic);

    // Replay settings (consumer side)
    public static final Setting<Integer> REPLAY_BULK_SIZE = Setting.intSetting(
        PREFIX + "replay.bulk_size", 500, 1, 10000,
        Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<Integer> REPLAY_POLL_BATCH = Setting.intSetting(
        PREFIX + "replay.poll_batch", 10, 1, 10,
        Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<Integer> REPLAY_POLL_WAIT_SEC = Setting.intSetting(
        PREFIX + "replay.poll_wait_seconds", 10, 0, 20,
        Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<Integer> REPLAY_WORKER_THREADS = Setting.intSetting(
        PREFIX + "replay.worker_threads", 4, 1, 16,
        Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<>();
        settings.addAll(Arrays.asList(
            ROLE, BATCH_SIZE, BATCH_AGE_MS, BATCH_BYTES,
            INCLUDE_INDICES, EXCLUDE_INDICES,
            REPLAY_BULK_SIZE, REPLAY_POLL_BATCH, REPLAY_POLL_WAIT_SEC,
            REPLAY_WORKER_THREADS
        ));
        settings.addAll(TransportConfig.getSettings());
        return settings;
    }
}
