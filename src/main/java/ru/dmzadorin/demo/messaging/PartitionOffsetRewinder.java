package ru.dmzadorin.demo.messaging;

import java.util.Map;

public interface PartitionOffsetRewinder {
    void rewindPartitionOffset(Map<Integer, Long> partitionsOffset);
}
