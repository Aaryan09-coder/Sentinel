package com.sentinel.common;

import java.time.Instant;

public record MetricEvent(String sourceId, String asset, String metric, double value, Instant timestamp) {}
