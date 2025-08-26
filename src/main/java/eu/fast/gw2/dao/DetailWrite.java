package eu.fast.gw2.dao;

/** DTO for overlay_detail upserts. */
public record DetailWrite(long featureId, String key, String tier, String json) {
}
