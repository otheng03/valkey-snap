package io.otheng.valkeysnap.consumer.events;

/**
 * Event emitted when a database is selected during RDB parsing.
 *
 * @param dbNumber the database number (0-15 typically)
 */
public record DbSelectEvent(
    int dbNumber
) {}
