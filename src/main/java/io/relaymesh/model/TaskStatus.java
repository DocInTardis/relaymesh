package io.relaymesh.model;

public enum TaskStatus {
    PENDING,
    RUNNING,
    SUCCESS,
    FAILED,
    RETRYING,
    TIMEOUT,
    ABANDONED,
    DEAD_LETTER,
    CANCELLED
}
