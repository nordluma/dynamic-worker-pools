-- Add migration script here
CREATE TABLE IF NOT EXISTS messages(
    id INTEGER PRIMARY KEY,
    content TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
