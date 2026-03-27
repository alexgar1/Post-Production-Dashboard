CREATE TABLE IF NOT EXISTS monday_users (
    user_id BIGINT PRIMARY KEY,
    username TEXT NOT NULL,
    pay_rate NUMERIC(6, 2)
);

CREATE TABLE IF NOT EXISTS listing_items (
    item_id BIGINT PRIMARY KEY,
    board_id BIGINT NOT NULL,
    name TEXT,
    column_values JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS listing_subitems (
    subitem_id BIGINT PRIMARY KEY,
    parent_item_id BIGINT NOT NULL REFERENCES listing_items (item_id) ON DELETE CASCADE,
    name TEXT,
    column_values JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS social_items (
    item_id BIGINT PRIMARY KEY,
    board_id BIGINT NOT NULL,
    name TEXT,
    column_values JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS social_subitems (
    subitem_id BIGINT PRIMARY KEY,
    parent_item_id BIGINT NOT NULL REFERENCES social_items (item_id) ON DELETE CASCADE,
    name TEXT,
    column_values JSONB NOT NULL DEFAULT '{}'::jsonb
);
