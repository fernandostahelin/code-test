DROP TABLE IF EXISTS target_products;
-- Target products table with tracking columns
CREATE TABLE IF NOT EXISTS target_products (
    -- New primary key different from source
    target_product_id SERIAL PRIMARY KEY,

    -- Original columns from source
    -- Original ID kept as unique constraint
    product_id INTEGER NOT NULL UNIQUE,
    product_name VARCHAR(100) NOT NULL,
    category_id VARCHAR(100),          -- Monitored for changes
    quantity_per_unit VARCHAR(100),      -- Monitored for changes
    unit_price DECIMAL(10, 2),           -- Monitored for changes
    units_in_stock INTEGER,             -- Monitored for changes
    units_in_order INTEGER,
    discontinued INTEGER,                -- Monitored for changes

    -- Audit columns
    created_by VARCHAR(100) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_by VARCHAR(100) NOT NULL,
    modified_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for better performance
CREATE INDEX IF NOT EXISTS idx_target_products_product_id
ON target_products (product_id);

CREATE INDEX IF NOT EXISTS idx_target_products_modified
ON target_products (modified_at);
