CREATE DATABASE IF NOT EXISTS app;
USE app;

CREATE TABLE IF NOT EXISTS users (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  email VARCHAR(255) NOT NULL,
  full_name VARCHAR(255) NOT NULL,
  status VARCHAR(50) NOT NULL,
  is_active TINYINT(1) NOT NULL DEFAULT 1,
  tags VARCHAR(255) DEFAULT NULL,
  metadata JSON DEFAULT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  sku VARCHAR(100) NOT NULL,
  name VARCHAR(255) NOT NULL,
  price DECIMAL(10, 2) NOT NULL,
  is_published TINYINT(1) NOT NULL DEFAULT 1,
  categories JSON DEFAULT NULL,
  attributes JSON DEFAULT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL
);

INSERT INTO users (email, full_name, status, is_active, tags, metadata, created_at, updated_at)
VALUES
  (
    'alice@example.com',
    'Alice Nguyen',
    'active',
    1,
    'vip,newsletter',
    JSON_OBJECT('city', 'Hanoi', 'tier', 'gold'),
    NOW(),
    NOW()
  ),
  (
    'bob@example.com',
    'Bob Tran',
    'inactive',
    0,
    'trial',
    JSON_OBJECT('city', 'Da Nang', 'tier', 'silver'),
    NOW(),
    NOW()
  );

INSERT INTO products (sku, name, price, is_published, categories, attributes, created_at, updated_at)
VALUES
  (
    'SKU-001',
    'Mechanical Keyboard',
    129.99,
    1,
    JSON_ARRAY('peripherals', 'keyboards'),
    JSON_OBJECT('switch', 'brown', 'layout', '75%'),
    NOW(),
    NOW()
  ),
  (
    'SKU-002',
    'Wireless Mouse',
    59.50,
    1,
    JSON_ARRAY('peripherals', 'mouse'),
    JSON_OBJECT('dpi', 26000, 'wireless', true),
    NOW(),
    NOW()
  );
