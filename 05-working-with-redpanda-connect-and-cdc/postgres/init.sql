CREATE TABLE orders
(
    id          BIGSERIAL PRIMARY KEY,
    customer_id BIGINT         NOT NULL,
    amount      NUMERIC(10, 2) NOT NULL,
    status      VARCHAR(30)    NOT NULL DEFAULT 'CREATED',
    created_at  TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

INSERT INTO orders (customer_id, amount)
VALUES
    (1001, 99.95),
    (1002, 149.50),
    (1003, 12.99);