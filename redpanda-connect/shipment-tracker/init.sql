-- Transactional "operations" side of the house.
-- Shipments are created and updated here; Redpanda Connect pulls them out.

CREATE TABLE IF NOT EXISTS shipments (
    shipment_id      SERIAL PRIMARY KEY,
    customer_id      INT         NOT NULL,
    customer_name    TEXT        NOT NULL,   -- sensitive
    customer_email   TEXT        NOT NULL,   -- sensitive
    customer_address TEXT        NOT NULL,   -- sensitive
    carrier          TEXT        NOT NULL,
    tracking_number  TEXT        NOT NULL,
    origin           TEXT        NOT NULL,
    destination      TEXT        NOT NULL,
    package_details  TEXT        NOT NULL,
    shipment_status  TEXT        NOT NULL,   -- label_created | picked_up | in_transit | out_for_delivery | delivered | exception
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO shipments
    (customer_id, customer_name, customer_email, customer_address, carrier, tracking_number, origin, destination, package_details, shipment_status)
VALUES
    (1, 'John Doe',      'john.doe@example.com',   '123 Main St, Springfield, IL',  'UPS',    '1Z999AA10123456784', 'Chicago, IL',      'Springfield, IL',   '1x 55" OLED TV',              'label_created'),
    (2, 'Jane Smith',    'jane.smith@example.com', '456 Oak Ave, Austin, TX',       'FedEx',  '7712-3345-9981',     'Memphis, TN',      'Austin, TX',        '1x espresso machine',         'picked_up'),
    (3, 'Amir Haddad',   'amir.h@example.com',     '78 Elm Rd, Portland, OR',       'DHL',    'JD0002123456789',    'Los Angeles, CA',  'Portland, OR',      '3x hardcover books',          'in_transit'),
    (4, 'Wei Chen',      'wei.chen@example.com',   '900 Pine Blvd, Seattle, WA',    'USPS',   '9400-1000-0000-1234','Seattle, WA',      'Seattle, WA',       '1x mechanical keyboard',      'out_for_delivery'),
    (5, 'Sofia Rossi',   'sofia.r@example.com',    '12 Market St, Denver, CO',      'UPS',    '1Z999AA10999888777', 'Salt Lake City, UT','Denver, CO',       '2x running shoes',            'delivered'),
    (6, 'Liam O''Brien', 'liam.o@example.com',     '55 River Ln, Boston, MA',       'FedEx',  '7712-8899-0011',     'Newark, NJ',       'Boston, MA',        '1x fragile glassware set',    'exception');
