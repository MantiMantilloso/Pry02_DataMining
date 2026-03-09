

SELECT
    payment_type_key::INT,
    payment_type,
    description
FROM (VALUES
    (1, 'card',    'Credit card'),
    (2, 'cash',    'Cash'),
    (3, 'other',   'No charge / Voided'),
    (4, 'unknown', 'Unknown or unspecified')
) AS t(payment_type_key, payment_type, description)