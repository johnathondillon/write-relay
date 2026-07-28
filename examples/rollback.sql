BEGIN;

INSERT INTO orders (id, status)
VALUES ('ord_rolled_back', 'paid')
ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status;

SELECT writerelay.emit(
    '{
      "specversion": "1.0",
      "id": "evt-example-rolled-back",
      "source": "urn:service:billing",
      "type": "order.paid",
      "subject": "ord_rolled_back"
    }'::jsonb
);

ROLLBACK;

