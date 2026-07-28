BEGIN;

INSERT INTO orders (id, status)
VALUES ('ord_committed', 'paid')
ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status;

SELECT writerelay.emit(
    '{
      "specversion": "1.0",
      "id": "evt-example-committed",
      "source": "urn:service:billing",
      "type": "order.paid",
      "subject": "ord_committed",
      "datacontenttype": "application/json",
      "data": {"amount": 12900, "currency": "USD"}
    }'::jsonb
);

COMMIT;

