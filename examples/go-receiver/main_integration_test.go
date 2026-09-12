//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	writerelay "github.com/johnathondillon/write-relay/sdk/go"
)

func TestReceiverHTTPRetries(t *testing.T) {
	dsn := os.Getenv("WRITERELAY_INTEGRATION_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres-dev-password@localhost:5432/writerelay?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	table := writerelay.InboxTable{Table: "wr_http_inbox_" + suffix}
	orders := "wr_http_orders_" + suffix
	ddl, err := writerelay.InboxTableSQL(table)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, ddl); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, err := pool.Exec(cleanup, "DROP TABLE IF EXISTS "+orders+", "+table.Table); err != nil {
			t.Error(err)
		}
	}()
	if _, err = pool.Exec(ctx, "CREATE TABLE "+orders+" (source text,event_id text,order_id text,PRIMARY KEY(source,event_id))"); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(receiver{pool: pool, table: table, orders: orders})
	defer server.Close()
	client := server.Client()
	client.Timeout = 5 * time.Second
	send := func(key string, body []byte, mode, token string) (int, string, error) {
		request, err := http.NewRequestWithContext(ctx, "POST", server.URL+"/webhook", bytes.NewReader(body))
		if err != nil {
			return 0, "", err
		}
		request.Close = true
		request.Header.Set("Idempotency-Key", key)
		request.Header.Set("Authorization", token)
		request.Header.Set("X-Example-Failure", mode)
		response, err := client.Do(request)
		if err != nil {
			return 0, "", err
		}
		defer response.Body.Close()
		payload, err := io.ReadAll(response.Body)
		return response.StatusCode, string(payload), err
	}
	count := func(key, id string, want int) {
		t.Helper()
		for _, query := range []struct{ sql, arg string }{
			{"SELECT count(*) FROM " + table.Table + " WHERE idempotency_key=$1", key},
			{"SELECT count(*) FROM " + orders + " WHERE event_id=$1", id},
		} {
			var n int
			if err := pool.QueryRow(ctx, query.sql, query.arg).Scan(&n); err != nil || n != want {
				t.Fatal(n, want, err)
			}
		}
	}
	for _, mode := range []string{"rollback", "drop-response"} {
		t.Run(mode, func(t *testing.T) {
			body, err := json.Marshal(orderEvent{SpecVersion: "1.0", ID: mode, Source: "urn:test", Type: "order.paid", Subject: "order-1"})
			if err != nil {
				t.Fatal(err)
			}
			key := fmt.Sprintf("%x", sha256.Sum256([]byte(mode)))
			code, _, err := send(key, body, "", "wrong-token")
			if err != nil || code != 401 {
				t.Fatal(code, err)
			}
			count(key, mode, 0)
			code, _, err = send("bad-key", body, "", "Bearer local-example-token")
			if err != nil || code != 400 {
				t.Fatal(code, err)
			}
			count(key, mode, 0)
			code, _, err = send(key, body, mode, "Bearer local-example-token")
			if mode == "rollback" {
				if err != nil || code != 503 {
					t.Fatal(code, err)
				}
				count(key, mode, 0)
			} else {
				if err == nil {
					t.Fatal("expected lost response")
				}
				count(key, mode, 1)
			}
			code, payload, err := send(key, body, "", "Bearer local-example-token")
			want := writerelay.InboxProcessed
			if mode == "drop-response" {
				want = writerelay.InboxDuplicate
			}
			var result struct {
				Status writerelay.InboxResult `json:"status"`
			}
			if err != nil || code != 200 || json.Unmarshal([]byte(payload), &result) != nil || result.Status != want {
				t.Fatal(code, payload, err)
			}
			count(key, mode, 1)
			code, _, err = send(key, append(body, ' '), "", "Bearer local-example-token")
			if err != nil || code != 409 {
				t.Fatal(code, err)
			}
			count(key, mode, 1)
		})
	}
}
