<script setup lang="ts">
import { courses } from "#shared/courses";

type Completion = {
  id: string;
  learner: string;
  course_title: string;
  completed_at: string;
};
type Certificate = {
  completion_id: string;
  certificate_id: string;
  learner: string;
  course_title: string;
  issued_at: string;
};
type Receipt = {
  sequence: string;
  completion_id: string;
  idempotency_key: string;
  outcome: string;
  received_at: string;
};
type Receiver = {
  mode: string;
  certificates: Certificate[];
  receipts: Receipt[];
};
type Status = { completions: Completion[]; receiver: Receiver | null };

const state = ref<Status>({ completions: [], receiver: null });
const learner = ref("Alex Morgan");
const selected = ref<string>(courses[0].id);
const busy = ref(false);
const changingMode = ref(false);
const message = ref("");
const error = ref("");
const connected = ref(false);
let pendingRequest: {
  id: string;
  learner: string;
  courseId: string;
  rollback: boolean;
} | null = null;
let timer: ReturnType<typeof setTimeout>;
let stopped = false;

async function refresh() {
  try {
    state.value = await $fetch<Status>("/api/status", {
      retry: 0,
      timeout: 5000,
    });
    connected.value = true;
  } catch {
    connected.value = false;
  }
}
async function poll() {
  await refresh();
  if (!stopped) timer = setTimeout(poll, 1500);
}
onMounted(() => {
  void poll();
});
onBeforeUnmount(() => {
  stopped = true;
  clearTimeout(timer);
});

async function complete(rollback = false) {
  busy.value = true;
  error.value = "";
  message.value = "";
  // Retain this request on an ambiguous network failure; retry with the same ID.
  pendingRequest ??= {
    id: crypto.randomUUID(),
    learner: learner.value.trim(),
    courseId: selected.value,
    rollback,
  };
  try {
    const result = await $fetch("/api/completions", {
      method: "POST",
      body: pendingRequest,
      retry: 0,
      timeout: 10_000,
    });
    message.value = result.rolledBack
      ? "Rolled back. Neither the completion nor its event was saved."
      : "Completion and event committed together. WriteRelay will handle delivery.";
    pendingRequest = null;
    await refresh();
  } catch {
    error.value =
      "Could not confirm completion. Retry to safely check the same request; its original learner and course will be used.";
  } finally {
    busy.value = false;
  }
}
async function setMode(mode: string) {
  changingMode.value = true;
  error.value = "";
  try {
    await $fetch("/api/receiver", { method: "POST", body: { mode }, retry: 0 });
    await refresh();
  } catch {
    error.value =
      "Cannot reach the certificate service. Start its container, then try again.";
  } finally {
    changingMode.value = false;
  }
}

const mode = computed(() => state.value.receiver?.mode ?? "offline");
const certificates = computed(() => state.value.receiver?.certificates ?? []);
const receipts = computed(() => state.value.receiver?.receipts ?? []);
const duplicateCount = computed(
  () =>
    receipts.value.filter((receipt) => receipt.outcome === "duplicate").length,
);
const latest = computed(() => certificates.value[0]);
const modeName: Record<string, string> = {
  normal: "Accepting events",
  unavailable: "Returning 503",
  reject: "Returning 422",
  "drop-response": "Next response will be lost",
  offline: "Service unreachable",
};
const outcomeName: Record<string, string> = {
  issued: "Certificate issued",
  duplicate: "Duplicate handled",
  unavailable: "Retryable failure · 503",
  rejected: "Rejected · 422",
  "response-lost": "Issued · response lost",
};
function time(value: string) {
  return new Date(value).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}
function hasCertificate(id: string) {
  return certificates.value.some(
    (certificate) => certificate.completion_id === id,
  );
}
</script>

<template>
  <div class="lab">
    <header class="masthead">
      <a class="brand" href="/" aria-label="WriteRelay learning lab"
        ><span class="brand-mark">↗</span> WriteRelay
        <span class="brand-divider">/</span>
        <span class="lab-label">learning lab</span></a
      >
      <span class="local-badge"><span class="dot" /> LOCAL EXAMPLE</span>
    </header>

    <main>
      <section class="intro">
        <div>
          <p class="eyebrow">A SMALL LMS. A REAL DELIVERY FLOW.</p>
          <h1>Complete a course.<br /><em>Follow the event.</em></h1>
        </div>
        <p class="intro-copy">
          A database write, a durable event, and a certificate.<br />See what
          happens when the happy path takes a detour.
        </p>
      </section>

      <section class="flow" aria-label="How the example works">
        <div>
          <span class="step-number">01</span>
          <div>
            <strong>Nuxt LMS</strong
            ><small>Completion + event in one transaction</small>
          </div>
        </div>
        <span class="flow-arrow" aria-hidden="true">→</span>
        <div>
          <span class="step-number">02</span>
          <div>
            <strong>WriteRelay</strong
            ><small>Durable capture, delivery, and retries</small>
          </div>
        </div>
        <span class="flow-arrow" aria-hidden="true">→</span>
        <div>
          <span class="step-number">03</span>
          <div>
            <strong>Certificate service</strong
            ><small>Certificate + key in one transaction</small>
          </div>
        </div>
      </section>

      <div v-if="!connected" class="notice warning" role="status">
        Connecting to the LMS database. If this persists, check
        <code>docker compose logs lms</code>.
      </div>
      <div v-if="error" class="notice warning" role="alert">{{ error }}</div>
      <div v-if="message" class="notice" role="status">{{ message }}</div>

      <div class="workspace">
        <section class="course-panel" aria-labelledby="course-heading">
          <div class="section-heading">
            <div>
              <p class="eyebrow">THE PRODUCER</p>
              <h2 id="course-heading">Your next small win.</h2>
            </div>
            <span class="tag">Nuxt + PostgreSQL</span>
          </div>
          <label class="field-label" for="learner">Demo learner</label>
          <input
            id="learner"
            v-model="learner"
            maxlength="80"
            autocomplete="off"
            :disabled="busy"
          />
          <fieldset>
            <legend>Choose a course</legend>
            <label
              v-for="course in courses"
              :key="course.id"
              class="course"
              :class="{ selected: selected === course.id }"
            >
              <input
                v-model="selected"
                type="radio"
                name="course"
                :value="course.id"
                :disabled="busy"
              />
              <span class="course-number">{{ course.number }}</span>
              <span class="course-content"
                ><small>{{ course.category }} · {{ course.duration }}</small
                ><strong>{{ course.title }}</strong></span
              >
              <span class="course-radio" aria-hidden="true" />
            </label>
          </fieldset>
          <button
            class="primary"
            :disabled="busy || !learner.trim() || !connected"
            @click="complete(false)"
          >
            {{ busy ? "Saving completion…" : "Complete course" }}
            <span aria-hidden="true">↗</span>
          </button>
          <button
            class="text-button"
            :disabled="busy || !learner.trim() || !connected"
            @click="complete(true)"
          >
            Try a rollback instead
          </button>
          <p class="fine-print">
            Each click starts a new demo completion. A rollback saves neither
            the completion nor the notification intent.
          </p>
        </section>

        <section class="receiver-panel" aria-labelledby="receiver-heading">
          <div class="section-heading">
            <div>
              <p class="eyebrow">THE RECEIVER</p>
              <h2 id="receiver-heading">Make room for failure.</h2>
            </div>
          </div>
          <p class="panel-copy">
            Change how the certificate service responds, then complete a course.
            WriteRelay handles retryable failures automatically.
          </p>
          <div class="receiver-state" :class="{ unhealthy: mode !== 'normal' }">
            <span class="dot" />{{ modeName[mode] }}
          </div>
          <div class="mode-buttons" aria-label="Certificate service behavior">
            <button
              :aria-pressed="mode === 'normal'"
              :disabled="changingMode || mode === 'offline'"
              @click="setMode('normal')"
            >
              Normal
            </button>
            <button
              :aria-pressed="mode === 'unavailable'"
              :disabled="changingMode || mode === 'offline'"
              @click="setMode('unavailable')"
            >
              Simulate outage
            </button>
            <button
              :aria-pressed="mode === 'drop-response'"
              :disabled="changingMode || mode === 'offline'"
              @click="setMode('drop-response')"
            >
              Lose next response
            </button>
            <button
              :aria-pressed="mode === 'reject'"
              :disabled="changingMode || mode === 'offline'"
              @click="setMode('reject')"
            >
              Reject events
            </button>
          </div>
          <p class="mode-help" v-if="mode === 'normal'">
            Returns success after the certificate and deduplication key are
            committed.
          </p>
          <p class="mode-help" v-else-if="mode === 'unavailable'">
            Returns 503. Switch back to Normal within about 40 seconds to see
            automatic recovery before retries run out.
          </p>
          <p class="mode-help" v-else-if="mode === 'drop-response'">
            The next event creates a certificate, then loses its response. A
            retry uses the same key and creates no second certificate.
          </p>
          <p class="mode-help" v-else-if="mode === 'reject'">
            Returns 422. Delivery becomes a dead letter. Restore Normal, then
            use the redrive command in the example README.
          </p>
          <p class="mode-help" v-else>
            The container is stopped or unreachable. Existing records remain in
            its database. Restart it to reconnect.
          </p>

          <div class="certificate-preview" :class="{ empty: !latest }">
            <div class="certificate-top">
              <span>RELAY ACADEMY</span
              ><span class="seal" aria-hidden="true">✳</span>
            </div>
            <p class="certificate-kicker">CERTIFICATE OF COMPLETION</p>
            <template v-if="latest"
              ><h3>{{ latest.learner }}</h3>
              <p>{{ latest.course_title }}</p>
              <small
                >Issued {{ time(latest.issued_at) }} ·
                {{ latest.certificate_id.slice(0, 8) }}</small
              ></template
            >
            <template v-else
              ><h3>Your certificate<br />starts here.</h3>
              <p>Complete a course to see the result.</p>
              <small>Saved by the receiving service</small></template
            >
          </div>
        </section>
      </div>

      <section class="ledger" aria-labelledby="ledger-heading">
        <div class="section-heading">
          <div>
            <p class="eyebrow">THE EVIDENCE</p>
            <h2 id="ledger-heading">Every step leaves a record.</h2>
          </div>
          <span class="live-label"
            ><span class="dot" /> Updates automatically</span
          >
        </div>
        <div class="stats">
          <div>
            <strong>{{ state.completions.length }}</strong
            ><span>Recent completions</span>
          </div>
          <div>
            <strong>{{ state.receiver ? certificates.length : "—" }}</strong
            ><span>Recent certificates</span>
          </div>
          <div>
            <strong>{{ state.receiver ? duplicateCount : "—" }}</strong
            ><span>Recent duplicates handled</span>
          </div>
        </div>
        <div class="table-wrap">
          <table>
            <caption class="sr-only">
              Recent course completions and certificate outcomes
            </caption>
            <thead>
              <tr>
                <th>Learner / course</th>
                <th>Completion ID</th>
                <th>LMS database</th>
                <th>Certificate</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="completion in state.completions" :key="completion.id">
                <td>
                  <strong>{{ completion.learner }}</strong
                  ><small>{{ completion.course_title }}</small>
                </td>
                <td>
                  <code class="event-id">{{ completion.id }}</code>
                </td>
                <td><span class="status saved">Saved</span></td>
                <td>
                  <span
                    class="status"
                    :class="hasCertificate(completion.id) ? 'saved' : 'waiting'"
                    >{{
                      !state.receiver
                        ? "Unknown · offline"
                        : hasCertificate(completion.id)
                          ? "Issued"
                          : "Not issued yet"
                    }}</span
                  >
                </td>
              </tr>
              <tr v-if="!state.completions.length">
                <td colspan="4" class="empty-row">
                  Your first completion will appear here. Everything stays
                  local.
                </td>
              </tr>
            </tbody>
          </table>
        </div>
        <details class="receipt-details">
          <summary>
            Receiver request history
            <span>{{ receipts.length }} recent requests</span>
          </summary>
          <p class="fine-print">
            These are receiver observations. Use
            <code>spool deliveries</code> for WriteRelay's authoritative
            pending, delivered, and dead-letter states. Requests that never
            reach the receiver cannot appear here.
          </p>
          <div
            v-for="receipt in receipts"
            :key="receipt.sequence"
            class="receipt"
          >
            <span>{{ time(receipt.received_at) }}</span
            ><strong>{{ outcomeName[receipt.outcome] }}</strong
            ><code>{{ receipt.completion_id }}</code
            ><small>Key: {{ receipt.idempotency_key }}</small>
          </div>
          <p v-if="!receipts.length" class="fine-print">
            No received requests to display.
          </p>
        </details>
      </section>
      <footer>
        <span>WriteRelay / local learning lab</span
        ><span>One committed event. At-least-once delivery.</span>
      </footer>
    </main>
  </div>
</template>

<style>
@import "./assets/main.css";
</style>
