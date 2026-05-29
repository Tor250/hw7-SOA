import http from "k6/http";
import { check, sleep } from "k6";

const producerBaseUrl = __ENV.PRODUCER_URL || "http://producer:8000";

export const options = {
  scenarios: {
    movie_events: {
      executor: "constant-vus",
      vus: 10,
      duration: "30s",
      gracefulStop: "5s",
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    'checks{check:"producer health is 200"}': ["rate>0.99"],
    'checks{check:"event accepted"}': ["rate>0.99"],
    "http_req_duration{endpoint:events}": ["p(95)<500"],
  },
};

function buildPayload() {
  const eventTypes = ["VIEW_STARTED", "VIEW_FINISHED", "VIEW_PAUSED", "VIEW_RESUMED", "LIKED", "SEARCHED"];
  const deviceTypes = ["MOBILE", "DESKTOP", "TV", "TABLET"];
  const eventType = eventTypes[Math.floor(Math.random() * eventTypes.length)];
  const isViewingEvent = eventType === "VIEW_STARTED" || eventType === "VIEW_FINISHED" || eventType === "VIEW_PAUSED" || eventType === "VIEW_RESUMED";

  return {
    user_id: `load-user-${Math.floor(Math.random() * 100000)}`,
    movie_id: `load-movie-${Math.floor(Math.random() * 10000)}`,
    event_type: eventType,
    timestamp: new Date().toISOString(),
    device_type: deviceTypes[Math.floor(Math.random() * deviceTypes.length)],
    session_id: `load-session-${Math.floor(Math.random() * 100000)}`,
    progress_seconds: isViewingEvent ? Math.floor(Math.random() * 3600) : 0,
  };
}

export default function () {
  const healthResponse = http.get(`${producerBaseUrl}/health`, {
    tags: { endpoint: "health" },
  });
  check(healthResponse, {
    "producer health is 200": (response) => response.status === 200,
  });

  const eventResponse = http.post(`${producerBaseUrl}/events`, JSON.stringify(buildPayload()), {
    headers: { "Content-Type": "application/json" },
    tags: { endpoint: "events" },
  });
  check(eventResponse, {
    "event accepted": (response) => response.status === 200,
  });

  sleep(1);
}

export function handleSummary(data) {
  return {
    "summary.json": JSON.stringify(data, null, 2),
  };
}
