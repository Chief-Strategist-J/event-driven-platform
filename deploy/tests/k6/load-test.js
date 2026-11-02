import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '30s', target: 50 },
    { duration: '1m', target: 200 },
    { duration: '30s', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<150', 'p(99)<300'],
    http_req_failed: ['rate<0.01'],
  },
};

export default function () {
  const url = __ENV.TARGET_URL || 'http://localhost:8080/events.v1.EventService/PublishEvent';
  const payload = JSON.stringify({
    aggregate_id: `agg-${__VU}-${__ITER}`,
    event_type: 'OrderCreated',
    data: {
      amount: 150.00,
      currency: 'USD',
      customer_id: 'cust-994',
    },
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Connect-Protocol-Version': '1',
    },
  };

  const res = http.post(url, payload, params);
  check(res, {
    'status is 200': (r) => r.status === 200,
  });

  sleep(0.05);
}
