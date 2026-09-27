import http from 'k6/http';
import { check } from 'k6';
export const options = { vus: 10, duration: '30s', thresholds: { http_req_failed: ['rate<0.01'], http_req_duration: ['p(95)<500'] } };
export default function () {
  const key = `${__VU}-${__ITER}-${Date.now()}`;
  const r = http.post(`${__ENV.BASE_URL || 'http://localhost:8080'}/v1/tasks`, JSON.stringify({ type: 'report.aggregate', payload: { numbers: [1,2,3] } }), { headers: { 'Content-Type':'application/json', 'X-API-Key':__ENV.API_KEY, 'Idempotency-Key':key } });
  check(r, { 'created': r => r.status === 201 });
}
