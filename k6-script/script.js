import http from 'k6/http';
import { check } from 'k6';

export const options = {
  vus: 1,
  iterations: 20000,
};

export default function () {
  const id = Math.floor(Math.random() * 20000) + 1;

  const response = http.get(
    `http://localhost:8080/api/call/grpc/${id}`,
    {
      timeout: '60s',
    }
  );

  check(response, {
    'status is 200': (res) => res.status === 200,
  });
}
