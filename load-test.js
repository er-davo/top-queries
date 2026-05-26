import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend } from 'k6/metrics';

// Создаем кастомные тренды, чтобы k6 разделил latency по бакетам лимитов
const durationLimit10 = new Trend('http_req_duration_limit_10');
const durationLimit50 = new Trend('http_req_duration_limit_50');
const durationLimit100 = new Trend('http_req_duration_limit_100');

export const options = {
    stages: [
        { duration: '15s', target: 400 },  // Разгон
        { duration: '30s', target: 2000 }, // Полка средней нагрузки (~2000 RPS)
        { duration: '30s', target: 1400 }, 
        { duration: '15s', target: 0 },   // Снижение
    ],
    thresholds: {
        http_req_failed: ['rate<0.01'],   // Ошибок меньше 1%
        'http_req_duration{name:api-top}': ['p(95)<15'], // Общий p95 ниже 15мс
    },
};

// Пул лимитов, из которого k6 будет выбирать случайный элемент
const limits = [10, 50, 100];

export default function () {
    // Выбираем случайный лимит из массива
    const randomLimit = limits[Math.floor(Math.random() * limits.length)];
    const url = `http://localhost:8080/api/v1/top?limit=${randomLimit}`;

    // Передаем параметр tags, чтобы группировать общие метрики
    const res = http.get(url, { tags: { name: 'api-top' } });

    // Проверяем успешность
    const success = check(res, {
        'status is 200': (r) => r.status === 200,
        'body contains data': (r) => r.body.includes('"data"'),
    });

    // Направляем тайминг ответа в соответствующий кастомный тренд
    if (success) {
        if (randomLimit === 10) durationLimit10.add(res.timings.duration);
        else if (randomLimit === 50) durationLimit50.add(res.timings.duration);
        else if (randomLimit === 100) durationLimit100.add(res.timings.duration);
    }
}