# Proxy Load Tester

An asynchronous HTTP load testing tool with built-in proxy rotation, written in Python.

Features rotating proxies, random User-Agents, rate limiting, live statistics, and an interactive menu — no CLI experience required.

## Features

- **Async & fast** — built on `asyncio` + `aiohttp`, hundreds of concurrent requests from a single process
- **Proxy support** — HTTP, HTTPS (CONNECT), SOCKS4 and SOCKS5 proxies
- **Auto proxy fetching** — pulls free proxy lists from multiple public sources and merges them with your existing lists (de-duplicated)
- **Proxy health check** — tests all proxies in parallel before the run and drops dead ones; live lists are saved to disk for reuse
- **Self-healing pool** — proxies that fail repeatedly during a run are removed from rotation automatically
- **Rate limiting** — lock-free sliding-window limiter holds a precise requests/second target
- **Live stats** — per-second QPS, p50/p95 latency, and HTTP status distribution in the terminal
- **Interactive menu** — run with no arguments for a step-by-step setup; full CLI flags also available
- **Cache buster** — appends a random query parameter to each request to bypass CDN/page caches
- **Custom requests** — any HTTP method, custom headers, request body, keep-alive toggle

## Requirements

- Python 3.10+
- `aiohttp`
- `aiohttp-socks` (optional, only needed for SOCKS proxies)

```bash
pip install aiohttp aiohttp-socks
```

## Quick Start

### Interactive menu (recommended)

```bash
python loadtest.py
```

Opens a menu where you set the target URL, request rate, total request count, concurrency, timeout, proxy options, and more. Press Enter to start.

### CLI

```bash
# 200 requests at 50 req/s using checked proxies
python loadtest.py --url https://target.example --count 200 --rate 50 --concurrency 100

# Endless run with cache bypass, quiet output
python loadtest.py --url https://target.example --rate 30 --cache-buster -q

# POST request with body and header
python loadtest.py --url https://target.example/api --method POST \
  --body '{"key":"value"}' --header "Content-Type: application/json"

# Skip proxy health check
python loadtest.py --url https://target.example --no-check

# Reuse connections (keep-alive)
python loadtest.py --url https://target.example --keepalive
```

## Proxy Lists

Place proxy files in the `proxies/` directory as plain `ip:port` lines (lines starting with `#` are ignored):

```
proxies/
├── http.txt      # plain entries get http:// prefix automatically
├── https.txt     # CONNECT-capable HTTP proxies
├── socks4.txt
└── socks5.txt
```

- Entries with a scheme (`socks5://1.2.3.4:1080`) are used as-is.
- After a health check, verified proxies are saved as `<type>_alive.txt` and can be reused without re-checking (menu option 13).
- Menu option 14 fetches fresh lists from public sources and merges them into your files — your existing entries are never removed.

## Menu Options

| # | Setting | Default |
|---|---------|---------|
| 1 | Target URL | — (required) |
| 2 | HTTP method | `GET` |
| 3 | Requests per second (0 = unlimited) | `20` |
| 4 | Total requests (0 = endless) | `0` |
| 5 | Parallel workers | `50` |
| 6 | Request timeout (seconds) | `10` |
| 7 | Request body (switches method to POST) | — |
| 8 | Cache buster | off |
| 9 | Keep-alive connections | off |
| 10 | Proxy directory | `proxies` |
| 11 | Proxy type filter (e.g. `http,socks5`) | all |
| 12 | Skip proxy health check | auto |
| 13 | Start from saved live list | auto |
| 14 | Fetch fresh proxies from the internet | off |

## Output Example

```
[i] http.txt: 9397 proxies loaded
[i] http: 9397 -> 812 alive
[i] Target: https://target.example | worker=50 | rate=20.0/s | proxy=812
[    3.0s] qps=   19.8 sent=60 ok=58 fail=2 p50=812ms p95=1904ms | status 200:58
[    4.0s] qps=   20.1 sent=80 ok=78 fail=2 p50=795ms p95=1920ms | status 200:78

[=] duration=10.2s sent=200 ok=195 fail=5 avg_qps=19.6 p50=820ms p95=1990ms
```

## Tips

- **Rate vs. concurrency:** if the achieved QPS is below your target, increase `--concurrency`. A good starting point: `concurrency ≈ rate × average latency (seconds)`.
- **Cache-buster matters:** without it, cached responses make latency look far better than reality.
- **Public proxies are flaky:** expect 10–30% failure rates; keep some headroom in your proxy list, or re-fetch (menu option 14) and re-check regularly.
- **Use responsibly:** only load-test targets you own or are authorized to test.
