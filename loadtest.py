#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Proxy'li async HTTP yük testi (v2).

Değişiklikler v1'e göre:
- SOCKS proxy başına TEK kalıcı session (session-per-request maliyeti yok)
- Global kilit yok: kayan slot tabanlı rate limiter
- İstek başına print yerine saniyelik canlı istatistik (QPS, p50/p95, status dağılımı)
- Ölen proxy'ler otomatik havuzdan çıkarılır; istek başına retry
- --header/--body/--cache-buster/--proxy-type/--keepalive/--quiet
- Ctrl+C'de de özet basılır, rlimit yükseltilir
"""

import argparse
import asyncio
import os
import random
import sys
import time
from collections import Counter, deque
from pathlib import Path

import aiohttp

try:
    from aiohttp_socks import ProxyConnector
    HAS_SOCKS = True
except ImportError:
    ProxyConnector = None
    HAS_SOCKS = False

DEFAULT_UA_URL = (
    "https://raw.githubusercontent.com/Vann-Dev/proxy-list/"
    "refs/heads/main/user_agents.txt"
)

FALLBACK_UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
]

PROXY_FILES = {
    "http.txt": "http://",
    "https.txt": "http://",     # "https proxy" = CONNECT destekleyen HTTP proxy
    "socks4.txt": "socks4://",
    "socks5.txt": "socks5://",
}

MAX_CONSECUTIVE_FAILS = 3

# ---------------------------------------------------------------- proxy kaynakları

GITHUB_RAW = "https://raw.githubusercontent.com"
VANN = f"{GITHUB_RAW}/Vann-Dev/proxy-list/refs/heads/main/proxies"
IPLOCATE = f"{GITHUB_RAW}/iplocate/free-proxy-list/main/protocols"

# tip -> [url, ...]  (tek tip birden çok kaynaktan birleştirilir)
PROXY_SOURCES: dict[str, list[str]] = {
    "http": [f"{VANN}/http.txt", f"{IPLOCATE}/http.txt"],
    "https": [f"{VANN}/https.txt", f"{IPLOCATE}/https.txt"],
    "socks4": [f"{VANN}/socks4.txt", f"{IPLOCATE}/socks4.txt"],
    "socks5": [f"{VANN}/socks5.txt", f"{IPLOCATE}/socks5.txt"],
}

GEONODE_API = "https://proxylist.geonode.com/api/proxy-list"


async def _read_text(session, url: str) -> str:
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
        r.raise_for_status()
        return await r.text()


def parse_ipport_lines(text: str) -> set[str]:
    """Satırlardan ip:port olanları çıkarır (format farklarına dayanıklı)."""
    out = set()
    import re
    for m in re.finditer(r"(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})", text):
        ip, port = m.group(1), m.group(2)
        if all(int(p) <= 255 for p in ip.split(".")) and int(port) <= 65535:
            out.add(f"{ip}:{port}")
    return out


async def fetch_geonode(session) -> dict[str, set[str]]:
    """Geonode API'sinden sayfa sayfa çeker. protokol -> {ip:port}"""
    result: dict[str, set[str]] = {"http": set(), "https": set(),
                                   "socks4": set(), "socks5": set()}
    page = 1
    while True:
        url = (f"{GEONODE_API}?limit=500&page={page}&sort_by=lastChecked"
               f"&sort_type=desc&filterUpTime=90")
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=25)) as r:
                data = (await r.json()).get("data", [])
        except Exception:
            break
        if not data:
            break
        for row in data:
            ip, port = row.get("ip"), str(row.get("port", ""))
            if not ip or not port.isdigit():
                continue
            for proto in row.get("protocols", ["http"]):
                if proto in result:
                    result[proto].add(f"{ip}:{port}")
        if len(data) < 500:
            break
        page += 1
        if page > 10:
            break
    return result


async def auto_fetch_proxies(proxy_dir: str) -> dict[str, int]:
    """4 kaynaktan proxy çeker, mevcut dosyalarla birleştirir, kaydeder."""
    d = Path(proxy_dir)
    d.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    conn = aiohttp.TCPConnector(ssl=False, limit=20)
    async with aiohttp.ClientSession(connector=conn, headers={"User-Agent": FALLBACK_UAS[0]}) as s:
        # github kaynakları paralel
        async def grab(ptype, url):
            try:
                return ptype, parse_ipport_lines(await _read_text(s, url))
            except Exception as e:
                print(f"[!] {url} alınamadı ({type(e).__name__})")
                return ptype, set()

        tasks = [grab(pt, u) for pt, urls in PROXY_SOURCES.items() for u in urls]
        merged: dict[str, set[str]] = {"http": set(), "https": set(),
                                       "socks4": set(), "socks5": set()}
        for ptype, ips in await asyncio.gather(*tasks):
            merged[ptype] |= ips

        # geonode
        try:
            geo = await fetch_geonode(s)
            for proto, ips in geo.items():
                merged[proto] |= ips
        except Exception as e:
            print(f"[!] Geonode alınamadı ({type(e).__name__})")

    for ptype, ips in merged.items():
        if not ips:
            continue
        f = d / f"{ptype}.txt"
        existing = load_proxy_file(f, f"{ptype}://") if f.is_file() else []
        old_addrs = {p.split("://", 1)[-1] for p in existing}
        all_addrs = old_addrs | ips
        f.write_text("\n".join(sorted(all_addrs)), encoding="utf-8")
        counts[ptype] = len(all_addrs)
        print(f"[i] {f.name}: {len(all_addrs)} proxy "
              f"(yeni: {len(all_addrs - old_addrs)})")
    return counts


# ---------------------------------------------------------------- yardımcılar

def quiet_proactor(loop):
    """Windows Proactor'ın ConnectionResetError gürültüsünü susturur."""
    def handler(loop, context):
        exc = context.get("exception")
        if isinstance(exc, (ConnectionResetError, ConnectionAbortedError,
                            asyncio.TimeoutError, OSError)):
            return
        loop.default_exception_handler(context)
    loop.set_exception_handler(handler)


def bump_rlimit():
    try:
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        want = hard if hard != resource.RLIM_INFINITY else 65535
        resource.setrlimit(resource.RLIMIT_NOFILE, (want, hard))
    except Exception:
        pass


def load_proxy_file(path: Path, scheme: str) -> list[str]:
    out = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        out.append(line if "://" in line else f"{scheme}{line}")
    return out


def load_all_proxies(proxy_dir: str) -> dict[str, list[str]]:
    by_type: dict[str, list[str]] = {}
    d = Path(proxy_dir)
    if not d.is_dir():
        return by_type
    for fname, scheme in PROXY_FILES.items():
        f = d / fname
        if f.is_file():
            plist = load_proxy_file(f, scheme)
            if plist:
                by_type[scheme.replace("://", "")] = plist
                print(f"[i] {fname}: {len(plist)} proxy yüklendi")
    return by_type


def load_alive_proxies(proxy_dir: str) -> dict[str, list[str]]:
    """Sadece *_alive.txt dosyalarını yükler (önceki testte canlı çıkanlar)."""
    by_type: dict[str, list[str]] = {}
    d = Path(proxy_dir)
    if not d.is_dir():
        return by_type
    for f in sorted(d.glob("*_alive.txt")):
        ptype = f.name.removesuffix("_alive.txt")
        plist = load_proxy_file(f, f"{ptype}://")
        if plist:
            by_type[ptype] = plist
            print(f"[i] {f.name}: {len(plist)} canlı proxy yüklendi")
    return by_type


async def fetch_user_agents(session: aiohttp.ClientSession, url: str) -> list[str]:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            uas = [l.strip() for l in (await r.text()).splitlines() if l.strip()]
            if uas:
                print(f"[i] {len(uas)} user agent yüklendi")
                return uas
    except Exception as e:
        print(f"[!] UA listesi alınamadı ({e}), fallback kullanılıyor")
    return FALLBACK_UAS


# ---------------------------------------------------------------- proxy havuzu

class Slot:
    """Tek bir proxy. SOCKS için kalıcı session tutar, fail sayar."""
    __slots__ = ("url", "ptype", "session", "fails", "dead", "lock")

    def __init__(self, url: str, ptype: str):
        self.url = url
        self.ptype = ptype
        self.session = None
        self.fails = 0
        self.dead = False
        self.lock = asyncio.Lock()

    async def get_session(self, keepalive: bool) -> aiohttp.ClientSession:
        if self.session is not None and not self.session.closed:
            return self.session
        async with self.lock:
            if self.session is None or self.session.closed:
                conn = ProxyConnector.from_url(
                    self.url, force_close=not keepalive, limit=0,
                    rdns=True,
                )
                self.session = aiohttp.ClientSession(connector=conn)
        return self.session

    async def close(self):
        if self.session is not None and not self.session.closed:
            try:
                await self.session.close()
            except Exception:
                pass

    def hit(self):
        self.fails = 0

    def miss(self) -> bool:
        """True dönerse proxy ölmüş sayılır."""
        self.fails += 1
        return self.fails >= MAX_CONSECUTIVE_FAILS


class Pool:
    def __init__(self, alive: dict[str, list[str]]):
        self.slots = [Slot(p, t) for t, plist in alive.items() for p in plist]
        random.shuffle(self.slots)
        self._idx = 0

    def pick(self) -> Slot | None:
        n = len(self.slots)
        if n == 0:
            return None
        # round-robin + hafif rastgelelik: dağılım düzgün, örnekleme yine karışık
        if n > 4096:
            return self.slots[random.randrange(n)]
        s = self.slots[self._idx % n]
        self._idx += 1
        return s

    def drop(self, slot: Slot):
        if slot.dead:
            return
        slot.dead = True
        try:
            self.slots.remove(slot)
        except ValueError:
            pass

    async def close(self):
        await asyncio.gather(*(s.close() for s in self.slots),
                             return_exceptions=True)


# ---------------------------------------------------------------- rate limiter

class RateLimiter:
    """Kilit yok: her çağrı kendine bir zaman dilimi ayırır."""
    __slots__ = ("rate", "next")

    def __init__(self, rate: float):
        self.rate = rate
        self.next = 0.0

    async def acquire(self):
        if self.rate <= 0:
            return
        loop = asyncio.get_running_loop()
        now = loop.time()
        if self.next < now:
            self.next = now
        slot = self.next
        self.next += 1.0 / self.rate
        delay = slot - now
        if delay > 0:
            await asyncio.sleep(delay)


# ---------------------------------------------------------------- istatistik

class Stats:
    def __init__(self):
        self.sent = 0
        self.ok = 0
        self.fail = 0
        self.status: Counter[int] = Counter()
        self.lat = deque(maxlen=10000)
        self.window_sent = 0
        self.window_ok = 0
        self.start = time.perf_counter()
        self._last = 0.0

    def pct(self, q: float) -> float:
        if not self.lat:
            return 0.0
        data = sorted(self.lat)
        return data[min(len(data) - 1, int(len(data) * q))]

    def tick(self) -> str:
        done = time.perf_counter() - self.start
        qps = self.window_sent / max(done - self._last, 1e-9)
        self._last = done
        line = (f"[{done:7.1f}s] qps={qps:7.1f} sent={self.sent} ok={self.ok} "
                f"fail={self.fail} p50={self.pct(.5) * 1000:.0f}ms "
                f"p95={self.pct(.95) * 1000:.0f}ms")
        self.window_sent = 0
        if self.status:
            top = ", ".join(f"{k}:{v}" for k, v in self.status.most_common(4))
            line += f" | status {top}"
        return line

    def report(self) -> str:
        dur = time.perf_counter() - self.start
        return (f"\n[=] süre={dur:.1f}s gönderilen={self.sent} ok={self.ok} "
                f"fail={self.fail} ort_qps={self.sent / max(dur, 1e-9):.1f} "
                f"p50={self.pct(.5) * 1000:.0f}ms p95={self.pct(.95) * 1000:.0f}ms")


# ---------------------------------------------------------------- worker

def build_headers(cfg, uas) -> dict[str, str]:
    h = {
        "User-Agent": random.choice(uas),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive" if cfg.keepalive else "close",
    }
    for raw in cfg.header or []:
        if ":" in raw:
            k, v = raw.split(":", 1)
            h[k.strip()] = v.strip()
    return h


def build_url(cfg) -> str:
    if not cfg.cache_buster:
        return cfg.url
    sep = "&" if "?" in cfg.url else "?"
    return f"{cfg.url}{sep}_cb={random.getrandbits(48):012x}"


async def fire(cfg, ctx, slot: Slot | None):
    st: Stats = ctx["stats"]
    url = build_url(cfg)
    headers = build_headers(cfg, ctx["uas"])
    t0 = time.perf_counter()
    try:
        if slot and slot.ptype.startswith("socks"):
            sess = await slot.get_session(cfg.keepalive)
            async with sess.request(
                cfg.method, url, headers=headers, data=cfg.body,
                timeout=aiohttp.ClientTimeout(total=cfg.timeout),
                allow_redirects=False,
            ) as resp:
                await resp.read()
                code = resp.status
        else:
            async with ctx["session"].request(
                cfg.method, url, headers=headers, data=cfg.body,
                proxy=slot.url if slot else None,
                timeout=aiohttp.ClientTimeout(total=cfg.timeout),
                allow_redirects=False,
            ) as resp:
                await resp.read()
                code = resp.status
        st.lat.append(time.perf_counter() - t0)
        st.status[code] += 1
        if code < 500:
            st.ok += 1
            st.window_ok += 1
            if slot:
                slot.hit()
        else:
            st.fail += 1
            if slot and slot.miss():
                ctx["pool"].drop(slot)
        if not cfg.quiet:
            print(f"[{code}] {slot.url if slot else 'direct'} "
                  f"{(time.perf_counter() - t0) * 1000:.0f}ms")
    except asyncio.CancelledError:
        raise
    except Exception:
        st.fail += 1
        if slot and slot.miss():
            ctx["pool"].drop(slot)
        if not cfg.quiet:
            print(f"[ERR] {slot.url if slot else 'direct'}")


async def worker(cfg, ctx):
    while ctx["running"]:
        if cfg.count and ctx["stats"].sent >= cfg.count:
            return
        await ctx["limiter"].acquire()
        if not ctx["running"]:
            return
        if cfg.count and ctx["stats"].sent >= cfg.count:
            return
        ctx["stats"].sent += 1
        ctx["stats"].window_sent += 1
        await fire(cfg, ctx, ctx["pool"].pick())


async def reporter(cfg, ctx):
    while ctx["running"]:
        await asyncio.sleep(1.0)
        if ctx["running"]:
            print(ctx["stats"].tick())


# ---------------------------------------------------------------- proxy test

async def check_proxy(proxy: str, ptype: str, sem, target, out, timeout, sess):
    async with sem:
        try:
            if ptype.startswith("socks"):
                conn = ProxyConnector.from_url(proxy, force_close=True)
                async with aiohttp.ClientSession(connector=conn) as s:
                    async with s.get(target, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                        if r.status < 500:
                            out.append(proxy)
            else:
                async with sess.get(proxy=proxy, url=target,
                                    timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                    if r.status < 500:
                        out.append(proxy)
        except Exception:
            pass


async def filter_alive(by_type, check_url, timeout, conc) -> dict[str, list[str]]:
    sem = asyncio.Semaphore(conc)
    alive: dict[str, list[str]] = {}
    conn = aiohttp.TCPConnector(ssl=False, limit=0, force_close=True)
    async with aiohttp.ClientSession(connector=conn) as sess:
        for ptype, plist in by_type.items():
            if ptype.startswith("socks") and not HAS_SOCKS:
                print("[!] aiohttp-socks yok, SOCKS atlanıyor")
                continue
            out: list[str] = []
            print(f"[i] {ptype}: {len(plist)} proxy test ediliyor "
                  f"(paralel={conc}, timeout={timeout}s)...")
            await asyncio.gather(*[
                check_proxy(p, ptype, sem, check_url, out, timeout, sess)
                for p in plist
            ])
            alive[ptype] = out
            print(f"[i] {ptype}: {len(plist)} -> {len(out)} canlı")
    return alive


# ---------------------------------------------------------------- main

async def run(cfg) -> int:  # noqa: C901
    bump_rlimit()
    quiet_proactor(asyncio.get_running_loop())

    by_type: dict[str, list[str]]
    if getattr(cfg, "_use_alive", False):
        # menüde "canlı listeden başla" seçildi -> sadece *_alive.txt oku
        by_type = {}
        d = Path(cfg.proxy_dir)
        for f in sorted(d.glob("*_alive.txt")):
            ptype = f.name.removesuffix("_alive.txt")
            plist = load_proxy_file(f, f"{ptype}://")
            if plist:
                by_type[ptype] = plist
                print(f"[i] {f.name}: {len(plist)} canlı proxy yüklendi")
    else:
        by_type = load_all_proxies(cfg.proxy_dir)
    if cfg.proxy_type:
        by_type = {k: v for k, v in by_type.items()
                   if k in cfg.proxy_type.split(",")}

    live_pool: dict[str, list[str]] = {}
    if by_type and not cfg.no_check:
        check_url = cfg.check_url or cfg.url
        print(f"[i] Proxy testi: {check_url}")
        alive = await filter_alive(by_type, check_url, cfg.check_timeout,
                                   cfg.check_concurrency)
        total = sum(len(v) for v in alive.values())
        if total == 0:
            print("[!] Canlı proxy yok, direkt istek atılacak")
        else:
            d = Path(cfg.proxy_dir)
            for ptype, plist in alive.items():
                (d / f"{ptype}_alive.txt").write_text("\n".join(plist), encoding="utf-8")
                print(f"[i] Canlı liste kaydedildi: {ptype}_alive.txt ({len(plist)})")
            live_pool = alive
    else:
        live_pool = by_type

    connector = aiohttp.TCPConnector(
        ssl=False, limit=0, force_close=not cfg.keepalive,
        enable_cleanup_closed=True, ttl_dns_cache=300,
    )
    uas_conn = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=uas_conn) as ua_sess:
        uas = await fetch_user_agents(ua_sess, cfg.ua_url)

    ctx = {
        "stats": Stats(),
        "uas": uas,
        "pool": Pool(live_pool),
        "limiter": RateLimiter(cfg.rate),
        "running": True,
        "session": None,
    }

    async with aiohttp.ClientSession(connector=connector) as sess:
        ctx["session"] = sess
        total = len(ctx["pool"].slots)
        print(f"[i] Hedef: {cfg.url} | worker={cfg.concurrency} | rate={cfg.rate}/sn "
              f"| proxy={total} | keepalive={cfg.keepalive}")

        tasks = [asyncio.create_task(worker(cfg, ctx))
                 for _ in range(cfg.concurrency)]
        rep = asyncio.create_task(reporter(cfg, ctx))
        try:
            await asyncio.gather(*tasks)
        finally:
            ctx["running"] = False
            rep.cancel()
            print(ctx["stats"].report())
            await ctx["pool"].close()

    return 0


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description="Proxy'li async HTTP yük testi")
    ap.add_argument("--url", help="Hedef URL")
    ap.add_argument("--method", default="GET")
    ap.add_argument("--body", default=None, help="POST gövdesi")
    ap.add_argument("--header", action="append", help="Ek header (tekrarlanabilir)")
    ap.add_argument("--cache-buster", action="store_true",
                    help="Her isteğe rastgele _cb parametresi ekle")
    ap.add_argument("--proxy-dir", default="proxies")
    ap.add_argument("--proxy-type", default="", help="http,socks5 gibi filtre")
    ap.add_argument("--ua-url", default=DEFAULT_UA_URL)
    ap.add_argument("--concurrency", type=int, default=50)
    ap.add_argument("--rate", type=float, default=20.0, help="İstek/sn (0=sınırsız)")
    ap.add_argument("--count", type=int, default=0, help="Toplam istek (0=sonsuz)")
    ap.add_argument("--timeout", type=float, default=10.0)
    ap.add_argument("--check-timeout", type=float, default=5.0)
    ap.add_argument("--check-concurrency", type=int, default=1000)
    ap.add_argument("--check-url", default="")
    ap.add_argument("--no-check", action="store_true")
    ap.add_argument("--keepalive", action="store_true",
                    help="Bağlantıları yeniden kullan (varsayılan: her istek yeni bağlantı)")
    ap.add_argument("--quiet", "-q", action="store_true", help="İstek başına log yok")
    return ap.parse_args(argv)


# ---------------------------------------------------------------- menü

def ask(prompt: str, default: str = "") -> str:
    suffix = f" [{default}]" if default != "" else ""
    val = input(f"{prompt}{suffix}: ").strip()
    return val or default


def ask_int(prompt: str, default: int) -> int:
    raw = ask(prompt, str(default))
    try:
        return int(raw)
    except ValueError:
        return default


def ask_float(prompt: str, default: float) -> float:
    raw = ask(prompt, str(default))
    try:
        return float(raw)
    except ValueError:
        return default


def ask_bool(prompt: str, default: bool = False) -> bool:
    d = "e/h"
    raw = input(f"{prompt} [{d}, varsayılan={'e' if default else 'h'}]: ").strip().lower()
    if raw in ("e", "evet", "y", "yes", "1"):
        return True
    if raw in ("h", "hayır", "n", "no", "0"):
        return False
    return default


MENU_STEPS = [
    ("1", "Hedef URL (--url)", "url", "str", True),
    ("2", "Metod (GET/POST/...)", "method", "str", False),
    ("3", "Saniyedeki istek --rate", "rate", "float", False),
    ("4", "Toplam istek --count", "count", "int", False),
    ("5", "Paralel worker --concurrency", "concurrency", "int", False),
    ("6", "Timeout (sn) --timeout", "timeout", "float", False),
    ("7", "Body (POST verisi)", "body", "str", False),
    ("8", "Cache buster", "cache_buster", "bool", False),
    ("9", "Keepalive (bağlantı yeniden kullan)", "keepalive", "bool", False),
    ("10", "Proxy klasörü --proxy-dir", "proxy_dir", "str", False),
    ("11", "Proxy tipi filtresi (http,socks5)", "proxy_type", "str", False),
    ("12", "Proxy testini atla (--no-check)", "no_check", "bool", False),
    ("13", "Canlı listeden başla (check etmeden)", "alive_only", "flag", False),
    ("14", "Proxy'leri internetten güncelle", "auto_fetch", "flag", False),
]


def print_menu(values: dict, alive_exists: bool):
    print()
    print("=" * 60)
    print("  YÜK TESTİ AYARLARI  (değiştirmek için numara gir,")
    print("  boş bırak = devam)")
    print("=" * 60)
    for key, label, attr, *_ in MENU_STEPS:
        if key == "13" and not alive_exists:
            continue
        v = values.get(attr, "")
        print(f"  {key:>2}. {label:<38} = {v}")
    print("=" * 60)


def menu() -> argparse.Namespace:
    cfg = parse_args([])
    alive_dir = Path(cfg.proxy_dir)
    alive_exists = any(alive_dir.glob("*_alive.txt")) if alive_dir.is_dir() else False
    # menü varsayılanı: canlı liste varsa onu kullan, yoksa taze test et
    if alive_exists:
        cfg._use_alive = True  # type: ignore[attr-defined]

    while True:
        values = {
            "url": cfg.url or "(girilmedi)",
            "method": cfg.method,
            "rate": cfg.rate,
            "count": cfg.count if cfg.count else 0,
            "concurrency": cfg.concurrency,
            "timeout": cfg.timeout,
            "body": cfg.body or "-",
            "cache_buster": "evet" if cfg.cache_buster else "hayır",
            "keepalive": "evet" if cfg.keepalive else "hayır",
            "proxy_dir": cfg.proxy_dir,
            "proxy_type": cfg.proxy_type or "-",
            "no_check": "evet" if cfg.no_check else "hayır",
            "alive_only": "evet" if getattr(cfg, "_use_alive", False) else "hayır",
            "auto_fetch": "evet" if getattr(cfg, "auto_fetch", False) else "hayır",
        }
        print_menu(values, alive_exists)

        raw = input("Seçim (1-14, boş=başlat, q=çık): ").strip()
        if raw == "":
            if not cfg.url:
                print("[!] Önce hedef URL girin (1)")
                continue
            break
        if raw.lower() in ("q", "quit", "çık"):
            sys.exit(0)
        if raw == "1":
            cfg.url = ask("Hedef URL")
        elif raw == "2":
            cfg.method = ask("Metod", cfg.method).upper()
        elif raw == "3":
            cfg.rate = ask_float("İstek/sn (0=sınırsız)", cfg.rate)
        elif raw == "4":
            cfg.count = ask_int("Toplam istek (0=sonsuz)", cfg.count)
        elif raw == "5":
            cfg.concurrency = ask_int("Paralel worker", cfg.concurrency)
        elif raw == "6":
            cfg.timeout = ask_float("Timeout sn", cfg.timeout)
        elif raw == "7":
            b = ask("Body (boş=POST verisi yok)", cfg.body or "")
            cfg.body = b or None
            if cfg.body and cfg.method == "GET":
                cfg.method = "POST"
        elif raw == "8":
            cfg.cache_buster = not cfg.cache_buster
        elif raw == "9":
            cfg.keepalive = not cfg.keepalive
        elif raw == "10":
            cfg.proxy_dir = ask("Proxy klasörü", cfg.proxy_dir)
            d = Path(cfg.proxy_dir)
            alive_exists = any(d.glob("*_alive.txt")) if d.is_dir() else False
        elif raw == "11":
            cfg.proxy_type = ask("Proxy tipi (http,socks4,socks5 / boş=hepsi)",
                                 cfg.proxy_type)
        elif raw == "12":
            cfg.no_check = not cfg.no_check
        elif raw == "13":
            if not alive_exists:
                print("[!] Canlı liste yok, önce proxy testi yapılmalı")
                continue
            cfg._use_alive = not getattr(cfg, "_use_alive", False)
            if cfg._use_alive:
                cfg.no_check = True
        elif raw == "14":
            cfg.auto_fetch = not getattr(cfg, "auto_fetch", False)
            if cfg.auto_fetch:
                cfg._use_alive = False
        else:
            print("[!] Geçersiz seçim")

    return cfg


def main():
    # argüman yoksa interaktif menü, argüman varsa eski CLI davranışı
    interactive = len(sys.argv) == 1
    cfg = menu() if interactive else parse_args()

    if interactive and getattr(cfg, "auto_fetch", False):
        print("[i] Proxy'ler internetten çekiliyor...")
        counts = asyncio.run(auto_fetch_proxies(cfg.proxy_dir))
        print(f"[i] Toplam {sum(counts.values())} proxy indirildi")

    try:
        sys.exit(asyncio.run(run(cfg)))
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
