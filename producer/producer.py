#!/usr/bin/env python3
"""
Real-world-style interaction generator + Kafka producer
-------------------------------------------------------
• 10 000 realistic users  :  firstname.lastname####
• 2 000 products          :  8 retail categories, Zipf popularity
• Session funnel          :  view → click → add_to_cart → purchase
• Extra fields            :  session_id, category, price_usd
• Idempotent event_id     :  deterministic counter
"""

from __future__ import annotations
import os, json, time, argparse, random, logging, math, uuid
from datetime import datetime, timezone, timedelta
from itertools import count, islice
from kafka import KafkaProducer

# ────────────── Logging ──────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("logs/generator.log"),
        logging.StreamHandler()
    ],
)

# ──────────── Constants ──────────────
CATEGORIES = [
    "electronics", "fashion", "home", "beauty",
    "sports", "toys", "automotive", "grocery"
]
N_USERS = 10_000
N_ITEMS = 2_000

FIRST_NAMES_RAW = """
Olivia Liam Emma Noah Charlotte Oliver Amelia Elijah Ava Mateo Sophia Lucas
Isabella Levi Mia Leo Luna Ezra Harper Luca Camila Asher Evelyn Milo Gianna
Aria Hudson Scarlett Elias Chloe Benjamin
"""

LAST_NAMES_RAW = """
Smith Johnson Williams Brown Jones Garcia Miller Davis Rodriguez Martinez
Hernandez Lopez Gonzalez Wilson Anderson Thomas Taylor Moore Jackson Martin
Lee Perez Thompson White Harris Sanchez Clark Ramirez Lewis Robinson
"""

FIRST_NAMES = FIRST_NAMES_RAW.split()
LAST_NAMES  = LAST_NAMES_RAW.split()

FUNNEL_P = {"view": 0.30, "click": 0.15, "add_to_cart": 0.40}  # → 1.8 % purchase
INTERACTION_SEQ = ("view", "click", "add_to_cart", "purchase")

# Zipf weights for item popularity
def zipf_weights(n: int, alpha: float = 1.2) -> list[float]:
    w = [1 / (i + 1) ** alpha for i in range(n)]
    s = sum(w)
    return [x / s for x in w]

ITEM_POP = zipf_weights(N_ITEMS)

# ────────── Synthetic corpora ──────────
def build_users() -> list[str]:
    rng = random.Random(42)
    users = []
    while len(users) < N_USERS:
        fn, ln = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        users.append(f"{fn.lower()}.{ln.lower()}{rng.randint(0, 9999):04d}")
    return users

def build_catalog() -> list[dict]:
    rng = random.Random(99)
    catalog = []
    for i in range(N_ITEMS):
        cat = CATEGORIES[i % len(CATEGORIES)]
        sku = f"{cat[:3].upper()}-{i:05d}"
        price = round(math.exp(rng.gauss(3.7, 0.5)), 2)  # log-normal ≈ $40 mean
        catalog.append({"item_id": sku, "category": cat, "price_usd": price})
    return catalog

USERS   = build_users()
CATALOG = build_catalog()

# ────────── Generators ──────────
def choose_item(rng: random.Random):
    idx = rng.choices(range(N_ITEMS), ITEM_POP, k=1)[0]
    return CATALOG[idx]

def session_events(seed: int = 0):
    """Infinite generator of funnelled events per session."""
    rng = random.Random(seed)
    while True:
        user = rng.choice(USERS)
        sess = uuid.uuid4().hex[:16]
        item = choose_item(rng)
        ts   = datetime.utcnow().replace(tzinfo=timezone.utc)

        # view
        yield user, sess, item, "view", ts
        if rng.random() >= FUNNEL_P["view"]:
            continue

        # click
        ts += timedelta(seconds=rng.uniform(1, 30))
        yield user, sess, item, "click", ts
        if rng.random() >= FUNNEL_P["click"]:
            continue

        # add_to_cart
        ts += timedelta(seconds=rng.uniform(1, 60))
        yield user, sess, item, "add_to_cart", ts
        if rng.random() >= FUNNEL_P["add_to_cart"]:
            continue

        # purchase
        ts += timedelta(seconds=rng.uniform(5, 120))
        yield user, sess, item, "purchase", ts

def event_stream(start_index: int = 0):
    sess_gen = session_events()
    for seq in count(start_index):
        user, sess, item, itype, ts = next(sess_gen)
        yield {
            "event_id": seq,
            "session_id": sess,
            "user_id": user,
            "item_id": item["item_id"],
            "category": item["category"],
            "price_usd": item["price_usd"],
            "interaction_type": itype,
            "timestamp": ts.isoformat(),
        }

# ───────────── Main ─────────────
def main(args):
    producer = KafkaProducer(
        bootstrap_servers=args.broker,
        value_serializer=lambda v: json.dumps(v).encode(),
        linger_ms=args.linger_ms,
        batch_size=args.batch_size,
        acks="all",
    )
    log = logging.getLogger("generator")
    log.info("Kafka producer connected → %s / topic=%s", args.broker, args.topic)
    stream = event_stream()
    try:
        while True:
            batch = list(islice(stream, args.rate))
            for rec in batch:
                producer.send(args.topic, rec)
            producer.flush()
            log.info("sent %d events", len(batch))
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("shutdown requested")
    finally:
        producer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", default=os.getenv("KAFKA_BROKER", "localhost:9092"))
    parser.add_argument("--topic",  default=os.getenv("KAFKA_TOPIC",  "interactions"))
    parser.add_argument("--rate", type=int, default=100, help="events/second")
    parser.add_argument("--linger-ms", type=int, default=300)
    parser.add_argument("--batch-size", type=int, default=16 * 1024)
    main(parser.parse_args())
