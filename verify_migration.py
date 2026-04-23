"""Verify migration to PostgreSQL"""
from storage.scd2_database import SCD2DatabaseManager

db = SCD2DatabaseManager()
trials = db.get_current_trials(limit=1000)

print(f"Total trials in PostgreSQL: {len(trials)}")
print("\nSample trials:")
for t in trials[:5]:
    title = t['title'][:60] + '...' if len(t['title']) > 60 else t['title']
    print(f"  - {t['trial_id']}: {title}")

print("\nBy source:")
sources = {}
for t in trials:
    src = t['source']
    sources[src] = sources.get(src, 0) + 1
for src, count in sources.items():
    print(f"  - {src}: {count}")
