import json

with open("c:/Projects/forex_system/services/collector_events/collector_events/globalintel/mock_intel_items_big_process_result.json", "r", encoding="utf-8") as f:
    data = json.load(f)

for item in data:
    title = item.get("title", "")[:50].replace('\n', ' ')
    res = item.get("process_result", {})
    cat = res.get("impact_category", "N/A")
    score = res.get("danger_score", 0.0)
    print(f"Title: {title:<50} | Category: {cat:<40} | Score: {score}")
