import json
import sys

# Ensure stdout uses UTF-8 even on Windows
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

file_path = "c:/Projects/forex_system/services/collector_events/collector_events/globalintel/mock_intel_items_big_process_result.json"

with open(file_path, "r", encoding="utf-8") as f:
    data = json.load(f)

print(f"{'Title':<50} | {'Category':<40} | {'Score':<10}")
print("-" * 105)

for item in data:
    title = item.get("title", "")[:50].replace('\n', ' ')
    extra = item.get("extra", {})
    
    # Try to get impact_category and danger_score from extra or direct keys
    cat = extra.get("impact_category", item.get("impact_category", "N/A"))
    score = extra.get("danger_score", item.get("danger_score", 0.0))
    
    # If still N/A, try nlp_features
    if cat == "N/A":
        nlp = extra.get("nlp_features", {})
        cat = nlp.get("inferred_category", "N/A")
    
    print(f"{title:<50} | {cat:<40} | {score:<10.4f}")
