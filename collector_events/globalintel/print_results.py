import json
import sys

# Ensure stdout uses UTF-8 even on Windows
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

file_path = "c:/Projects/forex_system/services/collector_events/collector_events/globalintel/mock_intel_items_big_sprint_02.json"

try:
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
except FileNotFoundError:
    print(f"Error: {file_path} not found.")
    sys.exit(1)

print(f"{'Title':<50} | {'Category':<40} | {'Score':<8} | {'Num Features'}")
print("-" * 130)

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
    
    num_features = extra.get("numeric_features", {})
    feat_str = ""
    if num_features:
        parts = []
        if num_features.get("large_money_amount"):
            parts.append(f"${num_features['large_money_amount']:,}")
        if num_features.get("casualty_count"):
            parts.append(f"Casualties: {num_features['casualty_count']}")
        if num_features.get("percent_change"):
            parts.append(f"{num_features['percent_change']}%")
        feat_str = ", ".join(parts)
    
    print(f"{title:<50} | {cat:<40} | {score:<8.4f} | {feat_str}")
