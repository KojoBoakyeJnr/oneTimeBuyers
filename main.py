#!/usr/bin/env python3
"""
Daily pipeline:
1. Fetch each customer's FIRST QC purchase date (YYYY-MM-DD) up to today.
2. Filter to customers whose first purchase was exactly 7 days ago.
3. Fetch all purchase logs (YYYY-MM-DD) for those customers.
4. Keep only customers who never purchased again.
5. Add a column indicating if they are on app version >= 8.8.14.
6. Save final results to Excel in ./output (or ~/Downloads when run locally).

Run:
    export JQL_API_KEY="your_base64_api_key"
    python3 one_time_buyers.py
"""

import os
import json
import subprocess
from datetime import datetime, timedelta
import pandas as pd
from packaging import version   # handles dotted version comparisons

# ========= CONFIG =========
AUTH_HEADER = os.getenv("JQL_API_KEY")  # must be set as an environment variable
if not AUTH_HEADER:
    raise EnvironmentError("Environment variable JQL_API_KEY is missing.")

today_str = datetime.utcnow().strftime("%Y-%m-%d")
target_date = (datetime.utcnow().date() - timedelta(days=7)).strftime("%Y-%m-%d")

# Save inside ./output for CI/CD (GitHub Actions) or ~/Downloads locally
output_dir = os.getenv("OUTPUT_DIR", os.path.expanduser("~/Downloads"))
os.makedirs(output_dir, exist_ok=True)
OUTPUT_EXCEL = os.path.join(output_dir, f"one_time_buyers_{target_date}.xlsx")

TEMP_JSON = "mixpanel_results.json"
LATEST_VERSION = "8.8.14"
# ==========================


def run_jql(jql_script: str, outfile: str) -> list:
    """
    Execute Mixpanel JQL via curl and return parsed JSON list.
    """
    curl_cmd = f"""
    curl --silent --request POST \\
         --url https://mixpanel.com/api/query/jql \\
         --header 'accept: application/json' \\
         --header 'authorization: {AUTH_HEADER}' \\
         --header 'content-type: application/x-www-form-urlencoded' \\
         --data-urlencode 'script={jql_script}' \\
         --output {outfile}
    """
    subprocess.run(curl_cmd, shell=True, check=True)
    with open(outfile, "r") as f:
        text = f.read().strip()
        return json.loads(text) if text else []


def build_first_purchase_jql() -> str:
    return r"""
function main() {
  function isoDate(ts) { return new Date(ts).toISOString().slice(0, 10); }

  return Events({
    from_date: "2020-01-01",
    to_date: new Date().toISOString().slice(0, 10)
  })
  .filter(function(e) {
    var phone   = e.properties["CustomerPhoneNumber"];
    var station = (e.properties["Station"] || "").toLowerCase();
    var zone    = (e.properties["Zone"] || "").toLowerCase();

    var isGuest       = e.properties["IsGuest"] === true || !phone || String(phone).toLowerCase() === "guest";
    var isTestStation = station.indexOf("test") !== -1;
    var isTestZone    = zone.indexOf("hubtel test zone – kubekrom") !== -1 || zone.indexOf("test") !== -1;

    return e.name === "Purchase" &&
           ["Food","Shop","Health"].indexOf(e.properties["AppSection"]) !== -1 &&
           e.properties["AppName"] === "Hubtel" &&
           !isGuest &&
           !isTestZone &&
           !isTestStation;
  })
  .groupBy(["properties.CustomerPhoneNumber"], mixpanel.reducer.min("time"))
  .map(function(row) {
    return {
      customer_phone_number: row.key[0],
      first_purchase_date: isoDate(row.value)
    };
  });
}
"""


def build_purchase_logs_jql(phone_numbers: list[str]) -> str:
    number_list = ",".join(f'"{n}"' for n in phone_numbers)
    return rf"""
function main() {{
  function isoDate(ts) {{ return new Date(ts).toISOString().slice(0, 10); }}

  var targetCustomers = [{number_list}];

  return Events({{
    from_date: "2020-01-01",
    to_date: new Date().toISOString().slice(0, 10)
  }})
  .filter(function(e) {{
    var station = (e.properties["Station"] || "").toLowerCase();
    var zone    = (e.properties["Zone"] || "").toLowerCase();

    var isTestStation = station.indexOf("test") !== -1;
    var isTestZone    = zone.indexOf("hubtel test zone – kubekrom") !== -1 || zone.indexOf("test") !== -1;

    return e.name === "Purchase" &&
           ["Food","Shop","Health"].indexOf(e.properties["AppSection"]) !== -1 &&
           e.properties["AppName"] === "Hubtel" &&
           e.properties["IsGuest"] === false &&
           !isTestZone &&
           !isTestStation &&
           targetCustomers.indexOf(e.properties["CustomerPhoneNumber"]) !== -1;
  }})
  .map(function(e) {{
    return {{
      customer_phone_number: e.properties["CustomerPhoneNumber"],
      EventDateISO: isoDate(e.time),
      Station: e.properties["Station"] || "unknown",
      Zone: e.properties["Zone"] || "unknown",
      AppVersion: e.properties["AppVersion"] || "unknown",
      Amount: e.properties["Amount"] || 0
    }};
  }});
}}
"""


def main():
    print("=== Stage 1: Fetch first QC purchase dates ===")
    base_results = run_jql(build_first_purchase_jql(), TEMP_JSON)
    if not base_results:
        print("No data from Stage 1. Exiting.")
        return

    first_df = pd.DataFrame(base_results)
    if first_df.empty:
        print("No valid data to process. Exiting.")
        return

    print(f"Target date (first purchase exactly 7 days ago): {target_date}")
    target_customers_df = first_df[first_df["first_purchase_date"] == target_date]
    target_customers = target_customers_df["customer_phone_number"].astype(str).tolist()
    if not target_customers:
        print("No customers match the target date. Exiting.")
        return

    print("=== Stage 3: Fetch all purchase logs ===")
    logs_results = run_jql(build_purchase_logs_jql(target_customers), TEMP_JSON)
    if not logs_results:
        print("No purchase logs found. Exiting.")
        return

    logs_df = pd.DataFrame(logs_results)
    one_time = logs_df.groupby("customer_phone_number").filter(lambda x: len(x) == 1).copy()
    if one_time.empty:
        print("No one-time customers found. Exiting.")
        return

    final_df = one_time.merge(
        target_customers_df[["customer_phone_number", "first_purchase_date"]],
        on="customer_phone_number",
        how="left"
    )

    final_df["is_latest_version"] = final_df["AppVersion"].apply(
        lambda v: "Yes" if version.parse(v) >= version.parse(LATEST_VERSION) else "No"
    )

    final_df = final_df.rename(columns={
        "Station": "station",
        "Zone": "zone",
        "AppVersion": "app_version",
        "Amount": "amount"
    })[[
        "customer_phone_number",
        "station",
        "zone",
        "app_version",
        "is_latest_version",
        "amount",
        "first_purchase_date"
    ]].sort_values("first_purchase_date")

    os.makedirs(os.path.dirname(OUTPUT_EXCEL), exist_ok=True)
    final_df.to_excel(OUTPUT_EXCEL, index=False)
    print(f"Excel file created: {OUTPUT_EXCEL}")


if __name__ == "__main__":
    main()