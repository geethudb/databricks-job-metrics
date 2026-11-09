"""Cost metrics (I1-I13), platform-neutral and migration-aware.

`leg` identifies which side of the migration this run is. Source-only fees
(e.g. a Dataproc per-vCPU service fee, an external metastore) are emitted as
`not_applicable` on the target leg, and target-only fees (DBU/credit cost) are
emitted as `not_applicable` on a source leg. The included components in the
per-run total (I11) are chosen by leg so the two totals are comparable.
"""
from canonical import metric_envelope as M

EXTERNAL, PLATFORM, DERIVED = "External evidence", "Platform-derived", "Derived"


def _num(value):
    return float(value or 0)


def _is_attributable(row, run_id=None, job_id=None, cluster_id=None):
    supplied = {k: str(v) for k, v in
                {"run_id": run_id, "job_id": job_id, "cluster_id": cluster_id}.items() if v is not None}
    if not supplied:
        return bool(row.get("attributable"))
    meta = {**(row.get("usage_metadata") or {}), **(row.get("custom_tags") or {}), **row}
    aliases = {"run_id": ("run_id", "job_run_id"), "job_id": ("job_id",), "cluster_id": ("cluster_id",)}
    return any(any(str(meta.get(a)) == v for a in aliases[k]) for k, v in supplied.items())


def _cloud_totals(rows):
    cats = {"vm": 0.0, "disk": 0.0, "source_service_fee": 0.0, "metastore": 0.0, "storage": 0.0, "network": 0.0}
    counts = {k: 0 for k in cats}
    for row in rows:
        c = str(row.get("category", "")).lower()
        if c in cats:
            cats[c] += _num(row.get("cost"))
            counts[c] += 1
    return cats, counts


def cost_metrics(platform_usage, cloud_billing=None, rates=None, list_prices=None,
                 leg="target", run_id=None, job_id=None, cluster_id=None,
                 input_bytes=None, runs_per_day=None, operating_days_per_month=None):
    cloud_billing = cloud_billing or []
    rates = rates or {}
    list_prices = list_prices or {}
    source_leg = str(leg).lower() in ("source", "dataproc", "emr")

    attributable = [r for r in platform_usage if _is_attributable(r, run_id, job_id, cluster_id)]
    outside = [r for r in platform_usage if r not in attributable]
    units_by_sku = {}
    for r in attributable:
        sku = str(r.get("sku_name") or "UNKNOWN")
        units_by_sku[sku] = units_by_sku.get(sku, 0.0) + _num(r.get("usage_quantity"))
    outside_units = sum(_num(r.get("usage_quantity")) for r in outside)

    cloud, counts = _cloud_totals(cloud_billing)
    sku_rates = {s: rates.get(s) for s in units_by_sku}
    missing = sorted(s for s, v in sku_rates.items() if v is None)
    unit_cost_by_sku = {s: q * _num(sku_rates[s]) for s, q in units_by_sku.items() if sku_rates[s] is not None}
    unit_cost = sum(unit_cost_by_sku.values()) if unit_cost_by_sku and not missing else None
    storage_network = cloud["storage"] + cloud["network"]

    total = None
    if (attributable and unit_cost is not None) and cloud_billing:
        total = cloud["vm"] + cloud["disk"] + unit_cost + storage_network
        if source_leg:
            total += cloud["source_service_fee"] + cloud["metastore"]
    input_tb = _num(input_bytes) / 1e12 if input_bytes else None
    per_tb = total / input_tb if total is not None and input_tb else None
    monthly = (total * _num(runs_per_day) * _num(operating_days_per_month)
               if total is not None and runs_per_day is not None and operating_days_per_month is not None else None)

    def cloud_metric(mid, name, cat, applicable=True):
        if not applicable:
            return M(mid, name, None, category=EXTERNAL, availability="not_applicable",
                     source="cloud billing export", limitation=f"Not applicable to the {leg} leg.")
        present = counts[cat] > 0
        return M(mid, name, cloud[cat] if present else None, category=EXTERNAL,
                 availability="available" if present else "unavailable", source="normalized cloud billing export",
                 limitation=None if present else f"No {cat} billing rows supplied.")

    return {
        "leg": leg,
        "metrics": {
            "I1": cloud_metric("I1", "VM cost", "vm"),
            "I2": cloud_metric("I2", "Disk cost", "disk"),
            "I3": cloud_metric("I3", "Source service fee", "source_service_fee", source_leg),
            "I4": cloud_metric("I4", "Metastore share", "metastore", source_leg),
            "I6": M("I6", "Platform units (DBUs/credits)",
                    {"total": sum(units_by_sku.values()), "by_sku": units_by_sku} if units_by_sku else None,
                    category=PLATFORM, availability="available" if units_by_sku else "unavailable",
                    source="platform billing usage"),
            "I8": M("I8", "Platform unit cost", {"total": unit_cost, "by_sku": unit_cost_by_sku} if unit_cost is not None else None,
                    category=DERIVED, availability="available" if unit_cost is not None else "unavailable",
                    source="I6 units × contract rate", inputs=["I6"],
                    limitation=None if unit_cost is not None else f"Missing contract rates for: {', '.join(missing) or 'all SKUs'}."),
            "I9": M("I9", "Units outside the job", outside_units if platform_usage else None, category=PLATFORM,
                    availability="available" if platform_usage else "unavailable", source="usage rows in the interval, not attributable"),
            "I10": M("I10", "Storage and network", {"total": storage_network, "storage": cloud["storage"], "network": cloud["network"]}
                     if (counts["storage"] or counts["network"]) else None, category=EXTERNAL,
                     availability="available" if (counts["storage"] or counts["network"]) else "unavailable", source="cloud billing export"),
            "I11": M("I11", "Total cost per run", total, category=DERIVED,
                     availability="available" if total is not None else "unavailable",
                     source="sum of included leg components", inputs=["I1", "I2", "I3", "I4", "I8", "I10"],
                     limitation=None if total is not None else "Requires cloud billing rows and complete contract rates."),
            "I12": M("I12", "Cost per TB", per_tb, category=DERIVED,
                     availability="available" if per_tb is not None else "unavailable",
                     source="I11 / D2 input TB", inputs=["I11", "D2"]),
            "I13": M("I13", "Monthly cost", monthly, category=DERIVED,
                     availability="available" if monthly is not None else "unavailable",
                     source="I11 × runs/day × operating days/month", inputs=["I11"]),
        },
    }
