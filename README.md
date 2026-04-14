# LHI Related Table Sync Utility

A reusable Python tool for synchronizing attribute values between a hosted feature layer and a related table in ArcGIS Online.

---

## 🧠 Problem

In many GIS workflows, critical data is captured in **related tables** (e.g., inspections, planting records, maintenance logs) but not reflected in the **main feature layer**.

This creates gaps such as:

* missing attributes
* inconsistent reporting
* incomplete dashboards

---

## ⚙️ Solution

This utility:

* Reads a source table
* Matches records using a shared key
* Applies a rule (latest / earliest / first)
* Updates missing values in the main layer
* Runs safely in batches
* Generates a full audit trail (CSV)

---

## 🚀 Features

* Config-driven (no hardcoding)
* Dry-run mode for safe testing
* Batch updates for AGOL
* Audit logging (before/after)
* Supports large datasets
* Reusable across domains

---

## 🧪 Example Use Case

Tree inventory correction:

* Source: Planting table (`PLANTED_DATE`)
* Target: Inventory layer (`PLANTED_DATE_TI`)
* Key: `TREE_ID`
* Rule: latest date

**Result:**
742 missing records identified and updated with 0 failures

---

## ▶️ How to Run

```bash
python lhi_related_table_sync.py config_example.json
```

---

## ⚙️ Config Example

See `config_example.json`

---

## 🧭 Workflow

1. Run dry-run
2. Review audit CSV
3. Validate sample records
4. Run live update
5. Confirm results

---

## 💡 Why This Matters

This tool supports **data governance and quality assurance** in enterprise GIS environments by:

* preventing silent data gaps
* enabling controlled updates
* providing full auditability

---

## 🏷️ Tags

ArcGIS Online · Python · GIS Automation · Data Quality · Spatial Data · ESRI
