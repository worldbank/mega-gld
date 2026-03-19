# Databricks notebook source
# ==============================================================================
#
# PURPOSE:
#   Reads variable_coverage.json and renders the full GLD Survey Finder
# ==============================================================================

import json
from IPython.display import HTML, display

# Load the JSON
with open("/Volumes/prd_csc_mega/sgld48/vgld48/Workspace/gld_website/variable_coverage.json", "r") as f:
    data = json.load(f)

# Inject data as inline JavaScript
surveys_json = json.dumps(data["surveys"])

html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: Arial, sans-serif; font-size: 13px; background: #f0f2f5; }}

  .site-header {{ background: #003366; color: white; padding: 14px 24px; }}
  .site-header h1 {{ font-size: 18px; font-weight: bold; }}
  .site-header p  {{ font-size: 12px; opacity: 0.8; margin-top: 2px; }}

  .tab-bar  {{ display: flex; flex-wrap: wrap; background: #dce3ed; border-bottom: 3px solid #003366; padding: 0 20px; }}
  .tab-btn  {{ padding: 10px 20px; cursor: pointer; font-size: 13px; color: #444;
               border: none; background: none; border-bottom: 3px solid transparent; margin-bottom: -3px; }}
  .tab-btn:hover  {{ background: #c8d3e3; color: #003366; }}
  .tab-btn.active {{ background: white; color: #003366; font-weight: bold; border-bottom: 3px solid white; }}

  .controls {{ display: flex; gap: 10px; align-items: center; padding: 10px 20px;
               background: white; border-bottom: 1px solid #ddd; flex-wrap: wrap; }}
  .controls input  {{ padding: 6px 10px; border: 1px solid #bbb; border-radius: 4px; font-size: 13px; width: 220px; }}
  .controls select {{ padding: 6px 10px; border: 1px solid #bbb; border-radius: 4px; font-size: 13px; }}
  .controls label  {{ font-size: 12px; color: #666; }}
  .row-count {{ font-size: 12px; color: #888; margin-left: auto; }}

  .panel {{ display: none; }}
  .panel.active {{ display: block; }}

  .note-banner {{ background: #fffbe6; border-left: 4px solid #f0a500; padding: 8px 16px;
                  font-size: 12px; color: #555; margin: 10px 20px 0; border-radius: 2px; }}

  .table-wrap {{ margin: 10px 20px 20px; overflow-x: auto; border: 1px solid #ccc;
                 border-radius: 4px; background: white; max-height: 65vh; overflow-y: auto; }}

  table {{ border-collapse: collapse; white-space: nowrap; width: 100%; }}

  tr.group-row th {{ background: #003366; color: white; font-size: 11px;
                     padding: 5px 8px; text-align: center; border-right: 1px solid #1a4d80; }}
  tr.group-row th.empty {{ background: #001f44; }}

  tr.def-row th {{ background: #e8eef5; color: #333; font-size: 11px; font-weight: normal;
                   padding: 4px 8px; border-bottom: 1px solid #ccc; border-right: 1px solid #ddd;
                   white-space: normal; max-width: 120px; vertical-align: top; text-align: center; }}

  tr.var-row th {{ background: #1a4d80; color: white; font-size: 11px; font-weight: bold;
                   padding: 5px 8px; border-right: 1px solid #2a5d90;
                   position: sticky; top: 0; z-index: 3; }}

  td {{ padding: 5px 8px; border-bottom: 1px solid #eee; border-right: 1px solid #f0f0f0;
        font-size: 12px; background: white; }}
  tr:hover td {{ background: #eef4ff !important; }}
  tr:nth-child(even) td {{ background: #f8f9fb; }}

  td.country {{ font-weight: bold; color: #003366; min-width: 60px; }}
  td.year    {{ color: #555; min-width: 55px; }}
  td.x-mark  {{ text-align: center; color: #1a7a1a; font-weight: bold; font-size: 14px; }}
  td.empty   {{ text-align: center; color: #ddd; font-size: 12px; }}
  td.text-val {{ color: #444; max-width: 130px; overflow: hidden; text-overflow: ellipsis; }}
</style>
</head>
<body>

<div class="site-header">
  <h1>GLD Survey Finder</h1>
  <p>Variable coverage across countries and survey years</p>
</div>

<div class="tab-bar" id="tabBar"></div>
<div id="allPanels"></div>

<script>
const ALL_SURVEYS = {surveys_json};

const TABS = [
  {{
    id: "data", label: "Data", note: null,
    groups: [
      {{ label: "Geographic Breakdown", cols: [
        {{ var: "urban", def: "Urban/Rural" }},
        {{ var: "supid", def: "Subnational identifier" }}
      ]}},
      {{ label: "Demographics", cols: [
        {{ var: "hsize", def: "Household Size" }},
        {{ var: "age",   def: "Age" }},
        {{ var: "male",  def: "Gender" }}
      ]}},
      {{ label: "Education", cols: [
        {{ var: "educat7", def: "Years of education: 7 Categories" }},
        {{ var: "educat5", def: "Years of education: 5 Categories" }},
        {{ var: "educat4", def: "Years of education: 4 Categories" }}
      ]}},
      {{ label: "Vocational Training", cols: [
        {{ var: "vocational",          def: "Any vocational training" }},
        {{ var: "vocational_type",     def: "Type" }},
        {{ var: "vocational_length_l", def: "Length" }},
        {{ var: "vocational_financed", def: "Financed by" }}
      ]}},
      {{ label: "Labor Status", cols: [
        {{ var: "lstatus",         def: "Labor market status" }},
        {{ var: "underemployment", def: "Underemployment" }},
        {{ var: "nlfreason",       def: "Reason for not in LF" }},
        {{ var: "empstat",         def: "Type of employment" }}
      ]}},
      {{ label: "Main Job — Occupation", cols: [
        {{ var: "occup",        def: "Occupation type: 10 Categories" }},
        {{ var: "occup_isco_x", def: "Occupation in ISCO codes" }},
        {{ var: "isco_version", def: "ISCO version" }}
      ]}},
      {{ label: "Main Job — Industry", cols: [
        {{ var: "industrycat10",      def: "Industry type: 10 categories" }},
        {{ var: "industrycat_isic_x", def: "Industry in ISIC codes" }},
        {{ var: "isic_version",       def: "ISIC Version" }}
      ]}},
      {{ label: "Main Job — Wages & Hours", cols: [
        {{ var: "wage_no_compen", def: "Wage" }},
        {{ var: "whours",         def: "Hours of Work" }},
        {{ var: "unitwage",       def: "Wage payment interval" }}
      ]}},
      {{ label: "Main Job — Job Quality", cols: [
        {{ var: "contract",  def: "Contract" }},
        {{ var: "healthins", def: "Health Insurance" }},
        {{ var: "socialsec", def: "Social Security" }}
      ]}},
      {{ label: "Main Job — Firm Size", cols: [
        {{ var: "firmsize_l", def: "Firm size: Lower limit" }},
        {{ var: "firmsize_u", def: "Firm size: Upper limit" }}
      ]}},
      {{ label: "Second Job", cols: [
        {{ var: "empstat_2", def: "Is there second job information" }}
      ]}},
      {{ label: "Recall Information", cols: [
        {{ var: "lstatus_year", def: "Is there 1 year recall information" }}
      ]}},
      {{ label: "Migration", cols: [
        {{ var: "migrated_binary",       def: "Migrated (binary)" }},
        {{ var: "migrated_years",        def: "Years since migration" }},
        {{ var: "migrated_from_urban",   def: "Migrated from urban" }},
        {{ var: "migrated_from_cat",     def: "Migrated from (category)" }},
        {{ var: "migrated_from_country", def: "Migrated from country" }},
        {{ var: "migrated_reason",       def: "Reason for migration" }}
      ]}},
      {{ label: "Disability", cols: [
        {{ var: "eye_dsablty",    def: "Disability (eye)" }},
        {{ var: "hear_dsablty",   def: "Disability (hearing)" }},
        {{ var: "walk_dsablty",   def: "Disability (walking)" }},
        {{ var: "conc_dsord",     def: "Concentration difficulty" }},
        {{ var: "slfcre_dsablty", def: "Self-care disability" }},
        {{ var: "comm_dsablty",   def: "Communication disability" }}
      ]}},
      {{ label: "Survey Details", cols: [
        {{ var: "icls_v",                   def: "ICLS Version" }},
        {{ var: "survname",                 def: "Survey Name" }},
        {{ var: "nationally_representative",def: "Nationally Representative" }}
      ]}},
      {{ label: "Potentials (not pre-constructed)", cols: [
        {{ var: "informality",       def: "Informal Sector" }},
        {{ var: "real_monthly_wage", def: "Real Monthly Earnings $" }}
      ]}}
    ]
  }},
  {{
    id: "area", label: "Area Disaggregation",
    note: "subnatid vars are admin units and represent different things in different countries and years. Sometimes not admin units.",
    groups: [
      {{ label: "Area Disaggregation", cols: [
        {{ var: "subnatid1",   def: "Level 1 Subnational ID available" }},
        {{ var: "n_subnatid1", def: "Level 1 Subnational ID unique values" }},
        {{ var: "subnatid2",   def: "Level 2 Subnational ID available" }},
        {{ var: "n_subnatid2", def: "Level 2 Subnational ID unique values" }},
        {{ var: "subnatid3",   def: "Level 3 Subnational ID available" }},
        {{ var: "n_subnatid3", def: "Level 3 Subnational ID unique values" }},
        {{ var: "subnatid4",   def: "Level 4 Subnational ID available" }},
        {{ var: "n_subnatid4", def: "Level 4 Subnational ID unique values" }}
      ]}}
    ]
  }},
  {{
    id: "occup", label: "Occupation Details",
    note: "ISCO depth: <75% ending in zero = likely 4-digit; ≥80% ending in zero = likely 3-digit; ≥80% ending in 00 = likely 2-digit. First jobs only.",
    groups: [
      {{ label: "Occupation Details", cols: [
        {{ var: "occup",             def: "Occupation type: 10 Categories" }},
        {{ var: "present",           def: "Occupation in ISCO codes" }},
        {{ var: "isco_version",      def: "ISCO version" }},
        {{ var: "likely_4digit",     def: "Likely at 4 digits" }},
        {{ var: "likely_3digit",     def: "Likely at 3 digits" }},
        {{ var: "likely_2digit",     def: "Likely at 2 digits" }}
      ]}}
    ]
  }},
  {{
    id: "industry", label: "Industry Details",
    note: "ISIC depth: Single letters (A–U) = Section; 2 digits = Division; 3 digits = Group; 4 digits = Class. Majority share ≥50% flags likely depth.",
    groups: [
      {{ label: "Industry Details", cols: [
        {{ var: "industrycat10",      def: "Industry type: 10 Categories" }},
        {{ var: "isic_present",       def: "Industry in ISIC codes" }},
        {{ var: "isic_version",       def: "ISIC version" }},
        {{ var: "isic_likely_4digit", def: "Likely at 4 digits (class)" }},
        {{ var: "isic_likely_3digit", def: "Likely at 3 digits (group)" }},
        {{ var: "isic_likely_2digit", def: "Likely at 2 digits (division)" }},
        {{ var: "section_present",    def: "Section level codes present (A–U)" }}
      ]}}
    ]
  }},
  {{
    id: "secondjob", label: "Second Job", note: null,
    groups: [
      {{ label: "Second Job Details", cols: [
        {{ var: "empstat_2",       def: "Type of employment: Second Job" }},
        {{ var: "occup_2",         def: "Occupation type: Second Job" }},
        {{ var: "industrycat10_2", def: "Industry Type: Second Job" }},
        {{ var: "wage_total_2",    def: "Wage: Second Job" }},
        {{ var: "whours_2",        def: "Hours of work: Second Job" }},
        {{ var: "unitwage_2",      def: "Wage payment interval: Second Job" }},
        {{ var: "firmsize_l_2",    def: "Firm size lower limit: Second Job" }},
        {{ var: "firmsize_u_2",    def: "Firm size upper limit: Second Job" }}
      ]}}
    ]
  }},
  {{
    id: "recall", label: "Yearly Recall", note: null,
    groups: [
      {{ label: "Labor Status", cols: [
        {{ var: "lstatus_year",         def: "Labor market status: Year" }},
        {{ var: "underemployment_year", def: "Underemployment: Year" }},
        {{ var: "nlfreason_year",       def: "Reason for not in LF: Year" }},
        {{ var: "unempldur_l_year",     def: "Unemployment duration (lower): Year" }},
        {{ var: "unempldur_u_year",     def: "Unemployment duration (upper): Year" }}
      ]}},
      {{ label: "Main Job", cols: [
        {{ var: "empstat_year",       def: "Type of employment: Year" }},
        {{ var: "occup_year",         def: "Occupation type: Year" }},
        {{ var: "industrycat10_year", def: "Industry Type: Year" }},
        {{ var: "wage_total_year",    def: "Wage: Year" }},
        {{ var: "whours_year",        def: "Hours of work: Year" }},
        {{ var: "unitwage_year",      def: "Wage payment interval: Year" }},
        {{ var: "contract_year",      def: "Contract: Year" }},
        {{ var: "healthins_year",     def: "Health Insurance: Year" }},
        {{ var: "socialsec_year",     def: "Social Security: Year" }}
      ]}},
      {{ label: "Second Job", cols: [
        {{ var: "empstat_2_year",        def: "Type of employment: Second Job Year" }},
        {{ var: "occup_2_year",          def: "Occupation type: Second Job Year" }},
        {{ var: "industrycat10_2_year",  def: "Industry Type: Second Job Year" }},
        {{ var: "wage_total_2_year",     def: "Wage: Second Job Year" }},
        {{ var: "whours_2_year",         def: "Hours of work: Second Job Year" }},
        {{ var: "unitwage_total_2_year", def: "Wage payment interval: Second Job Year" }}
      ]}}
    ]
  }}
];

function getVal(survey, varname) {{
  if (survey[varname] !== undefined) return survey[varname];
  if (survey.variables && survey.variables[varname] !== undefined) return survey.variables[varname];
  if (survey.isco_depth && survey.isco_depth[varname] !== undefined) return survey.isco_depth[varname];
  if (survey.isic_depth) {{
    const mapped = {{ "isic_present": "present", "isic_likely_4digit": "likely_4digit",
                      "isic_likely_3digit": "likely_3digit", "isic_likely_2digit": "likely_2digit" }};
    const key = mapped[varname] || varname;
    if (survey.isic_depth[key] !== undefined) return survey.isic_depth[key];
  }}
  return "";
}}

function buildTable(tab) {{
  const allCols = tab.groups.flatMap(g => g.cols);
  let html = "<table id='table-" + tab.id + "'><thead>";

  // Group row
  html += "<tr class='group-row'><th class='empty'></th><th class='empty'></th>";
  tab.groups.forEach(g => {{
    html += `<th colspan="${{g.cols.length}}">${{g.label}}</th>`;
  }});
  html += "</tr>";

  // Definition row
  html += "<tr class='def-row'><th>Country</th><th>Year</th>";
  allCols.forEach(c => {{ html += `<th>${{c.def}}</th>`; }});
  html += "</tr>";

  // Varname row
  html += "<tr class='var-row'><th>country</th><th>survey_year</th>";
  allCols.forEach(c => {{ html += `<th>${{c.var}}</th>`; }});
  html += "</tr></thead><tbody id='tbody-" + tab.id + "'>";

  // Data rows
  ALL_SURVEYS.forEach(s => {{
    html += `<tr data-country="${{s.country}}" data-year="${{s.survey_year}}">`;
    html += `<td class="country">${{s.country}}</td>`;
    html += `<td class="year">${{s.survey_year}}</td>`;
    allCols.forEach(c => {{
      const v = getVal(s, c.var);
      if (v === "X") html += `<td class="x-mark">✓</td>`;
      else if (v === "" || v == null) html += `<td class="empty">·</td>`;
      else html += `<td class="text-val" title="${{v}}">${{v}}</td>`;
    }});
    html += "</tr>";
  }});

  html += "</tbody></table>";
  return html;
}}

function filterTable(tabId) {{
  const search  = document.getElementById("search-" + tabId).value.toLowerCase();
  const country = document.getElementById("country-" + tabId).value;
  let visible = 0;
  document.querySelectorAll("#tbody-" + tabId + " tr").forEach(tr => {{
    const c = tr.dataset.country || "";
    const y = tr.dataset.year    || "";
    const show = (!search || c.toLowerCase().includes(search) || y.includes(search))
              && (!country || c === country);
    tr.style.display = show ? "" : "none";
    if (show) visible++;
  }});
  document.getElementById("count-" + tabId).textContent = visible + " survey" + (visible !== 1 ? "s" : "");
}}

function switchTab(tabId) {{
  document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
  document.querySelectorAll(".panel").forEach(p => p.classList.remove("active"));
  document.querySelector(".tab-btn[data-id='" + tabId + "']").classList.add("active");
  document.getElementById("panel-" + tabId).classList.add("active");
}}

// Build everything
const tabBar   = document.getElementById("tabBar");
const allPanels = document.getElementById("allPanels");
const countries = [...new Set(ALL_SURVEYS.map(s => s.country))].sort();

TABS.forEach((tab, idx) => {{
  // Tab button
  const btn = document.createElement("button");
  btn.className = "tab-btn" + (idx === 0 ? " active" : "");
  btn.dataset.id = tab.id;
  btn.textContent = tab.label;
  btn.onclick = () => switchTab(tab.id);
  tabBar.appendChild(btn);

  // Panel
  const panel = document.createElement("div");
  panel.className = "panel" + (idx === 0 ? " active" : "");
  panel.id = "panel-" + tab.id;

  let inner = `
    <div class="controls">
      <label>Search:</label>
      <input type="text" id="search-${{tab.id}}" placeholder="Filter by country or year..." oninput="filterTable('${{tab.id}}')">
      <label>Country:</label>
      <select id="country-${{tab.id}}" onchange="filterTable('${{tab.id}}')">
        <option value="">All countries</option>
        ${{countries.map(c => `<option value="${{c}}">${{c}}</option>`).join("")}}
      </select>
      <span class="row-count" id="count-${{tab.id}}">${{ALL_SURVEYS.length}} surveys</span>
    </div>`;

  if (tab.note) {{
    inner += `<div class="note-banner">Note: ${{tab.note}}</div>`;
  }}

  inner += `<div class="table-wrap">${{buildTable(tab)}}</div>`;
  panel.innerHTML = inner;
  allPanels.appendChild(panel);
}});
</script>
</body>
</html>
"""

display(HTML(html))

# COMMAND ----------

import json
with open("/Workspace/Users/jbajwa@worldbank.org/variable_coverage.json", "r") as f:
    data = json.load(f)

for s in data["surveys"]:
    if s["country"] == "BGD" and s["survey_year"] == "2005":
        print("isco_depth:", s.get("isco_depth"))
        print("isic_depth:", s.get("isic_depth"))
        break

# COMMAND ----------

print(spark.sql("SELECT current_catalog(), current_schema()").collect())

# COMMAND ----------

# Check what BGD tables exist
tables = spark.sql("SHOW TABLES IN prd_csc_mega.sgld48 LIKE 'bgd*'").collect()
for t in tables:
    print(t)

# COMMAND ----------

spark.sql("SHOW CATALOGS").show()

# COMMAND ----------

spark.sql("SHOW SCHEMAS IN prd_csc_mega").show()


# COMMAND ----------

tables = spark.sql("SHOW TABLES IN sgld48 LIKE 'bgd*'").collect()
for t in tables:
    print(t)
    