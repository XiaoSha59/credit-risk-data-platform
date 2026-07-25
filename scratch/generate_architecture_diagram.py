import matplotlib.pyplot as plt
import matplotlib.patches as patches
import os

os.makedirs("d:/credit-risk-data-platform/docs/images", exist_ok=True)

# 1. Figure Setup with Ultra HD resolution & Dark Theme
fig, ax = plt.subplots(figsize=(24, 15), dpi=300)
bg_color = '#0B0F19' # Deep Space Slate
ax.set_facecolor(bg_color)
fig.patch.set_facecolor(bg_color)

# 2. Header & Title Block
ax.text(12.0, 14.3, "CREDIT RISK DATA PLATFORM", fontsize=24, fontweight='bold', color='#F8FAFC',
        ha='center', va='center', family='sans-serif')
ax.text(12.0, 13.7, "Deployable Unit Architecture & Multi-Flow Data Lineage", fontsize=13, fontweight='bold', color='#38BDF8',
        ha='center', va='center', family='sans-serif')

# Helper: Draw Subgraph Zone Box
def draw_zone(ax, x, y, w, h, title, border_color='#38BDF8'):
    zone_patch = patches.FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.05,rounding_size=0.3",
                                        linewidth=1.5, edgecolor=border_color, facecolor='#0F172A', alpha=0.4, zorder=1)
    ax.add_patch(zone_patch)
    # Zone Title Badge
    ax.text(x + 0.4, y + h - 0.35, title.upper(), fontsize=10, fontweight='bold', color=border_color,
            ha='left', va='top', zorder=2, family='sans-serif')

# Subgraph Zones
draw_zone(ax, 0.6, 8.2, 16.2, 5.0, "1. Real-Time Streaming Ingestion Layer", "#38BDF8")
draw_zone(ax, 0.6, 4.2, 16.8, 3.6, "2. Offline Batch & Medallion Lakehouse Layer", "#4ADE80")
draw_zone(ax, 5.5, -2.6, 11.8, 6.4, "3. Serving Data Warehouse & Governance Layer", "#C084FC")
draw_zone(ax, 17.5, 0.2, 5.0, 13.0, "4. Users & Operational Dashboards", "#FB923C")

# Helper: Draw Container Node (Deployable Unit Card)
def draw_node(ax, x, y, w, h, badge, name, subtitle, tag_color='#38BDF8'):
    # Shadow
    shadow = patches.FancyBboxPatch((x+0.06, y-0.06), w, h, boxstyle="round,pad=0.03,rounding_size=0.15",
                                    linewidth=0, facecolor='#000000', alpha=0.5, zorder=2)
    ax.add_patch(shadow)
    
    # Main Card
    card = patches.FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.03,rounding_size=0.15",
                                  linewidth=2, edgecolor=tag_color, facecolor='#1E293B', zorder=3)
    ax.add_patch(card)
    
    # Top Accent Bar
    accent = patches.FancyBboxPatch((x+0.05, y+h-0.12), w-0.1, 0.08, boxstyle="round,pad=0,rounding_size=0.02",
                                    linewidth=0, facecolor=tag_color, zorder=4)
    ax.add_patch(accent)
    
    # Badge (e.g. DOCKER / FLINK / SPARK)
    ax.text(x + 0.3, y + h - 0.35, f"[{badge}]", fontsize=8.5, fontweight='bold', color=tag_color,
            ha='left', va='top', zorder=5, family='sans-serif')
    
    # Name
    ax.text(x + w/2, y + h - 0.75, name, fontsize=11.5, fontweight='bold', color='#F8FAFC',
            ha='center', va='top', zorder=5, family='sans-serif')
    
    # Subtitle / Container Name
    ax.text(x + w/2, y + 0.35, subtitle, fontsize=8.5, fontweight='bold', color='#94A3B8',
            ha='center', va='bottom', zorder=5, family='sans-serif')

# Define Deployable Unit Nodes
nodes = {
    "data_gen": (1.1, 9.0, 3.4, 1.8, "DOCKER CONTAINER", "Data Generator", "platform-data-generator", "#38BDF8"),
    "zookeeper": (6.2, 11.4, 3.2, 1.4, "DOCKER CONTAINER", "Zookeeper", "platform-zookeeper", "#64748B"),
    "kafka": (6.2, 8.8, 3.4, 1.8, "MESSAGE BROKER", "Kafka Broker", "platform-kafka:9092", "#38BDF8"),
    "flink": (12.0, 8.8, 3.8, 1.8, "STREAM CLUSTER", "PyFlink Stream Engine", "Flink TaskManager Workers", "#38BDF8"),
    
    "airflow": (1.1, 5.0, 3.4, 1.8, "ORCHESTRATOR", "Airflow Master", "credit_risk_airflow", "#4ADE80"),
    "spark": (6.8, 5.0, 3.8, 1.8, "BATCH ENGINE", "PySpark Engine", "Spark Exec Cluster", "#4ADE80"),
    "lakehouse": (12.8, 5.0, 4.0, 1.8, "DATA LAKE STORAGE", "Delta Lakehouse", "Bronze / Silver / Gold Storage", "#4ADE80"),
    
    "postgres": (12.8, 0.8, 4.0, 1.8, "POSTGRES DB", "PostgreSQL DW", "credit_risk_postgres:5432", "#FB923C"),
    "datahub": (6.8, 0.8, 3.8, 1.8, "GOVERNANCE SERVER", "DataHub Server", "platform-datahub-gms", "#C084FC"),
    "datahub_ui": (6.8, -2.2, 3.8, 1.6, "WEB APPLICATION", "DataHub Web UI", "Data Catalog Dashboard", "#C084FC"),
    
    "webui": (18.0, 8.8, 4.0, 1.8, "WEB DASHBOARD", "Flink & Airflow UI", "Port 8081 / 8085", "#FACC15"),
    "analyst": (18.0, 0.8, 4.0, 1.8, "SQL CLIENT", "Risk Analyst / DBeaver", "SQL Analytical Queries", "#FB923C")
}

for key, n in nodes.items():
    draw_node(ax, n[0], n[1], n[2], n[3], n[4], n[5], n[6], n[7])

# Helper: Flow Arrow with Step Badge
def draw_arrow(ax, start, end, step, label, color='#38BDF8', rad=0.0, label_pos=0.5):
    kw = dict(arrowstyle="-|>", color=color, lw=2.5, mutation_scale=18, linestyle='-')
    ax.annotate("", xy=end, xytext=start, arrowprops=dict(**kw, connectionstyle=f"arc3,rad={rad}"), zorder=6)
    
    mx = start[0] + (end[0] - start[0]) * label_pos
    my = start[1] + (end[1] - start[1]) * label_pos
    
    if rad != 0:
        mx += rad * 0.4 * (end[1] - start[1])
        my -= rad * 0.4 * (end[0] - start[0])
        
    bbox_props = dict(boxstyle="round,pad=0.35,rounding_size=0.1", facecolor='#090D16', edgecolor=color, linewidth=1.5, alpha=0.95)
    ax.text(mx, my, f" {step}  {label} ", fontsize=8.5, fontweight='bold', color='#FFFFFF',
            ha='center', va='center', zorder=7, bbox=bbox_props, family='sans-serif')

# --- FLOW 1: Real-Time Online Streaming (Blue: #38BDF8) ---
draw_arrow(ax, (4.5, 9.7), (6.2, 9.7), "1.1", "Raw Streaming Events", "#38BDF8", label_pos=0.5)
draw_arrow(ax, (9.6, 9.7), (12.0, 9.7), "1.2", "Topic: credit_risk_events", "#38BDF8", label_pos=0.5)
draw_arrow(ax, (15.8, 9.7), (18.0, 9.7), "1.3", "Stream Metrics & Logs", "#38BDF8", label_pos=0.5)
draw_arrow(ax, (13.9, 8.8), (14.8, 6.8), "1.4", "Stream Checkpoints & Parquet Sink", "#38BDF8", rad=0.1, label_pos=0.5)

# --- FLOW 2: Offline Batch & Lakehouse Pipeline (Green: #4ADE80) ---
draw_arrow(ax, (4.5, 5.9), (6.8, 5.9), "2.1", "Triggers Ingest & ETL DAGs", "#4ADE80", label_pos=0.5)
draw_arrow(ax, (12.8, 6.2), (10.6, 6.2), "2.2", "Reads Raw Source Parquet", "#4ADE80", label_pos=0.5)
draw_arrow(ax, (10.6, 5.4), (12.8, 5.4), "2.3", "Writes Bronze/Silver/Gold Delta", "#4ADE80", label_pos=0.5)
draw_arrow(ax, (9.8, 5.0), (13.4, 2.6), "2.4", "Syncs Gold DW OBT & Dim/Fact", "#4ADE80", rad=-0.1, label_pos=0.55)

# --- FLOW 3: Data Governance & Quality Audit (Purple: #C084FC) ---
draw_arrow(ax, (8.7, 5.0), (8.7, 2.6), "3.1", "Lineage, Schema Contracts & Quality Meta", "#C084FC", label_pos=0.5)
draw_arrow(ax, (8.7, 0.8), (8.7, -0.6), "3.2", "Lineage Graph & Catalog Metadata", "#C084FC", label_pos=0.5)

# --- FLOW 4: User Analytics & Operational Control (Orange/Yellow) ---
draw_arrow(ax, (18.0, 1.7), (16.8, 1.7), "4.1", "SQL Queries on Gold 360 Risk Tables", "#FB923C", label_pos=0.5)
draw_arrow(ax, (2.8, 6.8), (18.0, 10.2), "4.2", "Monitors & Triggers DAGs / Streaming", "#FACC15", rad=-0.55, label_pos=0.5)

# Internal Cluster Coordination (Zookeeper - Kafka)
ax.annotate("", xy=(7.8, 8.8), xytext=(7.8, 11.4),
            arrowprops=dict(arrowstyle="<->", color="#64748B", lw=1.5, linestyle="--"), zorder=5)
ax.text(7.8, 10.4, " Cluster State Sync ", fontsize=7.5, color="#CBD5E1", ha="center", va="center", zorder=6,
        bbox=dict(boxstyle="round,pad=0.2", facecolor="#0B0F19", edgecolor="#64748B", lw=1))

# Legend Box (Modern Control Panel Style)
leg_bg = patches.FancyBboxPatch((0.6, -2.6), 5.2, 2.2, boxstyle="round,pad=0.03,rounding_size=0.15",
                                linewidth=1.5, edgecolor='#334155', facecolor='#1E293B', zorder=3)
ax.add_patch(leg_bg)
ax.text(0.9, -0.65, "PIPELINE FLOW LEGEND", fontsize=9.5, fontweight='bold', color='#F8FAFC', zorder=4)
ax.text(0.9, -1.05, "━ Flow 1: Online Real-Time Streaming", fontsize=8.5, fontweight='bold', color='#38BDF8', zorder=4)
ax.text(0.9, -1.45, "━ Flow 2: Offline Batch Lakehouse Pipeline", fontsize=8.5, fontweight='bold', color='#4ADE80', zorder=4)
ax.text(0.9, -1.85, "━ Flow 3: Data Governance & Quality Audit", fontsize=8.5, fontweight='bold', color='#C084FC', zorder=4)
ax.text(0.9, -2.25, "━ Flow 4: User Analytics & Dashboards", fontsize=8.5, fontweight='bold', color='#FB923C', zorder=4)

# Principles Box
p_bg = patches.FancyBboxPatch((11.5, -2.6), 11.0, 2.2, boxstyle="round,pad=0.03,rounding_size=0.15",
                              linewidth=1.5, edgecolor='#38BDF8', facecolor='#1E293B', zorder=3)
ax.add_patch(p_bg)
ax.text(11.8, -0.65, "ARCHITECTURE PRINCIPLES & COMPLIANCE:", fontsize=9.5, fontweight='bold', color='#38BDF8', zorder=4)
ax.text(11.8, -1.05, "• Deployable Units Only: Every box represents an independently deployable container, cluster service, or external UI.", fontsize=8, color='#CBD5E1', zorder=4)
ax.text(11.8, -1.45, "• Data Flow Directions: All arrows strictly follow data transmission with explicit payload descriptions on arrow badges.", fontsize=8, color='#CBD5E1', zorder=4)
ax.text(11.8, -1.85, "• Multi-Flow Sequence: Distinct color-coded flows with step numbers (1.1-1.4, 2.1-2.4, 3.1-3.2, 4.1-4.2) for total clarity.", fontsize=8, color='#CBD5E1', zorder=4)
ax.text(11.8, -2.25, "• Solid vs Dashed Lines: Main data paths use solid lines; dashed lines are strictly reserved for internal broker sync.", fontsize=8, color='#CBD5E1', zorder=4)

ax.set_xlim(-0.2, 23.2)
ax.set_ylim(-3.2, 15.0)
ax.axis('off')

plt.tight_layout()
plt.savefig("d:/credit-risk-data-platform/docs/images/architecture-diagram.png", dpi=300, bbox_inches='tight')
print("Successfully generated super clean docs/images/architecture-diagram.png")
