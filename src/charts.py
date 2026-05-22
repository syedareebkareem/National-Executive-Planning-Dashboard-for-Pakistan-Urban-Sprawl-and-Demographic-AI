import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Pathing Fix
project_root = os.getcwd()
java11_dir = os.path.join(project_root, "java11_folder")
for item in os.listdir(java11_dir):
    if "jdk" in item.lower():
        os.environ['JAVA_HOME'] = os.path.join(java11_dir, item)
        break
os.environ['HADOOP_HOME'] = r'C:\hadoop'

# 2. Initialize Spark
spark = SparkSession.builder.appName("Charts").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

os.makedirs(os.path.join(project_root, "charts"), exist_ok=True)
sns.set_theme(style="darkgrid")
print("\n" + "="*45)
print("  VISUALIZATION: MATPLOTLIB & SEABORN CHARTS")
print("="*45)

# ─────────────────────────────────────────────
# CHART 1: National Population Growth Bar Chart
# ─────────────────────────────────────────────
print("[+] Generating Chart 1: Population Growth Timeline...")
years  = [1998, 2017, 2022, 2030]
pop_m  = [130.7, 204.4, 233.9, 280.5] # Updated 2022 prediction
colors = ['#32CD32', '#1E90FF', '#FFA500', '#FF0044']

fig, ax = plt.subplots(figsize=(10, 6))
bars = ax.bar(years, pop_m, color=colors, width=4, edgecolor='white', linewidth=0.8)
ax.set_title("Pakistan National Population Growth (1998–2030)", fontsize=16, fontweight='bold', pad=15)
ax.set_xlabel("Year", fontsize=13)
ax.set_ylabel("Population (Millions)", fontsize=13)
ax.set_xticks(years)
ax.set_ylim(0, 320)

for bar, val in zip(bars, pop_m):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 3,
            f'{val}M', ha='center', va='bottom', fontweight='bold', fontsize=11)

patches = [mpatches.Patch(color=c, label=f'{y} Census') for c, y in zip(colors, years)]
ax.legend(handles=patches, loc='upper left')
plt.tight_layout()
out = os.path.join(project_root, "charts", "population_growth.png")
plt.savefig(out, dpi=150)
plt.close()
print(f"    Saved -> {out}")

# ─────────────────────────────────────────────
# CHART 2: Top 15 Sprawl Epicenters Risk Bar
# ─────────────────────────────────────────────
print("[+] Generating Chart 2: Risk Ranking of Sprawl Epicenters...")
risk_path = os.path.join(project_root, "data", "predictions", "risk_ranking_2030.csv")

if os.path.exists(risk_path):
    risk_df = pd.read_csv(risk_path).head(15)
    risk_df["Label"] = risk_df.apply(
        lambda r: f"({r['Latitude']:.1f}, {r['Longitude']:.1f})", axis=1
    )
    palette = ["#FF0044" if i < 3 else "#FFA500" if i < 7 else "#1E90FF"
               for i in range(len(risk_df))]

    fig, ax = plt.subplots(figsize=(12, 7))
    sns.barplot(data=risk_df, x="sprawl_pixels", y="Label",
                palette=palette, ax=ax, orient='h')
    ax.set_title("Top 15 Urban Sprawl Epicenters — Risk Ranking 2030",
                 fontsize=15, fontweight='bold', pad=12)
    ax.set_xlabel("Sprawl Area (Grid Pixels)", fontsize=12)
    ax.set_ylabel("Epicenter Location (Lat, Lon)", fontsize=12)

    high  = mpatches.Patch(color='#FF0044', label='HIGH RISK (Top 3)')
    med   = mpatches.Patch(color='#FFA500', label='MEDIUM RISK')
    low   = mpatches.Patch(color='#1E90FF', label='LOWER RISK')
    ax.legend(handles=[high, med, low], loc='lower right')
    plt.tight_layout()
    out = os.path.join(project_root, "charts", "risk_ranking.png")
    plt.savefig(out, dpi=150)
    plt.close()
    print(f"    Saved -> {out}")
else:
    print("    [!] Skipped — run risk_dashboard.py first to generate risk_ranking_2030.csv")

# ─────────────────────────────────────────────
# CHART 3: Urban vs Rural Pixel Distribution
# ─────────────────────────────────────────────
print("[+] Generating Chart 3: Urban vs Rural Distribution...")
features_path = os.path.join(project_root, "data", "processed", "ml_features.parquet")

if os.path.exists(features_path):
    df = spark.read.parquet(features_path)
    total  = df.count()
    urban  = df.filter(col("is_urban") == 1).count()
    rural  = total - urban

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    # Pie Chart
    axes[0].pie([urban, rural], labels=['Urban', 'Rural'],
                colors=['#FF0044', '#32CD32'], autopct='%1.1f%%',
                startangle=140, textprops={'fontsize': 13})
    axes[0].set_title("Land Classification Split\n(WorldPop 2020 Baseline)",
                      fontsize=13, fontweight='bold')

    # KDE Density Plot
    df_sample = df.sample(fraction=0.05, seed=42).select("density_score").toPandas()
    sns.kdeplot(data=df_sample, x="density_score", fill=True,
                color="#1E90FF", ax=axes[1])
    axes[1].set_title("Population Density Score Distribution\n(Log-Transformed)",
                      fontsize=13, fontweight='bold')
    axes[1].set_xlabel("Density Score (log1p)")
    axes[1].set_ylabel("Frequency")

    plt.suptitle("Pakistan Spatial Data Overview", fontsize=15,
                 fontweight='bold', y=1.01)
    plt.tight_layout()
    out = os.path.join(project_root, "charts", "urban_rural_distribution.png")
    plt.savefig(out, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"    Saved -> {out}")
else:
    print("    [!] Skipped — run features.py first")

# ─────────────────────────────────────────────
# CHART 4: Urbanization Rate Line Chart
# ─────────────────────────────────────────────
print("[+] Generating Chart 4: Urbanization Rate Trend...")
data = {
    'Year':             [1998, 2017, 2022, 2030],
    'Urban %':          [32,   36,   39,   45],
    'Rural %':          [68,   64,   61,   55],
}
df_trend = pd.DataFrame(data)

fig, ax = plt.subplots(figsize=(10, 6))
ax.plot(df_trend['Year'], df_trend['Urban %'], marker='o', color='#FF0044',
        linewidth=2.5, markersize=8, label='Urban Population %')
ax.plot(df_trend['Year'], df_trend['Rural %'], marker='s', color='#32CD32',
        linewidth=2.5, markersize=8, label='Rural Population %')
ax.fill_between(df_trend['Year'], df_trend['Urban %'], alpha=0.15, color='#FF0044')
ax.fill_between(df_trend['Year'], df_trend['Rural %'], alpha=0.10, color='#32CD32')

for _, row in df_trend.iterrows():
    ax.annotate(f"{row['Urban %']}%", (row['Year'], row['Urban %']),
                textcoords="offset points", xytext=(0, 10), ha='center', fontsize=10)

ax.axvline(x=2022, color='gray', linestyle='--', alpha=0.5, label='Projection Starts')
ax.set_title("Pakistan Urbanization Rate Trend (1998–2030)",
             fontsize=15, fontweight='bold', pad=12)
ax.set_xlabel("Year", fontsize=12)
ax.set_ylabel("Population Share (%)", fontsize=12)
ax.set_ylim(0, 100)
ax.set_xticks([1998, 2017, 2022, 2030])
ax.legend(fontsize=11)
plt.tight_layout()
out = os.path.join(project_root, "charts", "urbanization_trend.png")
plt.savefig(out, dpi=150)
plt.close()
print(f"    Saved -> {out}")

print("\n[SUCCESS] All Charts Generated Successfully!")
print(f"   Find them in: {os.path.join(project_root, 'charts')}/")
spark.stop()