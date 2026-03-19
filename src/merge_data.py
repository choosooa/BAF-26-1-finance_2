import pandas as pd
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "datas")

# ── 1. 일별 데이터 ──────────────────────────────────────────────
daily_files = {
    "FRED_DollarIndex.csv":       "DTWEXBGS",
    "FRED_OilPrice.csv":          "DCOILWTICO",
    "FRED_TermSpread.csv":        "T10Y2Y",
    "FRED_TreasuryYield.csv":     "DGS10",
    "FRED_VIX.csv":               "VIXCLS",
}

# ── 2. 월별 데이터 ──────────────────────────────────────────────
monthly_files = {
    "FRED_CPI_month.csv":              "CPIAUCSL",
    "FRED_FedFundsRate.csv":           "FEDFUNDS",
    "FRED_IndustryProduction.csv":     "INDPRO",
    "FRED_RealInterestRate_month.csv": "REAINTRATREARAT10Y",
}

# ── 3. 주별 데이터 ──────────────────────────────────────────────
# eia 파일은 상단 4줄 메타데이터 + 1줄 헤더 구조
weekly_files = {
    "eia_OilInventories_week.csv": "OilInventories_week",
    "eia_OilProduction_week.csv":  "OilProduction_week",
}


def load_daily(filename, col):
    path = os.path.join(DATA_DIR, filename)
    df = pd.read_csv(path, parse_dates=["observation_date"])
    df = df.rename(columns={"observation_date": "date", col: col})
    df = df.set_index("date")[[col]]
    return df


def load_monthly(filename, col):
    path = os.path.join(DATA_DIR, filename)
    df = pd.read_csv(path, parse_dates=["observation_date"])
    df = df.rename(columns={"observation_date": "date", col: col})
    df = df.set_index("date")[[col]]
    return df  # 월별 날짜 그대로 유지 (나머지는 NaN)


def load_weekly_eia(filename, new_col):
    path = os.path.join(DATA_DIR, filename)
    # 5번째 줄(index=4)이 실제 헤더
    df = pd.read_csv(path, skiprows=4)
    df.columns = ["date", new_col]
    df = df.assign(date=pd.to_datetime(df["date"]))
    df = df.set_index("date")[[new_col]]
    return df  # 주별 날짜 그대로 유지 (나머지는 NaN)


def load_opec_production(filename, new_col):
    path = os.path.join(DATA_DIR, filename)
    # 행: 국가별 지표, 열: 날짜(Jan 1973, ...)
    # 1행: 메타, 2행: 날짜 헤더 → skiprows=1로 읽기
    raw = pd.read_csv(path, skiprows=1, header=None)
    date_cols = raw.iloc[0, 2:]  # "Jan 1973", "Feb 1973", ...
    # INTL.53-1-*-TBPD.M: 국가별 총 석유 생산량 행 추출
    mask = raw[0].str.match(r"INTL\.53-1-\w+-TBPD\.M", na=False)
    values = raw.loc[mask, 2:].apply(pd.to_numeric, errors="coerce")
    total = values.sum(axis=0)
    dates = pd.to_datetime(date_cols.values, format="%b %Y")
    result = pd.DataFrame({new_col: total.values}, index=dates)
    result.index.name = "date"
    return result


# ── 일별 데이터로 공통 날짜 인덱스 생성 ──────────────────────────
daily_frames = [load_daily(f, c) for f, c in daily_files.items()]
merged = daily_frames[0]
for df in daily_frames[1:]:
    merged = merged.join(df, how="outer")

# ── 월별/주별 데이터 병합 (해당 날짜에만 값, 나머지 NaN) ─────────
for filename, col in monthly_files.items():
    df = load_monthly(filename, col)
    merged = merged.join(df, how="outer")

for filename, col in weekly_files.items():
    df = load_weekly_eia(filename, col)
    merged = merged.join(df, how="outer")

# ── OPEC 생산량 (월별, 국가 합산) ────────────────────────────────
df_opec = load_opec_production("eia_OPECProduction_month.csv", "OPECProduction_month")
merged = merged.join(df_opec, how="outer")

# ── 인덱스 정렬 & 기간 필터 ──────────────────────────────────
merged = merged.sort_index()
merged = merged[merged.index >= "2006-01-01"]
merged.index.name = "date"

output_path = os.path.join(DATA_DIR, "F_merged.csv")
merged.to_csv(output_path)

print(f"저장 완료: {output_path}")
print(f"shape: {merged.shape}")
print(merged.head(10).to_string())
