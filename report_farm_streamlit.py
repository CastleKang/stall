#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
report_farm_streamlit.py

Streamlit을 활용한 Farm Report 앱:
  - 좌측 사이드바에서 농장(Farm)과 상태(Status)를 선택합니다.
  - 선택한 농장의 총 개체수, 암컷(Heifer) 수, 수퇘(‘Bull’) 수 등 기본 정보를 출력합니다.
  - 최근 3개월 동안의 Births, Calf Tags, Cull Count, Sales, Deaths 등의 관리 정보를 동적 계산합니다.
  - Usage & Reproduction 정보와 함께, 번식 진단, 개체 구분, 착유량 추이 등의 그래프를 표시합니다.
"""

import streamlit as st
import sqlite3, os, io
from datetime import datetime, timedelta, date
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# DB 경로 설정 (현재 파일과 동일한 디렉토리의 animals.db)
DB_PATH = os.path.join(os.path.dirname(__file__), "animals.db")

###############################################################################
# 농장 관련 데이터 계산 함수들 (상태 필터 적용)
###############################################################################
def get_filtered_ear_tags(farm, status):
    """
    만약 status가 "Total"이면 해당 농장의 모든 개체 ear_tag를 반환하고,
    그렇지 않으면 COALESCE(issue.event_status, 'owned')가 status와 일치하는 ear_tag를 반환합니다.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        if status != "Total":
            query = """
                SELECT a.ear_tag
                  FROM animals a
                  LEFT JOIN issue i ON a.farm_name = i.farm_name AND a.ear_tag = i.ear_tag
                 WHERE a.farm_name=? AND COALESCE(i.event_status, 'owned') = ?
                 ORDER BY a.ear_tag
            """
            c.execute(query, (farm, status))
        else:
            query = """
                SELECT ear_tag
                  FROM animals
                 WHERE farm_name=?
                 ORDER BY ear_tag
            """
            c.execute(query, (farm,))
        rows = c.fetchall()
        conn.close()
        return [row[0] for row in rows if row[0]]
    except Exception as e:
        st.error(f"get_filtered_ear_tags error: {e}")
        return []

def get_total_animals_filtered(farm, status):
    return len(get_filtered_ear_tags(farm, status))

def get_farm_total_milk_yield_filtered(farm, status):
    ear_tags = get_filtered_ear_tags(farm, status)
    if not ear_tags:
        return 0.0
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        placeholders = ",".join("?" for _ in ear_tags)
        query = f"SELECT SUM(yield_value) FROM milk_yield WHERE ear_tag IN ({placeholders})"
        c.execute(query, ear_tags)
        row = c.fetchone()
        conn.close()
        return row[0] if row and row[0] is not None else 0.0
    except Exception as e:
        st.error(f"get_farm_total_milk_yield_filtered error: {e}")
        return 0.0

def get_farm_total_lactation_days_filtered(farm, status):
    ear_tags = get_filtered_ear_tags(farm, status)
    if not ear_tags:
        return 0
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        placeholders = ",".join("?" for _ in ear_tags)
        query = f"SELECT COUNT(DISTINCT record_date) FROM milk_yield WHERE ear_tag IN ({placeholders})"
        c.execute(query, ear_tags)
        row = c.fetchone()
        conn.close()
        return row[0] if row and row[0] is not None else 0
    except Exception as e:
        st.error(f"get_farm_total_lactation_days_filtered error: {e}")
        return 0

def get_farm_milk_yield_by_year_filtered(farm, status):
    ear_tags = get_filtered_ear_tags(farm, status)
    if not ear_tags:
        return []
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        placeholders = ",".join("?" for _ in ear_tags)
        query = f"""
            SELECT record_year, SUM(yield_value) 
              FROM milk_yield 
             WHERE ear_tag IN ({placeholders})
             GROUP BY record_year
        """
        c.execute(query, ear_tags)
        rows = c.fetchall()
        conn.close()
        rows.sort(key=lambda x: x[0])
        return rows
    except Exception as e:
        st.error(f"get_farm_milk_yield_by_year_filtered error: {e}")
        return []

def get_farm_abortion_count_filtered(farm, status):
    ear_tags = get_filtered_ear_tags(farm, status)
    if not ear_tags:
        return 0
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        placeholders = ",".join("?" for _ in ear_tags)
        query = f"""
            SELECT COUNT(*) FROM repro 
             WHERE animal_id IN (
                SELECT id FROM animals 
                WHERE ear_tag IN ({placeholders})
             )
             AND lower(delivery_status) = 'abortion'
        """
        c.execute(query, ear_tags)
        row = c.fetchone()
        conn.close()
        return row[0] if row and row[0] is not None else 0
    except Exception as e:
        st.error(f"get_farm_abortion_count_filtered error: {e}")
        return 0

###############################################################################
# Heifer & Bull Count Functions
###############################################################################
def get_heifer_count(farm):
    """24개월 미만인 암컷(F) 개체 수 계산"""
    try:
        today = date.today()
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT birth_date FROM animals WHERE farm_name=? AND gender='F'", (farm,))
        rows = c.fetchall()
        count = 0
        for row in rows:
            birth_date_str = row[0]
            if birth_date_str and birth_date_str.strip():
                try:
                    birth_date = datetime.strptime(birth_date_str, "%Y-%m-%d").date()
                    age_months = (today - birth_date).days / 30.5
                    if age_months < 24:
                        count += 1
                except:
                    continue
        conn.close()
        return count
    except Exception as e:
        st.error(f"get_heifer_count error: {e}")
        return 0

def get_bull_count(farm):
    """DB에서 성별이 'Bull'인 개체 수 계산"""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM animals WHERE farm_name=? AND gender='Bull'", (farm,))
        row = c.fetchone()
        conn.close()
        return row[0] if row and row[0] is not None else 0
    except Exception as e:
        st.error(f"get_bull_count error: {e}")
        return 0

###############################################################################
# Breeding Diagnosis & Animal Category 계산 함수
###############################################################################
def compute_breeding_diagnosis(ear_tag):
    """
    번식진단 (Breeding Diagnosis) 로직:
      - 동물의 성별이 F가 아니면 "Open" 반환.
      - F이면 repro 테이블에서 가장 최신 레코드를 확인.
          * calving_date가 있으면 "Open"
          * calving_date가 없고, pregnancy_status가 "임신"이면 "Pregnant"
          * 그 외에는 "Mating"
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT id, gender FROM animals WHERE ear_tag=?", (ear_tag,))
        row = c.fetchone()
        if not row:
            conn.close()
            return "Open"
        animal_id, gender = row
        if gender != "F":
            conn.close()
            return "Open"
        c.execute("SELECT calving_date, pregnancy_status FROM repro WHERE animal_id=? ORDER BY breeding_date DESC LIMIT 1", (animal_id,))
        repro = c.fetchone()
        conn.close()
        if not repro:
            return "Mating"
        calving_date, pregnancy_status = repro
        if calving_date and calving_date.strip() != "":
            return "Open"
        if pregnancy_status and pregnancy_status.strip() == "임신":
            return "Pregnant"
        return "Mating"
    except Exception as e:
        st.error(f"compute_breeding_diagnosis error: {e}")
        return "Open"

def compute_animal_category(ear_tag):
    """
    개체구분 (Animal Category) 로직:
      - lactation 데이터가 없으면 "Fattening"
      - 있으면 가장 최근 lactation period("start ~ end")의 end date 기준,
        오늘과의 차이가 10일 이하면 "Milking", 그 이상이면 "Dry"
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT id FROM animals WHERE ear_tag=?", (ear_tag,))
        row = c.fetchone()
        if not row:
            conn.close()
            return "Fattening"
        animal_id = row[0]
        c.execute("SELECT period FROM lactation WHERE animal_id=? ORDER BY period DESC LIMIT 1", (animal_id,))
        row = c.fetchone()
        conn.close()
        if not row or not row[0]:
            return "Fattening"
        period = row[0]
        if "~" not in period:
            return "Fattening"
        _, end_str = period.split("~")
        try:
            end_date = datetime.strptime(end_str.strip(), "%Y-%m-%d").date()
        except:
            return "Fattening"
        gap = (date.today() - end_date).days
        return "Milking" if gap <= 10 else "Dry"
    except Exception as e:
        st.error(f"compute_animal_category error: {e}")
        return "Fattening"

###############################################################################
# Usage & Reproduction Functions
###############################################################################
def get_top_sire_3y():
    try:
        three_years_ago = (date.today() - timedelta(days=3*365)).strftime("%Y-%m-%d")
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT bull_name, COUNT(*) FROM repro WHERE breeding_date >= ? GROUP BY bull_name ORDER BY COUNT(*) DESC LIMIT 1", (three_years_ago,))
        row = c.fetchone()
        conn.close()
        if row and row[0]:
            return f"{row[0]} ({row[1]} times)"
        else:
            return "-"
    except Exception as e:
        st.error(f"get_top_sire_3y error: {e}")
        return "-"

def get_top3_sire_usage_3y():
    try:
        three_years_ago = (date.today() - timedelta(days=3*365)).strftime("%Y-%m-%d")
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT bull_name, COUNT(*) as cnt FROM repro WHERE breeding_date >= ? GROUP BY bull_name ORDER BY cnt DESC LIMIT 3", (three_years_ago,))
        rows = c.fetchall()
        conn.close()
        if rows:
            return "\n".join([f"{row[0]}: {row[1]} times" for row in rows if row[0]])
        else:
            return "-"
    except Exception as e:
        st.error(f"get_top3_sire_usage_3y error: {e}")
        return "-"

def get_average_parity():
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            SELECT AVG(parity) FROM (
                SELECT r.parity
                FROM repro r
                JOIN animals a ON r.animal_id = a.id
                WHERE a.gender='F' AND r.parity IS NOT NULL
                GROUP BY a.id
            )
        """)
        row = c.fetchone()
        conn.close()
        if row and row[0]:
            return f"{row[0]:.1f}"
        else:
            return "-"
    except Exception as e:
        st.error(f"get_average_parity error: {e}")
        return "-"

def get_highest_parity_cow():
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            SELECT a.ear_tag, r.parity
            FROM repro r
            JOIN animals a ON r.animal_id = a.id
            WHERE a.gender='F' AND r.parity IS NOT NULL
            ORDER BY r.parity DESC
            LIMIT 1
        """)
        row = c.fetchone()
        conn.close()
        if row:
            return f"{row[0]} ({row[1]})"
        else:
            return "-"
    except Exception as e:
        st.error(f"get_highest_parity_cow error: {e}")
        return "-"

def get_frequent_abortion_cow():
    try:
        three_years_ago = (date.today() - timedelta(days=3*365)).strftime("%Y-%m-%d")
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            SELECT a.ear_tag, COUNT(*) as cnt
            FROM repro r
            JOIN animals a ON r.animal_id = a.id
            WHERE lower(r.delivery_status) = 'abortion' AND r.breeding_date >= ?
            GROUP BY a.id
            HAVING cnt >= 3
            ORDER BY cnt DESC
            LIMIT 1
        """, (three_years_ago,))
        row = c.fetchone()
        conn.close()
        if row:
            return f"{row[0]} ({row[1]})"
        else:
            return "-"
    except Exception as e:
        st.error(f"get_frequent_abortion_cow error: {e}")
        return "-"

###############################################################################
# 3개월 동적 계산 함수들
###############################################################################
def get_recent_births_count():
    """최근 3개월 동안 repro 테이블에서 delivery_status가 'Delivery'인 레코드 수"""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            SELECT COUNT(*) FROM repro 
            WHERE delivery_status = 'Delivery'
              AND breeding_date BETWEEN date('now', '-3 months') AND date('now')
        """)
        count = c.fetchone()[0]
        conn.close()
        return count
    except Exception as e:
        st.error(f"get_recent_births_count error: {e}")
        return 0

def get_recent_calf_tags():
    """최근 3개월 동안 delivery_status가 'Delivery'이고 calf_tag_number가 있는 레코드들의 calf_tag_number"""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            SELECT calf_tag_number FROM repro 
            WHERE delivery_status = 'Delivery'
              AND calf_tag_number IS NOT NULL
              AND calf_tag_number != ''
              AND breeding_date BETWEEN date('now', '-3 months') AND date('now')
        """)
        rows = c.fetchall()
        conn.close()
        return [row[0] for row in rows]
    except Exception as e:
        st.error(f"get_recent_calf_tags error: {e}")
        return []

def get_issue_count(status_value):
    """최근 3개월 동안 issue 테이블에서 event_status가 주어진 값(status_value)인 레코드 수"""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            SELECT COUNT(*) FROM issue 
            WHERE event_status = ?
              AND event_date BETWEEN datetime('now', '-3 months') AND datetime('now')
        """, (status_value,))
        count = c.fetchone()[0]
        conn.close()
        return count
    except Exception as e:
        st.error(f"get_issue_count ({status_value}) error: {e}")
        return 0

###############################################################################
# Graph Generation Functions
###############################################################################
def generate_breeding_diagnosis_bar_chart(farm, status):
    ear_tags = get_filtered_ear_tags(farm, status)
    if not ear_tags:
        return None
    diag_distribution = defaultdict(int)
    for tag in ear_tags:
        diag_distribution[compute_breeding_diagnosis(tag)] += 1
    if not diag_distribution:
        return None
    fig, ax = plt.subplots(figsize=(3, 3), dpi=350)
    cats = list(diag_distribution.keys())
    cnts = [diag_distribution[cat] for cat in cats]
    ax.bar(cats, cnts, color='skyblue')
    for i, v in enumerate(cnts):
        ax.text(i, v + 0.5, f"{v:,}", ha='center', va='bottom', fontsize=8)
    ax.set_ylabel("Count", fontsize=9, fontweight='bold')
    ax.set_title("Breeding Diagnosis", pad=10, fontsize=10, fontweight='bold')
    return fig

def generate_category_bar_chart(farm, status):
    ear_tags = get_filtered_ear_tags(farm, status)
    if not ear_tags:
        return None
    cat_distribution = defaultdict(int)
    for tag in ear_tags:
        cat_distribution[compute_animal_category(tag)] += 1
    if not cat_distribution:
        return None
    fig, ax = plt.subplots(figsize=(3, 3), dpi=350)
    cats = list(cat_distribution.keys())
    cnts = [cat_distribution[cat] for cat in cats]
    ax.bar(cats, cnts, color='lightgreen')
    for i, v in enumerate(cnts):
        ax.text(i, v + 0.5, f"{v:,}", ha='center', va='bottom', fontsize=8)
    ax.set_ylabel("Count", fontsize=9, fontweight='bold')
    ax.set_title("Animal Category", pad=10, fontsize=10, fontweight='bold')
    return fig

def plot_farm_milk_yield_trend_filtered(farm, status):
    ear_tags = get_filtered_ear_tags(farm, status)
    if not ear_tags:
        return None
    today = date.today()
    start_date = today - timedelta(days=365)
    start_date_str = start_date.strftime("%Y-%m-%d")
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        placeholders = ",".join("?" for _ in ear_tags)
        query = f"""
            SELECT strftime('%Y-%m', record_date) as month, SUM(yield_value)
              FROM milk_yield
             WHERE ear_tag IN ({placeholders}) AND record_date>=?
             GROUP BY month
             ORDER BY month
        """
        params = ear_tags + [start_date_str]
        c.execute(query, params)
        rows = c.fetchall()
        conn.close()
    except Exception as e:
        st.error(f"plot_farm_milk_yield_trend_filtered error: {e}")
        return None
    months = []
    values = []
    for i in range(12):
        month_date = today - timedelta(days=(11 - i) * 30)
        m_str = month_date.strftime("%Y-%m")
        months.append(m_str)
        values.append(0)
    data = {m: total for m, total in rows}
    for idx, m in enumerate(months):
        if m in data:
            values[idx] = data[m]
    import matplotlib.style as mplstyle
    if "seaborn-darkgrid" in mplstyle.available:
        mplstyle.use("seaborn-darkgrid")
    else:
        mplstyle.use("ggplot")
    fig, ax = plt.subplots(figsize=(7, 2), dpi=350, constrained_layout=True)
    ax.plot(months, values, marker='o', linestyle='-', color='#1E88E5', linewidth=2)
    ax.fill_between(months, values, alpha=0.3, color='#1E88E5')
    ax.set_xlabel("Month", fontsize=9)
    ax.set_ylabel("Cumulative Yield", fontsize=9)
    ax.set_title("Monthly Cumulative Yield (Past 12 Months)", fontsize=10, pad=10)
    base_year = months[0].split("-")[0]
    new_labels = [base_year] + [m.split("-")[1] for m in months[1:]]
    ax.set_xticks(months)
    ax.set_xticklabels(new_labels, rotation=45, ha="right", fontsize=9)
    plt.setp(ax.get_xticklabels(), fontstyle="normal")
    ax.tick_params(axis='y', labelsize=9)
    max_val = max(values) if values else 0
    ax.set_ylim(0, max_val + 2500)
    for x, y in zip(months, values):
        ax.annotate(f"{int(round(y)):,}", xy=(x, y), xytext=(0, 5),
                    textcoords="offset points", ha='center', fontsize=8)
    return fig

###############################################################################
# Streamlit UI 구성
###############################################################################
def main():
    st.title("Farm Report Streamlit")
    
    # 사이드바에 농장과 상태 선택
    st.sidebar.header("Select Farm and Status")
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT DISTINCT farm_name FROM animals WHERE farm_name != '' ORDER BY farm_name")
        farms = [row[0] for row in c.fetchall()]
        conn.close()
    except Exception as e:
        st.error(f"DB Error: {e}")
        farms = []
    
    selected_farm = st.sidebar.selectbox("Farm", ["Select Farm..."] + farms)
    status_options = ["owned", "Sell", "Dead", "Cull", "Total"]
    selected_status = st.sidebar.selectbox("Status", status_options)
    
    if selected_farm == "Select Farm...":
        st.info("Please select a farm from the sidebar.")
        return
    
    st.header(f"Farm: {selected_farm} (Status: {selected_status})")
    
    # General Info
    total_animals = get_total_animals_filtered(selected_farm, selected_status)
    heifer_count = get_heifer_count(selected_farm)
    bull_count = get_bull_count(selected_farm)
    
    st.subheader("General Info")
    st.write(f"**Total Animals:** {total_animals}")
    st.write(f"**Heifer Count:** {heifer_count}")
    st.write(f"**Bull Count:** {bull_count}")
    
    # Management (최근 3개월 동적 계산)
    births = get_recent_births_count()
    calf_tags = get_recent_calf_tags()
    cull_count = get_issue_count("Cull")
    sales = get_issue_count("Sell")
    deaths = get_issue_count("Dead")
    
    st.subheader("Management (Last 3 Months)")
    st.write(f"**Births:** {births} times")
    st.write(f"**Calf Tags:** {', '.join(calf_tags) if calf_tags else '-'}")
    st.write(f"**Cull Count:** {cull_count}")
    st.write(f"**Sales:** {sales}")
    st.write(f"**Deaths:** {deaths}")
    
    # Usage & Reproduction
    top_sire = get_top_sire_3y()
    top3_sire_usage = get_top3_sire_usage_3y()
    avg_parity = get_average_parity()
    highest_parity = get_highest_parity_cow()
    freq_abortion = get_frequent_abortion_cow()
    
    st.subheader("Usage & Reproduction")
    st.write(f"**Top Sire (3 Years):** {top_sire}")
    st.write(f"**Sire Usage Count (Top 3):** {top3_sire_usage}")
    st.write(f"**Average Parity:** {avg_parity}")
    st.write(f"**Highest Parity Cow:** {highest_parity}")
    st.write(f"**Frequent Abortion Cow:** {freq_abortion}")
    
    # Graphs
    st.subheader("Graphs")
    col1, col2 = st.columns(2)
    with col1:
        fig1 = generate_breeding_diagnosis_bar_chart(selected_farm, selected_status)
        if fig1:
            st.pyplot(fig1)
        else:
            st.write("No Breeding Diagnosis Data")
    with col2:
        fig2 = generate_category_bar_chart(selected_farm, selected_status)
        if fig2:
            st.pyplot(fig2)
        else:
            st.write("No Animal Category Data")
    
    fig3 = plot_farm_milk_yield_trend_filtered(selected_farm, selected_status)
    st.subheader("Milk Yield Trend (Past 12 Months)")
    if fig3:
        st.pyplot(fig3)
    else:
        st.write("No Milk Yield Trend Data")

if __name__ == "__main__":
    main()
