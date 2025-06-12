# app.py (Streamlit アプリ - 共通PW・フォーム修正・タイムアウト・管理者PW)
# -*- coding: utf-8 -*-

# === 1. ライブラリのインポート ===
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import datetime
from collections import defaultdict
import random
import os

# === Streamlit のページ設定 (一番最初に呼び出す) ===
st.set_page_config(page_title="バドミントン部 連絡システム", layout="centered", page_icon="shutlle.png") # アイコンを絵文字に修正

# === 2. 設定値 (st.secrets からも読み込む) ===
try:
    APP_PASSWORDS_SECRETS = st.secrets.get('app_passwords', {})
    GENERAL_PASSWORD_SECRET = APP_PASSWORDS_SECRETS.get("general_password")
    ADMIN_PASSWORD_SECRET = APP_PASSWORDS_SECRETS.get("admin_password")

    APP_CONFIG = st.secrets.get("app_config", {})
    DEBUG_MODE = APP_CONFIG.get("debug_mode", False)

    # 必須設定の確認
    if not GENERAL_PASSWORD_SECRET or not ADMIN_PASSWORD_SECRET:
        st.error("重大なエラー: secrets.toml の [app_passwords] に general_password または admin_password が設定されていません。")
        st.stop()
except Exception as e:
    st.error(f"重大なエラー: secrets.toml の読み込みまたは必須設定の取得中に問題が発生しました: {e}")
    DEBUG_MODE = False # フォールバック
    st.stop()

# --- サービスアカウント認証情報 (スプレッドシート操作用) ---
SCOPES_GSPREAD = ['https://www.googleapis.com/auth/sheets', 'https://www.googleapis.com/auth/drive']

# --- スプレッドシート情報 ---
SPREADSHEET_ID = '1jCCxSeECR7NZpCEXwZCDmW_NjcoEzBPg8wqM-IGyIS8' # ★あなたのスプレッドシートID
MEMBER_SHEET_NAME = '部員リスト'
ATTENDANCE_SHEET_NAME = '遅刻欠席連絡'
PARTICIPANT_LIST_SHEET_NAME = '参加者名簿'
ABSENT_LIST_SHEET_NAME = '欠席者名簿'
LATE_LIST_SHEET_NAME = '遅刻者名簿' # 新規追加: 遅刻者名簿シート名
ASSIGNMENT_SHEET_NAME_8 = '割り振り結果_8チーム'
ASSIGNMENT_SHEET_NAME_12 = '割り振り結果_12チーム'
ASSIGNMENT_SHEET_NAME_10 = '割り振り結果_10チーム' # 10チーム割り振り結果シート名
ASSIGNMENT_SHEET_NAME_3 = '割り振り結果_3チーム' # 新規追加: 3チーム割り振り結果シート名

# --- 列名 (ヘッダー名) ---
COL_MEMBER_ID = '学籍番号'; COL_MEMBER_NAME = '名前'; COL_MEMBER_GRADE = '学年';
COL_MEMBER_LEVEL = 'レベル'; COL_MEMBER_GENDER = '性別'; COL_MEMBER_DEPARTMENT = '学科'; # 新規追加: 学科
COL_ATTENDANCE_TIMESTAMP = '記録日時';
COL_ATTENDANCE_TARGET_DATE = '対象練習日';
COL_ATTENDANCE_STATUS = '状況';
COL_ATTENDANCE_LATE_TIME = '遅刻開始時刻';
COL_ATTENDANCE_REASON = '遅刻・欠席理由';
OUTPUT_COLUMNS_ORDER = ['記録日時', '対象練習日', '学籍番号', '学年', '名前', '状況', '遅刻・欠席理由', '遅刻開始時刻', '学科'] # 学科を追加
# 連絡確認フォームの表示用列 (学籍番号と遅刻・欠席理由を除外)
LOOKUP_DISPLAY_COLUMNS = ['記録日時', '対象練習日', '学年', '名前', '状況', '遅刻開始時刻'] # 学科を削除

# --- コート割り振り設定 ---
DEFAULT_PRACTICE_TYPE = 'ノック';
TEAMS_COUNT_MAP = {'ノック': 8, 'ハンドノック': 10, 'その他': 12}
INACTIVITY_TIMEOUT_MINUTES = 10

# === 3. 関数定義 ===
@st.cache_resource
def authenticate_gspread_service_account():
    """
    gspreadサービスアカウント認証を行います。
    Streamlit secretsまたはローカルのyour_credentials.jsonから認証情報を読み込みます。
    """
    if DEBUG_MODE: print("Attempting gspread Service Account Authentication...")
    try:
        if 'google_credentials' in st.secrets:
            # Streamlit secretsから辞書として直接サービスアカウント情報を読み込む
            creds_info = st.secrets['google_credentials']
            if DEBUG_MODE: print("Attempting gspread Service Account Authentication (from Secrets dict)...")
            client = gspread.service_account_from_dict(creds_info)
            if DEBUG_MODE: print(f"DEBUG: gspread Client Type (from Secrets): {type(client)}") # Debug print
            if DEBUG_MODE: print("gspread Service Account Authentication successful (from Secrets dict).")
            return client
        elif os.path.exists('your_credentials.json'):
            st.warning("警告: ローカルファイルから認証情報を読み込んでいます。本番環境ではSecretsを使用してください。")
            if DEBUG_MODE: print("Attempting gspread Service Account Authentication (from File).")
            client = gspread.service_account(filename='your_credentials.json')
            if DEBUG_MODE: print(f"DEBUG: gspread Client Type (from File): {type(client)}") # Debug print
            if DEBUG_MODE: print("gspread Service Account Authentication successful (from File).")
            return client
        else:
            st.error("認証エラー: Google Sheets 認証情報が見つかりません。Secretsに設定するか、your_credentials.jsonを配置してください。")
            print("ERROR: Google Sheets credentials not found.")
            return None
    except Exception as e:
        st.error(f"認証エラー(SA): {e}"); print(f"ERROR: SA Authentication error: {e}"); return None

def get_worksheet_safe(gspread_client, spreadsheet_id, sheet_name):
    """
    指定されたスプレッドシートからワークシートを安全に取得します。
    """
    if not gspread_client: 
        st.error(f"内部エラー: Google Sheetsクライアントが初期化されていません。")
        return None
    
    # gspread_clientがgspread.Clientインスタンスであることを明示的に確認
    if not isinstance(gspread_client, gspread.Client):
        st.error(f"内部エラー: Google Sheetsクライアントが不正な型です ({type(gspread_client)})。認証が失敗した可能性があります。")
        print(f"ERROR: Invalid gspread_client type: {type(gspread_client)}")
        return None

    if DEBUG_MODE: print(f"ワークシート '{sheet_name}' を取得中...")
    try:
        spreadsheet = gspread_client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(sheet_name)
        if DEBUG_MODE: print(f"-> '{sheet_name}' を取得しました。")
        return worksheet
    except Exception as e: st.error(f"ワークシート '{sheet_name}' 取得エラー: {e}"); print(f"Error getting worksheet '{sheet_name}': {e}"); return None

@st.cache_data(ttl=60)
def load_data_to_dataframe(_gspread_client, spreadsheet_id, sheet_name, required_cols=None):
    """
    スプレッドシートからデータをPandas DataFrameとして読み込みます。
    必要な列のチェックとデータ型の変換を行います。
    """
    if DEBUG_MODE: print(f"データを読み込みます: {sheet_name}")
    worksheet = get_worksheet_safe(_gspread_client, spreadsheet_id, sheet_name)
    if worksheet is None: return pd.DataFrame()
    try:
        data = worksheet.get_all_records(); df = pd.DataFrame(data)
        if DEBUG_MODE: print(f"-> {len(df)}件読み込み完了 ({sheet_name})")
        
        # 必須列のチェックを強化
        if required_cols:
            missing = [col for col in required_cols if col not in df.columns]
            if missing: 
                st.error(f"シート '{sheet_name}' に必要な列がありません: {missing}。スプレッドシートのヘッダーを確認してください。")
                print(f"ERROR: Missing required columns in sheet '{sheet_name}': {missing}"); 
                return pd.DataFrame()
        
        # データのクリーンアップと型変換
        if COL_MEMBER_ID in df.columns: df[COL_MEMBER_ID] = df[COL_MEMBER_ID].astype(str).str.strip()
        if COL_MEMBER_NAME in df.columns: df[COL_MEMBER_NAME] = df[COL_MEMBER_NAME].astype(str).str.strip()
        if COL_MEMBER_GRADE in df.columns: df[COL_MEMBER_GRADE] = df[COL_MEMBER_GRADE].astype(str).str.strip()
        # レベル列の数値変換、エラーはNaNとし、後で0で埋める
        if COL_MEMBER_LEVEL in df.columns: df[COL_MEMBER_LEVEL] = pd.to_numeric(df[COL_MEMBER_LEVEL], errors='coerce')
        if COL_MEMBER_GENDER in df.columns: df[COL_MEMBER_GENDER] = df[COL_MEMBER_GENDER].astype(str).str.strip()
        if COL_MEMBER_DEPARTMENT in df.columns: df[COL_MEMBER_DEPARTMENT] = df[COL_MEMBER_DEPARTMENT].astype(str).str.strip() # 新規追加

        if sheet_name == ATTENDANCE_SHEET_NAME:
            # タイムスタンプと対象練習日のdatetime変換
            if COL_ATTENDANCE_TIMESTAMP in df.columns: df['dt_timestamp'] = pd.to_datetime(df[COL_ATTENDANCE_TIMESTAMP], errors='coerce')
            if COL_ATTENDANCE_TARGET_DATE in df.columns: df['dt_target_date'] = pd.to_datetime(df[COL_ATTENDANCE_TARGET_DATE], errors='coerce').dt.date
        return df
    except Exception as e: st.error(f"データ読み込みエラー ({sheet_name}): {e}"); print(f"ERROR: Data loading error: {e}"); return pd.DataFrame()

def record_attendance_streamlit(worksheet, data_dict):
    """
    遅刻・欠席連絡をスプレッドシートに記録します。
    """
    if worksheet is None: st.error("記録用シートが見つかりません。"); return False
    try:
        row_data = [data_dict.get(col_name, "") for col_name in OUTPUT_COLUMNS_ORDER]
        worksheet.append_row(row_data, value_input_option='USER_ENTERED')
        if DEBUG_MODE: print(f"記録成功: {row_data}")
        return True
    except Exception as e: st.error(f"記録エラー: {e}"); print(f"ERROR: Error recording: {e}"); return False

def calculate_imbalance_score(male_count, female_count):
    """
    チームの男女比の偏りを数値で評価します。
    男性または女性のみのチーム、または人数が少ないチームでも機能するように設計されています。
    スコアが高いほど偏りが大きいことを示します。
    """
    if male_count == 0 and female_count == 0:
        return 0.0 # 空のチームは偏りなし
    if male_count == 0: # 女性のみのチーム
        return float(female_count) # 女性の数で偏りを評価
    if female_count == 0: # 男性のみのチーム
        return float(male_count) # 男性の数で偏りを評価
    # どちらも0でない場合、大きい方を小さい方で割ることで偏りを数値化
    # 割り算でゼロ除算を避けるためにminが0でないことを確認
    if min(male_count, female_count) == 0: # 片方が0でもう片方は0でない場合
        return max(male_count, female_count) * 1000.0 # 非常に高いペナルティ
    return max(male_count, female_count) / min(male_count, female_count)

def rebalance_teams_by_gender_and_level(teams, team_stats, late_member_ids, max_iterations=10): # Iterations increased for more attempts
    """
    チーム間の男女比、レベル、遅刻者数の偏りを、同レベル・同性別の部員を交換することで再調整します。
    チームの人数とレベル分布は維持されます。遅刻者は交換の対象外とします。
    """
    if DEBUG_MODE: print("\n性別・レベル・遅刻者均等化のためのチーム再調整を開始...")

    # For accurate statistics during rebalancing, re-calculate stats from current teams
    def update_stats_from_teams(current_teams, current_team_stats):
        for team_name in current_team_stats:
            # Reset all counts for recalculation
            for key in current_team_stats[team_name]:
                current_team_stats[team_name][key] = 0
            
        for team_name, members in current_teams.items():
            stats = current_team_stats[team_name]
            stats['count'] = len(members)
            for member in members:
                if member.get(COL_MEMBER_ID) in late_member_ids:
                    stats['late_count'] += 1
                if member.get(COL_MEMBER_GENDER) == '男性':
                    stats['male_count'] += 1
                else:
                    stats['female_count'] += 1
                level = member.get(COL_MEMBER_LEVEL)
                if pd.notna(level):
                    level = int(level)
                    if level == 6: stats['lv6_count'] += 1
                    elif level == 5: stats['lv5_count'] += 1
                    elif level == 4: stats['lv4_count'] += 1
                    elif level == 1: stats['lv1_count'] += 1
                    elif level in [2, 3]: stats['lv23_count'] += 1
                    elif level == 0: stats['lv0_count'] += 1
        return current_team_stats

    # Make a copy of team_stats to update it consistently during rebalancing
    current_team_stats = {k: v.copy() for k, v in team_stats.items()}

    for iteration in range(max_iterations):
        swapped_in_iteration = False
        team_names = list(teams.keys())
        random.shuffle(team_names)

        # Recalculate stats for current iteration to reflect previous swaps
        current_team_stats = update_stats_from_teams(teams, current_team_stats)
        
        # Determine average latecomers and standard deviation for robust imbalance check
        late_counts = {name: stats['late_count'] for name, stats in current_team_stats.items()}
        team_sizes = {name: stats['count'] for name, stats in current_team_stats.items()}
        
        if not late_counts: continue # No teams to rebalance

        avg_late = sum(late_counts.values()) / len(late_counts)
        
        # --- 1. 遅刻者数の均等化を最優先で試みる ---
        # Find teams with more latecomers than allowed max_diff (e.g., 1)
        max_late_count = max(late_counts.values())
        min_late_count = min(late_counts.values())

        if max_late_count - min_late_count > 1: # Only try to balance if difference is > 1
            high_late_teams = sorted([name for name, count in late_counts.items() if count == max_late_count], key=lambda k: late_counts[k], reverse=True)
            low_late_teams = sorted([name for name, count in late_counts.items() if count == min_late_count], key=lambda k: late_counts[k])
            
            for team_a_name in high_late_teams:
                for team_b_name in low_late_teams:
                    if team_a_name == team_b_name: continue
                    if team_sizes[team_a_name] < 1 or team_sizes[team_b_name] < 1: continue # Avoid empty teams

                    # team_a から遅刻者を探す
                    candidate_late_member = None
                    members_in_team_a = teams[team_a_name].copy() # Copy to iterate and modify original list
                    random.shuffle(members_in_team_a) 

                    for m_late in members_in_team_a:
                        if m_late.get(COL_MEMBER_ID) in late_member_ids: # team A から遅刻者
                            # team_b から非遅刻者を探す（同レベル・同性別）
                            candidate_non_late_member = None
                            members_in_team_b = teams[team_b_name].copy() # Copy
                            random.shuffle(members_in_team_b)

                            for m_non_late in members_in_team_b:
                                if m_non_late.get(COL_MEMBER_ID) not in late_member_ids and \
                                   pd.notna(m_late.get(COL_MEMBER_LEVEL)) and \
                                   pd.notna(m_non_late.get(COL_MEMBER_LEVEL)) and \
                                   int(m_late.get(COL_MEMBER_LEVEL, -1)) == int(m_non_late.get(COL_MEMBER_LEVEL, -1)) and \
                                   m_late.get(COL_MEMBER_GENDER) == m_non_late.get(COL_MEMBER_GENDER):
                                    # Ensure this swap improves overall late count balance
                                    # And doesn't drastically worsen other balances (gender, size)
                                    temp_teams_after_swap = {k: v[:] for k,v in teams.items()} # Deep copy
                                    temp_teams_after_swap[team_a_name].remove(m_late)
                                    temp_teams_after_swap[team_a_name].append(m_non_late)
                                    temp_teams_after_swap[team_b_name].remove(m_non_late)
                                    temp_teams_after_swap[team_b_name].append(m_late)
                                    
                                    temp_stats_after_swap = update_stats_from_teams(temp_teams_after_swap, {k: v.copy() for k,v in team_stats.items()})
                                    new_max_late = max(temp_stats_after_swap[n]['late_count'] for n in team_names)
                                    new_min_late = min(temp_stats_after_swap[n]['late_count'] for n in team_names)

                                    if new_max_late - new_min_late < (max_late_count - min_late_count):
                                        candidate_late_member = m_late
                                        candidate_non_late_member = m_non_late
                                        break
                            if candidate_late_member: break

                    if candidate_late_member and candidate_non_late_member:
                        # 実際に交換
                        teams[team_a_name].remove(candidate_late_member)
                        teams[team_a_name].append(candidate_non_late_member)
                        teams[team_b_name].remove(candidate_non_late_member)
                        teams[team_b_name].append(candidate_late_member)

                        # 統計を更新
                        current_team_stats = update_stats_from_teams(teams, current_team_stats)
                        
                        swapped_in_iteration = True
                        if DEBUG_MODE:
                            print(f"DEBUG: 遅刻者バランス調整 (Lv:{int(candidate_late_member.get(COL_MEMBER_LEVEL,-1))}, Gender:{candidate_late_member.get(COL_MEMBER_GENDER)}): {candidate_late_member.get(COL_MEMBER_NAME)} from {team_a_name} (late:{late_counts[team_a_name]}) swapped with {candidate_non_late_member.get(COL_MEMBER_NAME)} from {team_b_name} (late:{late_counts[team_b_name]}). New: {team_a_name} (late:{current_team_stats[team_a_name]['late_count']}), {team_b_name} (late:{current_team_stats[team_b_name]['late_count']}).")
                        break # Go to next iteration to re-evaluate all balances
                if swapped_in_iteration:
                    break # Break from outer loop (team_a_name), re-start iteration loop
        
        # --- 2. 性別・レベルの均等化を試みる (遅刻者数の差が1以下の場合、または遅刻者調整ができなかった場合) ---
        if not swapped_in_iteration: # Only proceed if no latecomer swaps were made in this iteration
            for team_a_name in team_names:
                team_a_stats = current_team_stats[team_a_name]

                if team_a_stats['count'] < 2:
                    continue

                current_imbalance_a = calculate_imbalance_score(team_a_stats['male_count'], team_a_stats['female_count'])

                if current_imbalance_a < 1.5: # Only rebalance if gender is significantly imbalanced
                    continue

                gender_to_swap_out_a = '男性' if team_a_stats['male_count'] > team_a_stats['female_count'] else '女性'
                gender_to_swap_in_a = '女性' if gender_to_swap_out_a == '男性' else '男性'

                member_a_candidate = None
                members_of_gender_to_swap_out_a = [m for m in teams[team_a_name] if m.get(COL_MEMBER_GENDER) == gender_to_swap_out_a and m.get(COL_MEMBER_ID) not in late_member_ids]
                if not members_of_gender_to_swap_out_a:
                    continue
                member_a_candidate = random.choice(members_of_gender_to_swap_out_a)
                level_a = member_a_candidate.get(COL_MEMBER_LEVEL)
                if pd.isna(level_a): continue
                level_a = int(level_a)

                for team_b_name in team_names:
                    if team_a_name == team_b_name: continue
                    team_b_stats = current_team_stats[team_b_name]

                    if team_b_stats['count'] < 2:
                        continue

                    member_b_candidate = None
                    members_of_gender_to_swap_in_a_from_b = [m for m in teams[team_b_name] if m.get(COL_MEMBER_GENDER) == gender_to_swap_in_a and int(m.get(COL_MEMBER_LEVEL, -1)) == level_a and m.get(COL_MEMBER_ID) not in late_member_ids]
                    if members_of_gender_to_swap_in_a_from_b:
                        member_b_candidate = random.choice(members_of_gender_to_swap_in_a_from_b)

                    if member_b_candidate:
                        # Simulate swap and check new imbalance scores
                        new_male_a = team_a_stats['male_count'] - (1 if gender_to_swap_out_a == '男性' else 0) + (1 if gender_to_swap_in_a == '男性' else 0)
                        new_female_a = team_a_stats['female_count'] - (1 if gender_to_swap_out_a == '女性' else 0) + (1 if gender_to_swap_in_a == '女性' else 0)
                        new_imbalance_a = calculate_imbalance_score(new_male_a, new_female_a)

                        new_male_b = team_b_stats['male_count'] - (1 if gender_to_swap_in_a == '男性' else 0) + (1 if gender_to_swap_out_a == '男性' else 0)
                        new_female_b = team_b_stats['female_count'] - (1 if gender_to_swap_in_a == '女性' else 0) + (1 if gender_to_swap_out_a == '女性' else 0)
                        new_imbalance_b = calculate_imbalance_score(new_male_b, new_female_b)
                        
                        # Only swap if it actually improves overall gender balance
                        if (new_imbalance_a < current_imbalance_a and new_imbalance_b < 1.5 * calculate_imbalance_score(team_b_stats['male_count'], team_b_stats['female_count'])) or \
                           (new_imbalance_a + new_imbalance_b < calculate_imbalance_score(team_a_stats['male_count'], team_a_stats['female_count']) + calculate_imbalance_score(team_b_stats['male_count'], team_b_stats['female_count'])):
                            
                            # Perform swap
                            teams[team_a_name].remove(member_a_candidate)
                            teams[team_a_name].append(member_b_candidate)
                            teams[team_b_name].remove(member_b_candidate)
                            teams[team_b_name].append(member_a_candidate)

                            # Update stats
                            current_team_stats = update_stats_from_teams(teams, current_team_stats)
                            
                            swapped_in_iteration = True
                            if DEBUG_MODE:
                                print(f"DEBUG: 性別/レベル調整: {member_a_candidate.get(COL_MEMBER_NAME)} (L{level_a}, {gender_to_swap_out_a}) を {team_a_name} から "
                                      f"{member_b_candidate.get(COL_MEMBER_NAME)} (L{level_a}, {gender_to_swap_in_a}) を {team_b_name} と交換しました。")
                                print(f"DEBUG: {team_a_name} の統計: {current_team_stats[team_a_name]['male_count']}M/{current_team_stats[team_a_name]['female_count']}F (新偏り: {new_imbalance_a:.2f})")
                                print(f"DEBUG: {team_b_name} の統計: {current_team_stats[team_b_name]['male_count']}M/{current_team_stats[team_b_name]['female_count']}F (新偏り: {new_imbalance_b:.2f})")
                            break # Break from inner loop (team_b_name), re-evaluate team_names in next outer loop
                if swapped_in_iteration:
                    break # Break from outer loop (team_a_name), re-start iteration loop
        
        if not swapped_in_iteration:
            # If no swaps were made in this entire iteration (neither latecomer nor gender/level), stop rebalancing
            if DEBUG_MODE: print(f"DEBUG: イテレーション {iteration+1} で交換が行われなかったため、再調整を停止します。")
            break

    if DEBUG_MODE: print("性別・レベル・遅刻者均等化のためのチーム再調整が完了しました。")
    return teams

def assign_teams(members_pool_df, late_member_ids, num_teams, assignment_type="general"):
    """
    レベル、遅刻者、性別の均等性を考慮した改善版割り振り関数。
    割り振り手順：
    1. 全参加者を「通常参加者」と「遅刻者」に分ける。
    2. 通常参加者をレベル順に、遅刻者をレベル順に割り振る。
    3. 各部員を割り振る際、チームの現在の状態に基づいて最適なチームをスコアリングで決定する。
    4. 最終的な性別・レベルの偏りを再調整する（遅刻者は動かさない）。
    """
    if DEBUG_MODE: print(f"\nコート割り振り開始 ({assignment_type} - {num_teams}チーム)... 参加者 {len(members_pool_df)} 名")
    if members_pool_df.empty:
        if DEBUG_MODE: print("参加者がいないため、割り振りできません。")
        return {}

    required_cols = [COL_MEMBER_ID, COL_MEMBER_NAME, COL_MEMBER_GRADE, COL_MEMBER_LEVEL, COL_MEMBER_GENDER]
    missing_cols = [col for col in required_cols if col not in members_pool_df.columns]
    if missing_cols:
        st.error(f"エラー: 部員リストに必要な列が見つかりません: {missing_cols}")
        print(f"ERROR: Missing required columns in member list: {missing_cols}")
        return {}

    total_members = len(members_pool_df)
    actual_num_teams = min(num_teams, total_members)
    if actual_num_teams <= 0:
        if DEBUG_MODE: print("割り当て可能なチーム数が0です。"); return {}
    if actual_num_teams != num_teams:
        print(f"参加者数 ({total_members}名) に基づき、チーム数を {actual_num_teams} に調整。")
        if actual_num_teams == 0: return {} # 調整の結果チーム数が0になった場合

    # 参加者全体の男女比
    total_male_present = len(members_pool_df[members_pool_df[COL_MEMBER_GENDER] == '男性'])
    total_present_members = len(members_pool_df)
    target_male_ratio_total = total_male_present / total_present_members if total_present_members > 0 else 0.5
    if DEBUG_MODE: print(f"参加者全体の男性比率: {target_male_ratio_total:.2f}")

    # 各レベルの総数を計算（偏りスコア計算用）
    # NaNを-1として扱うことで、to_numericが失敗してもint()に変換できるようになる
    members_pool_df[COL_MEMBER_LEVEL] = pd.to_numeric(members_pool_df[COL_MEMBER_LEVEL], errors='coerce').fillna(-1).astype(int)

    total_lv6 = len(members_pool_df[members_pool_df[COL_MEMBER_LEVEL] == 6])
    total_lv5 = len(members_pool_df[members_pool_df[COL_MEMBER_LEVEL] == 5])
    total_lv4 = len(members_pool_df[members_pool_df[COL_MEMBER_LEVEL] == 4])
    total_lv1 = len(members_pool_df[members_pool_df[COL_MEMBER_LEVEL] == 1])
    total_lv23 = len(members_pool_df[members_pool_df[COL_MEMBER_LEVEL].isin([2, 3])])
    total_lv0 = len(members_pool_df[members_pool_df[COL_MEMBER_LEVEL] == 0])
    total_late = len(late_member_ids)

    teams = defaultdict(list)
    # team_statsを初期化
    team_stats = {f"チーム {i+1}": {
        'count': 0, 'lv6_count': 0, 'lv5_count': 0, 'lv4_count': 0,
        'lv1_count': 0, 'lv23_count': 0, 'lv0_count': 0,
        'male_count': 0, 'female_count': 0, 'late_count': 0
    } for i in range(actual_num_teams)}

    # Helper function to assign a member and update stats
    def assign_single_member_to_team(member_dict, target_team_name, is_late_member=False):
        teams[target_team_name].append(member_dict)
        stats = team_stats[target_team_name]
        stats['count'] += 1
        level = member_dict.get(COL_MEMBER_LEVEL)
        if pd.notna(level):
            level = int(level)
            if level == 6: stats['lv6_count'] += 1
            elif level == 5: stats['lv5_count'] += 1
            elif level == 4: stats['lv4_count'] += 1
            elif level == 1: stats['lv1_count'] += 1
            elif level in [2, 3]: stats['lv23_count'] += 1
            elif level == 0: stats['lv0_count'] += 1
        if member_dict.get(COL_MEMBER_GENDER) == '男性':
            stats['male_count'] += 1
        else:
            stats['female_count'] += 1
        if is_late_member:
            stats['late_count'] += 1

    all_members_data = members_pool_df.to_dict('records')

    # Separate members by their status (late/regular)
    late_members_categorized = [m for m in all_members_data if m.get(COL_MEMBER_ID) in late_member_ids]
    regular_members_categorized = [m for m in all_members_data if m.get(COL_MEMBER_ID) not in late_member_ids]

    # Define the order of levels to process for initial assignment
    # Process higher impact levels first.
    level_processing_order = [6, 5, 4, 1, 3, 2, 0] # Order of levels for assignment

    # --- 割り振り実行 (レベル順に部員を処理し、最適なチームに割り振る) ---

    # まず、通常参加者をレベル順に割り振る
    for level_to_process in level_processing_order:
        members_at_this_level = [m for m in regular_members_categorized if pd.notna(m.get(COL_MEMBER_LEVEL)) and int(m.get(COL_MEMBER_LEVEL)) == level_to_process]
        random.shuffle(members_at_this_level) # Shuffle to add randomness and break ties for better distribution
        for member_data in members_at_this_level:
            is_male = (member_data.get(COL_MEMBER_GENDER) == '男性')
            member_level = member_data.get(COL_MEMBER_LEVEL) 

            team_candidate_scores = []
            for team_name in team_stats.keys():
                stats = team_stats[team_name]

                # Scoring components (lower score is better)
                score_current_size = stats['count'] # Smaller team size is preferred to balance counts
                
                # Gender balance (deviation from overall target ratio)
                predicted_team_size = stats['count'] + 1
                predicted_male_count = stats['male_count'] + (1 if is_male else 0)
                predicted_female_count = stats['female_count'] + (1 if not is_male else 0)
                score_gender_imbalance = calculate_imbalance_score(predicted_male_count, predicted_female_count)

                # Combine scores into a tuple for prioritization. Lower values are better.
                # Request 1: レベル6をまず各コートの人数ができるだけ均等になるように割り振る。
                # Request 2: 各コートのレベル5の人数をレベル6の人数と合わせた人数ができるだけ均等になるように配置する。
                # Request 3: レベル4同士がバラバラになるように配置する。配置先はチームの人数が少ないところから埋める。
                # Request 4: レベル1も同様に配置する。
                # Request 7: 最後に通常参加のレベル2、3をコートの人数差が1に収まるように割り振る。

                if level_to_process == 6:
                    combined_score = (
                        stats['lv6_count'],             # Primary: Minimize Lv6 count in team (to ensure all teams get one first)
                        score_current_size,             # Secondary: Balance overall team size
                        score_gender_imbalance          # Tertiary: Balance gender
                    )
                elif level_to_process == 5:
                    # Lv6とLv5の合計が均等になるように
                    combined_lv6_lv5_in_team = stats['lv6_count'] + stats['lv5_count']
                    # Aim to make combined Lv6+Lv5 count as even as possible across teams
                    # Use a very high penalty if it would create an extreme imbalance
                    combined_score = (
                        combined_lv6_lv5_in_team,       # Primary: Minimize sum of Lv6+Lv5
                        stats['lv5_count'],             # Secondary: Minimize Lv5 count specifically
                        score_current_size,             # Tertiary: Balance overall team size
                        score_gender_imbalance          # Quaternary: Balance gender
                    )
                elif level_to_process in [4, 1]:
                    combined_score = (
                        stats.get(f'lv{int(member_level)}_count', 0), # Primary: Minimize count of this specific level (to spread them out)
                        score_current_size,             # Secondary: Balance overall team size
                        score_gender_imbalance
                    )
                elif level_to_process in [3, 2, 0]: # 通常参加のLv2,3,0
                    combined_score = (
                        score_current_size,             # Primary: Balance overall team size (to ensure team count diff is 1)
                        score_gender_imbalance,         # Secondary: Balance gender
                        stats.get(f'lv{int(member_level)}_count', 0) # Tertiary: Balance this specific level
                    )
                else: # Fallback, should not happen with current level_processing_order
                    combined_score = (score_current_size, score_gender_imbalance)

                team_candidate_scores.append((combined_score, team_name))
            
            team_candidate_scores.sort() # Sort by the tuple score (Python sorts tuples element-wise)
            target_team_name = team_candidate_scores[0][1] # Select the team with the lowest score
            assign_single_member_to_team(member_data, target_team_name, is_late_member=False)
            if DEBUG_MODE: print(f"-> 通常: {member_data.get(COL_MEMBER_NAME, '?')} (L{int(member_data.get(COL_MEMBER_LEVEL, 0)) if pd.notna(member_data.get(COL_MEMBER_LEVEL)) else '?'}, {member_data.get(COL_MEMBER_GENDER, '?')}) を {target_team_name} に割り振り。")


    # 次に、遅刻者をレベル順に割り振る
    for level_to_process in level_processing_order:
        members_at_this_level = [m for m in late_members_categorized if pd.notna(m.get(COL_MEMBER_LEVEL)) and int(m.get(COL_MEMBER_LEVEL)) == level_to_process]
        random.shuffle(members_at_this_level)
        for member_data in members_at_this_level:
            is_male = (member_data.get(COL_MEMBER_GENDER) == '男性')
            member_level = member_data.get(COL_MEMBER_LEVEL)

            team_candidate_scores = []
            for team_name in team_stats.keys():
                stats = team_stats[team_name]
                
                score_current_size = stats['count'] # チームの現在の人数
                score_gender_imbalance = calculate_imbalance_score(
                    stats['male_count'] + (1 if is_male else 0),
                    stats['female_count'] + (1 if not is_male else 0)
                )
                score_late_count_imbalance = stats['late_count'] # 遅刻者全体の均等性

                combined_score = (0, 0, 0, 0) # Default, will be overwritten

                if level_to_process == 6: # 遅刻者のLv6
                    # 最優先：当該Lv6の人数が少ないところに配置
                    # 次点：遅刻者全体の均等性
                    # 次点：チームの人数
                    combined_score = (
                        stats['lv6_count'],             # Primary: Minimize Lv6 count in team
                        score_late_count_imbalance,     # Secondary: Balance overall latecomers
                        score_current_size,
                        score_gender_imbalance
                    )
                elif level_to_process == 5: # 遅刻者のLv5
                    # 通常参加者と同様に、チーム全体のLv6とLv5の合計が均等になるように配置
                    combined_lv6_lv5_in_team = stats['lv6_count'] + stats['lv5_count']
                    combined_score = (
                        combined_lv6_lv5_in_team,       # 1. チーム全体のLv6+Lv5の合計が少ない
                        score_late_count_imbalance,     # 2. 遅刻者の人数が少ない
                        stats['lv5_count'],             # 3. チーム全体のLv5の人数が少ない
                        score_current_size,             # 4. 全体の人数が少ない
                        score_gender_imbalance          # 5. 性別バランスが良い
                    )
                elif level_to_process in [4, 1]: # 遅刻者のLv4, Lv1
                    # 最優先：当該レベルの人数が少ないところに
                    # 次点：遅刻者全体の均等性
                    # 次点：チームの人数
                    combined_score = (
                        stats.get(f'lv{int(member_level)}_count', 0), # Primary: Minimize count of this specific level
                        score_late_count_imbalance,     # Secondary: Balance overall latecomers
                        score_current_size,
                        score_gender_imbalance
                    )
                elif level_to_process in [3, 2, 0]: # 遅刻者のLv2, Lv3, Lv0
                    # 最優先：遅刻者が各コートで均等に割り振られるようにする
                    # 次点：チームの人数も均等に
                    # 次点：男女比の偏りが少ないチーム
                    combined_score = (
                        score_late_count_imbalance,    # Primary: Balance overall latecomers
                        score_current_size,            # Secondary: Balance overall team size
                        score_gender_imbalance
                    )
                else: # Fallback
                    combined_score = (score_late_count_imbalance, score_current_size, score_gender_imbalance)

                team_candidate_scores.append((combined_score, team_name))
            
            team_candidate_scores.sort()
            target_team_name = team_candidate_scores[0][1]
            assign_single_member_to_team(member_data, target_team_name, is_late_member=True)
            if DEBUG_MODE: print(f"-> 遅刻: {member_data.get(COL_MEMBER_NAME, '?')} (L{int(member_data.get(COL_MEMBER_LEVEL, 0)) if pd.notna(member_data.get(COL_MEMBER_LEVEL)) else '?'}, {member_data.get(COL_MEMBER_GENDER, '?')}) を {target_team_name} に割り振り。")

    if DEBUG_MODE: print("\n一次割り振りループ完了。")

    # 最終的なバランス調整 (性別・レベルの偏りをさらに調整、遅刻者は動かさない)
    # Request 8: 最後に男女比調整のために交換を実施する。
    teams = rebalance_teams_by_gender_and_level(teams, team_stats, late_member_ids)

    if DEBUG_MODE:
        # 正確なデバッグ出力のために、最終的なチーム構成から統計を再計算する
        print("\n最終的なチーム統計を再計算中...")
        for team_name in team_stats:
            for key in team_stats[team_name]:
                team_stats[team_name][key] = 0 # Reset stats for recalculation
        for team_name, members in teams.items():
            stats = team_stats[team_name]
            stats['count'] = len(members)
            for member in members:
                if member.get(COL_MEMBER_ID) in late_member_ids:
                    stats['late_count'] += 1
                if member.get(COL_MEMBER_GENDER) == '男性':
                    stats['male_count'] += 1
                else:
                    stats['female_count'] += 1
                level = member.get(COL_MEMBER_LEVEL)
                if pd.notna(level):
                    level = int(level)
                    if level == 6: stats['lv6_count'] += 1
                    elif level == 5: stats['lv5_count'] += 1
                    elif level == 4: stats['lv4_count'] += 1
                    elif level == 1: stats['lv1_count'] += 1
                    elif level in [2, 3]: stats['lv23_count'] += 1
                    elif level == 0: stats['lv0_count'] += 1

        print(f"\n--- チーム割り振り最終結果 ({assignment_type} - {num_teams}チーム) ---")
        total_assigned = 0
        for team_name in sorted(teams.keys(), key=lambda name: int(name.split()[-1])):
            members_in_team = teams[team_name]
            total_assigned += len(members_in_team)
            member_names = [f"{m.get(COL_MEMBER_NAME, '?')} (L{int(m.get(COL_MEMBER_LEVEL, 0)) if pd.notna(m.get(COL_MEMBER_LEVEL)) else '?'})" for m in members_in_team]
            stats = team_stats[team_name]
            num_lv6 = stats['lv6_count']
            num_lv5 = stats['lv5_count']
            num_lv4 = stats['lv4_count']
            num_lv1 = stats['lv1_count']
            num_male = stats['male_count']
            num_female = stats['female_count']
            num_late = stats['late_count']
            num_lv23 = stats.get('lv23_count', 0)
            num_lv0 = stats.get('lv0_count', 0)
            print(f" {team_name} ({len(members_in_team)}名, Lv6:{num_lv6}, Lv5:{num_lv5}, Lv4:{num_lv4}, Lv1:{num_lv1}, Lv2/3:{num_lv23}, Lv0:{num_lv0}, 男:{num_male}, 女:{num_female}, 遅刻:{num_late}): {', '.join(member_names)}")
        print("---------------------------------")
        expected_count_for_debug = len(members_pool_df)
        print(f"合計割り当て人数: {total_assigned} (期待値: {expected_count_for_debug})")
        if total_assigned != expected_count_for_debug:
            print(f"警告: 割り当て人数が期待値と異なります。")

    return dict(teams)

def format_assignment_results(assignments, practice_type_or_teams, target_date):
    """
    割り振り結果をスプレッドシート書き込み用に整形します。
    """
    if DEBUG_MODE: print(f"\n割り振り結果 ({practice_type_or_teams} - {target_date.strftime('%Y-%m-%d')}) を整形中...")
    if not assignments: return [[f"割り振り結果なし ({practice_type_or_teams} - {target_date.strftime('%Y-%m-%d')})"]]
    
    output_rows = []
    output_rows.append([f"コート割り振り結果 ({practice_type_or_teams} - {target_date.strftime('%Y-%m-%d')})"])
    output_rows.append([]) # 空行
    
    # チーム名をソートしてヘッダーに追加
    team_names = sorted(assignments.keys(), key=lambda name: int(name.split()[-1]))
    output_rows.append(team_names)
    
    # 最大のチーム人数を取得し、行数を決定
    max_len = max(len(m) for m in assignments.values()) if assignments else 0
    
    # 各行に部員名を追加
    for i in range(max_len):
        row = []
        for team_name in team_names:
            members = assignments.get(team_name, [])
            cell_value = ""
            if i < len(members):
                member = members[i]
                name = member.get(COL_MEMBER_NAME, '?')
                level_val = member.get(COL_MEMBER_LEVEL, '?')
                level_display = int(level_val) if pd.notna(level_val) else '?'
                gender = member.get(COL_MEMBER_GENDER, '?')
                cell_value = f"{name} (L{level_display}/{gender})"
            else:
                cell_value = "" # そのチームに部員がいなければ空文字列
            row.append(cell_value)
        if DEBUG_MODE and i < 2 : print(f"DEBUG: Completed Row {i+1} for format: {row}")
        output_rows.append(row)
    
    if DEBUG_MODE: print("-> 整形完了")
    return output_rows

def write_results_to_sheet(worksheet, result_data, data_name="データ"):
    """
    整形された割り振り結果データをスプレッドシートに書き込みます。
    既存の内容はクリアされます。
    """
    if worksheet is None: st.error(f"エラー: {data_name}の出力用シートが見つかりません。"); return False
    if not result_data: st.warning(f"書き込む{data_name}がありません。"); return False
    if DEBUG_MODE: print(f"{data_name}書き込み中: '{worksheet.title}' ...")
    try:
        worksheet.clear(); worksheet.update(range_name='A1', values=result_data, value_input_option='USER_ENTERED')
        if DEBUG_MODE: print(f"-> {data_name}書き込み完了")
        st.success(f"{data_name}をシート '{worksheet.title}' に書き込みました。")
        return True
    except Exception as e: st.error(f"{data_name}のシートへの書き込み中にエラー: {e}"); print(f"ERROR: Error writing {data_name}: {e}"); return False

# === 4. Streamlit アプリ本体の開始 ===
st.title("🏸 バドミントン部 連絡システム")

# --- セッション状態の初期化 (アプリデータ用) ---
if 'member_df' not in st.session_state: st.session_state.member_df = pd.DataFrame()
if 'name_to_id_map_form' not in st.session_state: st.session_state.name_to_id_map_form = {}
if 'grade_options' not in st.session_state: st.session_state.grade_options = ["---"]
if 'department_options' not in st.session_state: st.session_state.department_options = ["---"] # 新規追加
if 'form_member_options' not in st.session_state: st.session_state.form_member_options = ["---"]
if 'show_success_message' not in st.session_state:
    st.session_state.show_success_message = False
if 'success_message_content' not in st.session_state:
    st.session_state.success_success_message_content = ""
# 名前カスタムマルチセレクトの初期化は不要なため削除
# if 'selected_names_form_custom_key' not in st.session_state:
#     st.session_state.selected_names_form_custom_key = []

# ログイン状態を管理するセッション変数 (共通パスワード方式)
if 'authentication_status' not in st.session_state:
    st.session_state.authentication_status = None
if 'user_name' not in st.session_state:
    st.session_state.user_name = None
if 'is_admin' not in st.session_state:
    st.session_state.is_admin = False
if 'last_interaction_time' not in st.session_state:
    st.session_state.last_interaction_time = datetime.datetime.now()

# --- 一般ログイン処理 ---
def check_general_password():
    """共通パスワードをチェックし、認証状態を更新します。"""
    if st.session_state.general_password_input == GENERAL_PASSWORD_SECRET:
        st.session_state.authentication_status = True
        st.session_state.user_name = "部員"
        st.session_state.last_interaction_time = datetime.datetime.now()
    else:
        st.error("共通パスワードが間違っています。")
        st.session_state.authentication_status = False

# --- 管理者ログイン処理 ---
def check_admin_password():
    """管理者パスワードをチェックし、管理者フラグを更新します。"""
    if st.session_state.admin_password_input_key == ADMIN_PASSWORD_SECRET:
        st.session_state.is_admin = True
        st.session_state.last_interaction_time = datetime.datetime.now()
        st.success("管理者として認証されました。")
    else:
        st.error("管理者パスワードが間違っています。")
        st.session_state.is_admin = False

# --- ログインフォームの表示 ---
if st.session_state.authentication_status is not True:
    st.subheader("アプリ利用のための共通パスワードを入力してください")
    st.text_input("共通パスワード", type="password", key="general_password_input", on_change=check_general_password)
    st.stop()

# --- メインコンテンツ (一般ログイン済みユーザー向け) ---
# 自動ログアウトチェック
if datetime.datetime.now() - st.session_state.last_interaction_time > datetime.timedelta(minutes=INACTIVITY_TIMEOUT_MINUTES):
    st.warning(f"{INACTIVITY_TIMEOUT_MINUTES}分間操作がなかったため、自動的にログアウトしました。再度ログインしてください。")
    # 関連するセッションステートをクリアして再ログインを促す
    keys_to_clear = ['authentication_status', 'user_name', 'is_admin', 'last_interaction_time',
                    'form_grade_select_key', # form_grade_select_keyは引き続き利用
                    'form_name_select_key', # form_name_select_keyは引き続き利用
                    'form_status_key_outside_form', 
                    'form_reason_input_key', 
                    'form_late_time_input_key', 
                    'form_target_date_key', # form_target_date_keyは引き続き利用
                    'name_to_id_map_form', 'form_member_options',
                    'show_success_message', 'success_message_content',
                    # 'selected_names_form_custom_key', # 削除されたカスタムキーなのでクリアリストから削除
                    'lookup_member_options', 'name_to_id_map_lookup', 
                    'lookup_grade_select_key', 'lookup_department_select_key', 'lookup_name_select_key', 
                    'admin_password_input_key' 
                    ] 
    for key in list(st.session_state.keys()):
        if key in st.session_state: 
            del st.session_state[key]
    st.rerun() 
    st.stop()

st.session_state.last_interaction_time = datetime.datetime.now()

# アプリに表示される現在時刻をJSTに修正
now_display_jst = datetime.datetime.now() + datetime.timedelta(hours=9)
st.caption(f"現在時刻: {now_display_jst.strftime('%Y-%m-%d %H:%M:%S')}")

gspread_client = authenticate_gspread_service_account()
if not gspread_client:
    st.error("スプレッドシートサービスへの接続に失敗しました。")
    st.stop()

# 部員データの読み込みと初期設定
if st.session_state.member_df.empty:
    required_member_cols_all = [COL_MEMBER_ID, COL_MEMBER_NAME, COL_MEMBER_GRADE, COL_MEMBER_LEVEL, COL_MEMBER_GENDER, COL_MEMBER_DEPARTMENT] # 学科も必須に
    st.session_state.member_df = load_data_to_dataframe(gspread_client, SPREADSHEET_ID, MEMBER_SHEET_NAME, required_cols=required_member_cols_all)
    if not st.session_state.member_df.empty:
        try:
            unique_grades_raw = st.session_state.member_df[COL_MEMBER_GRADE].astype(str).str.strip().unique()
            st.session_state.grade_options = ["---"] + sorted([g for g in unique_grades_raw if g])
            unique_depts_raw = st.session_state.member_df[COL_MEMBER_DEPARTMENT].astype(str).str.strip().unique() # 新規追加
            st.session_state.department_options = ["---"] + sorted([d for d in unique_depts_raw if d]) # 新規追加
        except KeyError as e: st.error(f"エラー: 部員リストに必須列がありません: {e}"); st.session_state.grade_options = ["---"]; st.session_state.department_options = ["---"] # フォールバック
    else:
        st.session_state.grade_options = ["---"]
        st.session_state.department_options = ["---"] # フォールバック
        st.warning("部員データが空か、読み込みに失敗しました。")

# --- 遅刻・欠席連絡フォーム ---
st.header("１．遅刻・欠席連絡")
if not st.session_state.member_df.empty:
    member_df_for_form = st.session_state.member_df

    # 変更: callback関数を引数で呼び出せるようにする (学年と学科でフィルタリング)
    def update_name_options_for_form_callback_internal(grade, department):
        name_options = []
        id_map = {}
        filtered_df = member_df_for_form.copy()

        if grade != "---":
            filtered_df = filtered_df[filtered_df[COL_MEMBER_GRADE].astype(str).str.strip() == str(grade).strip()]
        if department != "---": # 新規追加
            filtered_df = filtered_df[filtered_df[COL_MEMBER_DEPARTMENT].astype(str).str.strip() == str(department).strip()] # 新規追加

        if not filtered_df.empty:
            # 変更: 部員リストの順序を保持
            name_options = filtered_df[COL_MEMBER_NAME].tolist()
            id_map = pd.Series(filtered_df[COL_MEMBER_ID].values, index=filtered_df[COL_MEMBER_NAME]).to_dict()
        st.session_state.form_member_options = name_options
        st.session_state.name_to_id_map_form = id_map

    # UIのコールバックハンドラー
    # これらのコールバックは st.form の外側にあるウィジェット用
    def handle_form_grade_change():
        # 名前リストのオプションを更新
        update_name_options_for_form_callback_internal(st.session_state.form_grade_select_key, st.session_state.form_department_select_key)
        # 学年変更時に名前の選択を「---」に戻す
        st.session_state.form_name_select_key = "---"

    def handle_form_department_change():
        # 名前リストのオプションを更新
        update_name_options_for_form_callback_internal(st.session_state.form_grade_select_key, st.session_state.form_department_select_key)
        # 学科変更時に名前の選択を「---」に戻す
        st.session_state.form_name_select_key = "---"

    # フォーム外のウィジェット
    target_date_form = st.date_input("対象の練習日:", value=st.session_state.get('form_target_date_key', datetime.date.today()), min_value=datetime.date.today(), key="form_target_date_key")
    col_grade, col_department = st.columns(2)
    with col_grade:
        selected_grade_form = st.selectbox(
            "あなたの学年:",
            st.session_state.get('grade_options', ["---"]),
            key="form_grade_select_key",
            on_change=handle_form_grade_change
        )
    with col_department:
        selected_department_form = st.selectbox(
            "あなたの学科:",
            st.session_state.get('department_options', ["---"]),
            key="form_department_select_key",
            on_change=handle_form_department_change
        )

    # フォームレンダリング時に名前のオプションを更新 (初期表示と学年・学科変更時)
    current_grade_for_names = st.session_state.get('form_grade_select_key', selected_grade_form)
    current_department_for_names = st.session_state.get('form_department_select_key', selected_department_form)
    update_name_options_for_form_callback_internal(current_grade_for_names, current_department_for_names)

    # --- 名前選択をst.selectbox (単一選択)に変更 ---
    selected_name_display_form = st.selectbox(
        f"あなたの名前 ({selected_grade_form if selected_grade_form != '---' else '学年未選択'}"
        f"{' / ' + selected_department_form if selected_department_form != '---' else ''}):", # 学科表示も追加
        options=["---"] + st.session_state.get('form_member_options', []), # 単一選択なので先頭に「---」を追加
        key="form_name_select_key" 
    )


    # --- 状態選択 (st.form の外に移動) ---
    selected_status_form = st.radio(
        "状態:", 
        ["欠席", "遅刻", "参加"], 
        horizontal=True, 
        key="form_status_key_outside_form", # キー名を変更し、セッションステートで管理
        index=0 # デフォルトは「欠席」
    )

    # --- 遅刻開始時刻入力フィールド (状態選択に連動) ---
    late_time_form_val = ""
    if selected_status_form == "遅刻":
        late_time_form_val = st.text_input(
            "参加可能時刻 (例: 17:30):", 
            value=st.session_state.get("form_late_time_input_key", ""), # 以前の値を保持
            key="form_late_time_input_key" # 新しいキー
        )
    else:
        # 「遅刻」以外が選択された場合、遅刻時刻のセッションステートをクリア
        if "form_late_time_input_key" in st.session_state:
            del st.session_state["form_late_time_input_key"]
        late_time_form_val = "" # 値もクリア

    # 伝達事項を記入する欄とし、必須ではない項目とする
    reason_label = "伝達事項 (任意):"
    reason_placeholder = ""
    if selected_status_form in ["欠席", "遅刻"]:
        reason_label = "理由 (必須):"
        reason_placeholder = "例: 授業のため、実習のため"

    reason_form = st.text_area(
        reason_label,
        placeholder=reason_placeholder,
        value=st.session_state.get("form_reason_input_key", ""), # 以前の値を保持
        key="form_reason_input_key"
    )

    # --- 送信ボタンの動的なテキスト決定 ---
    submit_button_text = "連絡内容を送信する"
    if selected_name_display_form == "---":
        grade_dept_info = ""
        if selected_grade_form != "---": grade_dept_info += f"{selected_grade_form}"
        if selected_department_form != "---": grade_dept_info += f"{selected_department_form}"
        
        if grade_dept_info:
            submit_button_text = f"{grade_dept_info}全員の連絡を送信する"
        else:
            submit_button_text = "学年、学科、または名前を選択してください" # 学年・学科も名前も「---」の場合、送信不可を促す

    else: # 特定の名前が選択されている場合
        submit_button_text = f"{selected_name_display_form}さんの連絡を送信する"

    # st.form で囲むことで、送信時に自動でクリアされるようにする (伝達事項のみ)
    with st.form(key="attendance_form"): # clear_on_on_submit=True を削除して他のフィールドがクリアされないように
        submit_button_pressed = st.form_submit_button(submit_button_text)

    if submit_button_pressed: # ボタンが押されたときのみ処理
        st.session_state.last_interaction_time = datetime.datetime.now()
        # フォーム外のウィジェットから値を取得
        current_target_date = target_date_form # st.date_inputから変更されたため直接参照
        current_selected_grade = st.session_state.form_grade_select_key
        current_selected_department = st.session_state.form_department_select_key
        current_selected_name = selected_name_display_form # st.selectboxの選択値 (単一)
        current_status = st.session_state.form_status_key_outside_form # フォーム外から取得
        current_late_time = st.session_state.get("form_late_time_input_key", "") if current_status == "遅刻" else "" # フォーム外から取得

        # フォーム内部のウィジェットから値を取得 (今回は手動で管理)
        current_reason = st.session_state.form_reason_input_key
        
        errors = [];
        if current_target_date is None: errors.append("練習日を選択"); 

        # 名前が「---」の場合、学年と学科の選択を必須にする
        if current_selected_name == "---":
            if current_selected_grade == "---" and current_selected_department == "---":
                errors.append("学年、学科、または名前のいずれかを選択してください。")
        
        selected_names_to_process = []
        if current_selected_name == "---": # "---"が選択されている場合は、学年と学科でフィルタリングされた全員
            all_filtered_names_for_submit = st.session_state.get('form_member_options', []) # 現在のフィルタリング結果
            if all_filtered_names_for_submit: # フィルタリング結果が空でない場合のみ対象とする
                selected_names_to_process = all_filtered_names_for_submit 
            else: # フィルタリング結果が空の場合
                errors.append("選択された学年・学科に該当する部員がいません。連絡対象がいません。")
        else: # 特定の名前が選択されている場合
            selected_names_to_process = [current_selected_name] # 単一の名前をリストとして扱う
        
        # 理由（伝達事項）の必須チェックを「参加」以外に限定
        if current_status in ["欠席", "遅刻"] and not current_reason: 
            errors.append("理由を入力"); # エラーメッセージを修正
        if current_status == "遅刻" and not current_late_time: # 遅刻を選択したが時刻が空
            errors.append("遅刻時刻を入力");

        if errors: st.warning(f"入力エラー: {', '.join(errors)}してください。") 
        else:
            # Load all existing attendance logs for the target date
            # Ensure required_cols are passed here.
            required_attendance_cols_for_check = [COL_ATTENDANCE_TIMESTAMP, COL_MEMBER_ID, COL_ATTENDANCE_TARGET_DATE, COL_ATTENDANCE_STATUS]
            attendance_df_all_logs_current_date = load_data_to_dataframe(gspread_client, SPREADSHEET_ID, ATTENDANCE_SHEET_NAME, required_cols=required_attendance_cols_for_check)

            # Determine latest status for each member for the target date
            existing_records_student_ids = set()
            if not attendance_df_all_logs_current_date.empty:
                # Ensure dt_target_date column is processed correctly
                if 'dt_target_date' not in attendance_df_all_logs_current_date.columns:
                    attendance_df_all_logs_current_date['dt_target_date'] = pd.to_datetime(attendance_df_all_logs_current_date[COL_ATTENDANCE_TARGET_DATE], errors='coerce').dt.date

                # Filter logs for the specific target_date and get the latest status per member
                relevant_logs = attendance_df_all_logs_current_date[
                    attendance_df_all_logs_current_date['dt_target_date'] == current_target_date
                ].copy()

                if not relevant_logs.empty:
                    latest_logs_per_member = relevant_logs.sort_values(by='dt_timestamp', ascending=False).drop_duplicates(subset=[COL_MEMBER_ID], keep='first')
                    # Collect IDs of members who already have a record for this date
                    existing_records_student_ids = set(latest_logs_per_member[COL_MEMBER_ID].astype(str).tolist())
            
            members_to_record_new = []
            members_skipped_already_recorded_names = []

            for name_to_submit in selected_names_to_process:
                student_id_to_submit = st.session_state.get('name_to_id_map_form', {}).get(name_to_submit)
                if not student_id_to_submit:
                    st.error(f"エラー: {name_to_submit} の学籍番号が見つかりませんでした。スキップします。")
                    continue
                
                # Check if member already has a record for this date
                if student_id_to_submit in existing_records_student_ids:
                    members_skipped_already_recorded_names.append(name_to_submit)
                    if DEBUG_MODE: print(f"DEBUG: {name_to_submit} ({student_id_to_submit}) は既に {current_target_date} の連絡済みの為スキップします。")
                else:
                    members_to_record_new.append((name_to_submit, student_id_to_submit))
                    if DEBUG_MODE: print(f"DEBUG: {name_to_submit} ({student_id_to_submit}) を {current_target_date} の連絡対象に追加します。")

            if not members_to_record_new and not members_skipped_already_recorded_names:
                st.warning("送信対象となる部員がいません。学年、学科、または名前を選択し直してください。")
                #return # Stop processing if no valid members to record

            record_count = 0
            for name_to_submit, student_id_to_submit in members_to_record_new:
                # Recording part (unchanged from original code)
                now_jst = datetime.datetime.now() + datetime.timedelta(hours=9)
                record_timestamp = now_jst.strftime("%Y-%m-%d %H:%M:%S")
                member_info_rows = member_df_for_form[member_df_for_form[COL_MEMBER_ID] == student_id_to_submit]
                grade_to_submit = ''
                department_to_submit = ''
                if not member_info_rows.empty:
                    member_info = member_info_rows.iloc[0]
                    grade_to_submit = member_info.get(COL_MEMBER_GRADE, '')
                    department_to_submit = member_info.get(COL_MEMBER_DEPARTMENT, '')
                
                record_data = {
                    '記録日時': record_timestamp,
                    '対象練習日': current_target_date.strftime('%Y/%m/%d'),
                    '学籍番号': student_id_to_submit, 
                    '学年': grade_to_submit, 
                    '名前': name_to_submit, 
                    '状況': current_status,
                    '遅刻・欠席理由': current_reason, 
                    '遅刻開始時刻': current_late_time,
                    '学科': department_to_submit 
                }
                final_record_data = {col: record_data.get(col, "") for col in OUTPUT_COLUMNS_ORDER}
                attendance_ws = get_worksheet_safe(gspread_client, SPREADSHEET_ID, ATTENDANCE_SHEET_NAME)
                if record_attendance_streamlit(attendance_ws, final_record_data):
                    record_count += 1
                else:
                    st.error(f"{name_to_submit} さんの連絡記録に失敗しました。")
            
            # --- 記録成功メッセージの生成 ---
            final_message_prefix = ""
            if current_selected_name == "---":
                # 学科まとめて連絡の場合
                if current_selected_grade != "---":
                    final_message_prefix += f"{current_selected_grade}"
                if current_selected_department != "---":
                    final_message_prefix += f"{current_selected_department}"
                final_message_prefix += "の未連絡者" # 例: "2年看護学科の未連絡者"
            else:
                # 個人連絡の場合
                final_message_prefix = f"{current_selected_name}さん"

            new_records_message_part = ""
            if record_count > 0:
                new_records_message_part = f"{record_count}名の連絡を受け付けました。"
            
            skipped_message_part = ""
            if members_skipped_already_recorded_names:
                skipped_names_str = "、".join(members_skipped_already_recorded_names)
                skipped_message_part = f"（{skipped_names_str} {len(members_skipped_already_recorded_names)}名は既に連絡済みのためスキップしました。）"
            
            # 最終メッセージの結合
            full_success_message = f"{current_target_date.strftime('%m月%d日')}の{final_message_prefix}{new_records_message_part}{skipped_message_part}"

            # メッセージ表示
            if record_count > 0 or members_skipped_already_recorded_names: # 何らかの処理が行われた場合
                st.session_state.success_message_content = full_success_message
                st.session_state.show_success_message = True
                st.rerun() 
            else: # 誰も対象にならなかった場合 (通常はerrorsで捕捉されるはずだが念のため)
                st.warning("連絡対象の部員がいませんでした。")
                st.session_state.show_success_message = False

else:
    st.warning("部員データを読み込めないため連絡フォームを表示できません。")
st.caption("連絡フォーム終了")

# --- 成功メッセージ表示の処理 ---
if st.session_state.get('show_success_message', False):
    st.success(st.session_state.success_message_content)
    st.session_state.show_success_message = False
    st.session_state.success_message_content = ""

# --- 記録参照セクションの追加 ---
st.header("２．遅刻・欠席連絡の確認")
if 'lookup_member_options' not in st.session_state: st.session_state.lookup_member_options = ["---"]
if 'name_to_id_map_lookup' not in st.session_state: st.session_state.name_to_id_map_lookup = {}

# 連絡確認フォームのコールバック (学年と学科でフィルタリング)
def update_name_options_for_lookup_callback_internal(grade, department):
    name_options = ["---"]
    id_map = {}
    if 'member_df' in st.session_state and not st.session_state.member_df.empty:
        filtered_df = st.session_state.member_df.copy()

        if grade != "---":
            filtered_df = filtered_df[filtered_df[COL_MEMBER_GRADE].astype(str).str.strip() == str(grade).strip()]
        if department != "---":
            filtered_df = filtered_df[filtered_df[COL_MEMBER_DEPARTMENT].astype(str).str.strip() == str(department).strip()]

        if not filtered_df.empty:
            name_options = ["---"] + filtered_df[COL_MEMBER_NAME].tolist()
            id_map = pd.Series(filtered_df[COL_MEMBER_ID].values, index=filtered_df[COL_MEMBER_NAME]).to_dict()
    st.session_state.lookup_member_options = name_options
    st.session_state.name_to_id_map_lookup = id_map

# UIのコールバックハンドラー
def handle_lookup_grade_change():
    update_name_options_for_lookup_callback_internal(st.session_state.lookup_grade_select_key, st.session_state.lookup_department_select_key)

def handle_lookup_department_change():
    update_name_options_for_lookup_callback_internal(st.session_state.lookup_grade_select_key, st.session_state.lookup_department_select_key)

if st.session_state.authentication_status is True:
    if not st.session_state.member_df.empty:
        # 初期表示またはセッション状態が空の場合にオプションを更新
        if st.session_state.lookup_member_options == ["---"]:
            update_name_options_for_lookup_callback_internal(
                st.session_state.get('lookup_grade_select_key', "---"), 
                st.session_state.get('lookup_department_select_key', "---")
            )
        col_grade_lookup, col_department_lookup, col_name_lookup = st.columns(3) # カラム数変更
        with col_grade_lookup:
            selected_grade_lookup = st.selectbox(
                "あなたの学年:",
                st.session_state.get('grade_options', ["---"]),
                key="lookup_grade_select_key",
                on_change=handle_lookup_grade_change
            )
        with col_department_lookup: # 新規追加
            selected_department_lookup = st.selectbox( # 新規追加
                "あなたの学科:", # 新規追加
                st.session_state.get('department_options', ["---"]), # 新規追加
                key="lookup_department_select_key", # 新規追加
                on_change=handle_lookup_department_change # 新規追加
            ) # 新規追加
        with col_name_lookup: # 3カラム目
            selected_name_lookup = st.selectbox(
                f"あなたの名前 ({selected_grade_lookup if selected_grade_lookup != '---' else '学年未選択'}"
                f"{' / ' + selected_department_lookup if selected_department_lookup != '---' else ''}):", # 学科表示も追加
                options=st.session_state.get('lookup_member_options', ["---"]),
                key="lookup_name_select_key"
            )
        if st.button("過去の連絡を確認する", key="lookup_submit_button_key"):
            st.session_state.last_interaction_time = datetime.datetime.now()
            grade_to_lookup = st.session_state.lookup_grade_select_key
            name_to_lookup = st.session_state.lookup_name_select_key
            student_id_to_lookup = st.session_state.get('name_to_id_map_lookup', {}).get(name_to_lookup)

            if grade_to_lookup == "---" or name_to_lookup == "---" or not student_id_to_lookup:
                st.warning("学年と名前を選択してください。")
            else:
                with st.spinner("過去の連絡を読み込み中..."):
                    attendance_df_all = load_data_to_dataframe(gspread_client, SPREADSHEET_ID, ATTENDANCE_SHEET_NAME, required_cols=None)

                    if attendance_df_all.empty:
                        st.info("過去の連絡記録はまだありません。")
                    else:
                        user_records_df = attendance_df_all[
                            attendance_df_all[COL_MEMBER_ID].astype(str).str.strip() == str(student_id_to_lookup).strip()
                        ].copy()

                        if user_records_df.empty:
                            st.info(f"{name_to_lookup} さん ({student_id_to_lookup}) の過去の連絡記録が見つかりませんでした。")
                        else:
                            # 同じ日付で複数の連絡がある場合、最新のもののみを考慮して最終ステータスを表示
                            user_records_df['dt_timestamp'] = pd.to_datetime(user_records_df[COL_ATTENDANCE_TIMESTAMP], errors='coerce')
                            user_records_df['dt_target_date'] = pd.to_datetime(user_records_df[COL_ATTENDANCE_TARGET_DATE], errors='coerce').dt.date
                            user_records_df.dropna(subset=['dt_timestamp', 'dt_target_date'], inplace=True)
                            # 最新のステータスを優先 (タイムスタンプ降順、対象日降順でソート後、対象日で重複削除)
                            user_records_df_latest = user_records_df.sort_values(by=['dt_target_date', 'dt_timestamp'], ascending=[False, False]) \
                                .drop_duplicates(subset=['dt_target_date'], keep='first')
                            st.subheader(f"{name_to_lookup} さんの過去の連絡記録 (最新情報)")
                            st.dataframe(user_records_df_latest[LOOKUP_DISPLAY_COLUMNS])

    else:
        st.info("部員データを読み込めないため記録参照フォームを表示できません。")
elif st.session_state.authentication_status is True and not st.session_state.is_admin:
    st.info("過去の連絡を参照するには、まず共通パスワードでログインしてください。")

st.caption("記録参照機能終了")

# --- コート割り振りセクション (管理者向け) ---
st.header("３．コート割り振り (管理者向け)")
if not st.session_state.is_admin:
    st.subheader("管理者用パスワードを入力してください")
    admin_password_input = st.text_input("管理者パスワード", type="password", key="admin_password_input_key")
    if st.button("管理者としてログイン", key="admin_login_button_key"):
        check_admin_password()
        if st.session_state.is_admin: st.rerun()

if st.session_state.is_admin:
    st.success("管理者としてログイン済みです。")
    if not st.session_state.member_df.empty:
        target_date_assign_input = st.date_input("割り振り対象日を選択:", value=datetime.date.today(), key="assignment_date_admin_main")
        
        # 8チーム割り振りの1年生（レベル1）に関するラジオボタン
        include_level1_for_8_teams_selection = st.radio(
            "8チーム割り振りに1年生（レベル1）を含めますか？",
            options=["含める", "含めない"],
            key="include_level1_assign_radio_8_teams",
            horizontal=True
        )

        if st.button("コート割り振りを実行して結果シートを更新", key="assign_button_admin_main"):
            st.session_state.last_interaction_time = datetime.datetime.now()
            with st.spinner(f"{target_date_assign_input.strftime('%Y-%m-%d')} のコート割り振り中..."):
                attendance_df_all_logs = load_data_to_dataframe(gspread_client, SPREADSHEET_ID, ATTENDANCE_SHEET_NAME, required_cols=None)
                if DEBUG_MODE: st.write(f"割り振り対象日: {target_date_assign_input}")

                member_df_assign = st.session_state.member_df
                # --- 各部員の最終連絡ステータスを判定するロジック ---
                # まず、全ての部員IDを取得
                all_member_ids = set(member_df_assign[COL_MEMBER_ID].astype(str).tolist())
                # その日の全ての関連する連絡ログを取得
                latest_status_by_member = pd.DataFrame() 
                relevant_logs_for_target_date = pd.DataFrame() 
                if attendance_df_all_logs is not None and not attendance_df_all_logs.empty:
                    temp_df_logs_for_status = attendance_df_all_logs.copy()
                    temp_df_logs_for_status['dt_timestamp'] = pd.to_datetime(temp_df_logs_for_status[COL_ATTENDANCE_TIMESTAMP], errors='coerce')
                    temp_df_logs_for_status['dt_target_date'] = pd.to_datetime(temp_df_logs_for_status[COL_ATTENDANCE_TARGET_DATE], errors='coerce').dt.date
                    # 割り振り対象日のログに絞り込み
                    relevant_logs_for_target_date = temp_df_logs_for_status[temp_df_logs_for_status['dt_target_date'] == target_date_assign_input].copy()
                    if not relevant_logs_for_target_date.empty:
                        # 各部員IDに対して最新の連絡のみを保持 (最新のタイムスタンプを持つものを優先)
                        latest_status_by_member = relevant_logs_for_target_date.sort_values(by='dt_timestamp', ascending=False).drop_duplicates(subset=[COL_MEMBER_ID], keep='first')
                
                # 最終的なステータスセットを初期化
                participating_ids_final = set() # 「参加」ステータスの部員
                late_ids_final = set()          # 「遅刻」ステータスの部員
                absent_ids_final = set()        # 「欠席」ステータスの部員

                # 全ての部員について最終ステータスを決定
                for member_id in all_member_ids:
                    member_latest_log = latest_status_by_member[latest_status_by_member[COL_MEMBER_ID] == member_id]
                    if not member_latest_log.empty:
                        status = str(member_latest_log.iloc[0][COL_ATTENDANCE_STATUS]).strip()
                        if status == '参加':
                            participating_ids_final.add(member_id)
                        elif status == '遅刻':
                            late_ids_final.add(member_id)
                        elif status == '欠席':
                            absent_ids_final.add(member_id)
                    else:
                        # 連絡が全くない部員は「参加」とみなす (デフォルト)
                        participating_ids_final.add(member_id)

                # rebalance_teams_by_gender_and_level の引数にもなる late_member_ids は late_ids_final を使用
                late_member_ids_for_rebalance = late_ids_final 

                # --- 名簿出力用DataFrameの準備 (最新のfinal_idsに基づいて) ---
                # 参加者名簿用: 最終ステータスが「参加」の部員のみ
                pool_for_participant_list_output = member_df_assign[
                    member_df_assign[COL_MEMBER_ID].astype(str).isin(participating_ids_final)
                ].copy()
                if DEBUG_MODE: st.write(f"参加者名簿対象 (最終ステータスが「参加」): {len(pool_for_participant_list_output)} 名")


                # 欠席者名簿用: 最終ステータスが「欠席」の部員
                pool_for_absent_list_output = member_df_assign[
                    member_df_assign[COL_MEMBER_ID].astype(str).isin(absent_ids_final)
                ].copy()
                if DEBUG_MODE: st.write(f"欠席者名簿対象 (最終ステータスが「欠席」): {len(pool_for_absent_list_output)} 名")

                # 欠席理由をマージする
                if not pool_for_absent_list_output.empty and not latest_status_by_member.empty:
                    pool_for_absent_list_output = pd.merge(
                        pool_for_absent_list_output,
                        latest_status_by_member[[COL_MEMBER_ID, COL_ATTENDANCE_REASON]],
                        on=COL_MEMBER_ID,
                        how='left'
                    )
                else:
                    pool_for_absent_list_output[COL_ATTENDANCE_REASON] = '' # 理由列がない場合は追加

                # 遅刻者名簿用: 最終ステータスが「遅刻」の部員 (詳細情報をマージ)
                late_members_df_for_output = member_df_assign[member_df_assign[COL_MEMBER_ID].astype(str).isin(late_ids_final)].copy()
                if not late_members_df_for_output.empty and not relevant_logs_for_target_date.empty: 
                    # 遅刻時間と理由をマージ
                    late_members_df_for_output = pd.merge(late_members_df_for_output, latest_status_by_member[[COL_MEMBER_ID, COL_ATTENDANCE_LATE_TIME, COL_ATTENDANCE_REASON]], 
                                                          on=COL_MEMBER_ID, how='left')
                if DEBUG_MODE: st.write(f"遅刻者名簿対象 (最終ステータスが「遅刻」): {len(late_members_df_for_output)} 名")


                # --- チーム割り振り用プール ---
                # 8, 10, 12コート割り振り用: 最終ステータスが「参加」または「遅刻」の部員
                pool_for_8_10_12_assignment = member_df_assign[
                    member_df_assign[COL_MEMBER_ID].astype(str).isin(participating_ids_final | late_ids_final)
                ].copy()
                if DEBUG_MODE: st.write(f"8,10,12コート割り振り対象総数 (最終「参加」+「遅刻」): {len(pool_for_8_10_12_assignment)} 名")


                # 3チーム割り振り用: 最終ステータスが「参加」の部員のみ (遅刻者は除外)
                pool_for_3_team_assignment = member_df_assign[
                    member_df_assign[COL_MEMBER_ID].astype(str).isin(participating_ids_final)
                ].copy()
                if DEBUG_MODE: st.write(f"3チーム割り振り対象総数 (最終「参加」のみ): {len(pool_for_3_team_assignment)} 名")


                # --- 名簿シートの出力 (上記の新しいプール変数を使用) ---
                participant_ws = get_worksheet_safe(gspread_client, SPREADSHEET_ID, PARTICIPANT_LIST_SHEET_NAME) 
                if participant_ws: 
                    if DEBUG_MODE: st.write(f"参加者名簿 ({target_date_assign_input}) を出力...") 
                    if not pool_for_participant_list_output.empty:
                        output_cols_p = [COL_MEMBER_ID, COL_MEMBER_NAME, COL_MEMBER_GRADE, COL_MEMBER_LEVEL, COL_MEMBER_GENDER, COL_MEMBER_DEPARTMENT] 
                        valid_output_cols_p = [col for col in output_cols_p if col in pool_for_participant_list_output.columns] 
                        participant_list_output = [[f"{target_date_assign_input.strftime('%Y-%m-%d')} 参加者リスト"]] 
                        participant_list_output.append(valid_output_cols_p); 
                        participant_list_output.extend(pool_for_participant_list_output[valid_output_cols_p].values.tolist()) 
                        write_results_to_sheet(participant_ws, participant_list_output, data_name=f"{target_date_assign_input.strftime('%Y-%m-%d')} 参加者名簿") 
                    else: 
                        write_results_to_sheet(participant_ws, [[f"{target_date_assign_input.strftime('%Y-%m-%d')} の参加者なし"]], data_name="参加者名簿") 
                else: st.error(f"シート '{PARTICIPANT_LIST_SHEET_NAME}' が見つかりません。") 
                
                absent_ws = get_worksheet_safe(gspread_client, SPREADSHEET_ID, ABSENT_LIST_SHEET_NAME) 
                if absent_ws: 
                    if DEBUG_MODE: st.write(f"欠席者名簿 ({target_date_assign_input}) を出力...") 
                    if not pool_for_absent_list_output.empty: # 欠席者名簿用プールを使用
                        absent_output_cols = [COL_MEMBER_ID, COL_MEMBER_NAME, COL_MEMBER_GRADE, COL_ATTENDANCE_REASON, COL_MEMBER_DEPARTMENT] # 学科追加
                        valid_absent_cols = [col for col in absent_output_cols if col in pool_for_absent_list_output.columns] 
                        absent_list_output = [[f"{target_date_assign_input.strftime('%Y-%m-%d')} 欠席者リスト"]] 
                        absent_list_output.append(valid_absent_cols)
                        absent_list_output.extend(pool_for_absent_list_output[valid_absent_cols].fillna('').values.tolist()) 
                        write_results_to_sheet(absent_ws, absent_list_output, data_name=f"欠席者名簿") 
                    else: 
                        absent_list_output = [[f"{target_date_assign_input.strftime('%Y-%m-%d')} の欠席連絡者なし"]] 
                        write_results_to_sheet(absent_ws, absent_list_output, data_name=f"欠席者名簿") 
                else: st.error(f"シート '{ABSENT_LIST_SHEET_NAME}' が見つかりません。") 

                late_ws = get_worksheet_safe(gspread_client, SPREADSHEET_ID, LATE_LIST_SHEET_NAME)
                if late_ws:
                    if DEBUG_MODE: st.write(f"遅刻者名簿 ({target_date_assign_input}) を出力...")
                    if not late_members_df_for_output.empty: # 遅刻者名簿用プールはそのまま
                        late_output_cols = [COL_MEMBER_ID, COL_MEMBER_NAME, COL_MEMBER_GRADE, COL_ATTENDANCE_LATE_TIME, COL_ATTENDANCE_REASON, COL_MEMBER_DEPARTMENT] # 学科追加
                        valid_late_cols = [col for col in late_output_cols if col in late_members_df_for_output.columns]
                        late_list_output = [[f"{target_date_assign_input.strftime('%Y-%m-%d')} 遅刻者リスト"]]
                        late_list_output.append(valid_late_cols); 
                        late_list_output.extend(late_members_df_for_output[valid_late_cols].fillna('').values.tolist())
                        write_results_to_sheet(late_ws, late_list_output, data_name=f"遅刻者名簿")
                    else: 
                        late_list_output = [[f"{target_date_assign_input.strftime('%Y-%m-%m')} の遅刻連絡者なし"]]
                        write_results_to_sheet(late_ws, late_list_output, data_name=f"遅刻者名簿")
                else: st.error(f"シート '{LATE_LIST_SHEET_NAME}' が見つかりません。")
                # --- 名簿シートの出力ここまで ---


                if pool_for_8_10_12_assignment.empty:
                    st.warning("割り振り対象の参加予定者がいないため、コート割り振りは行いません。")
                else:
                    num_teams_8 = TEAMS_COUNT_MAP.get('ノック', 8)
                    num_teams_10 = TEAMS_COUNT_MAP.get('ハンドノック', 10)
                    num_teams_12 = TEAMS_COUNT_MAP.get('その他', 12)
                    num_teams_3 = 3 # 3チーム割り振りの場合

                    # --- 各割り振り用のメンバープールを準備 ---
                    # 8チーム割り振り用のメンバープール (1年生の扱いをラジオボタンで選択)
                    pool_for_8_teams = pool_for_8_10_12_assignment.copy() # 初期値は遅刻者含む全員
                    if include_level1_for_8_teams_selection == "含めない":
                        # レベル1を除外する選択の場合、プールからレベル1をフィルタリング
                        pool_for_8_teams = pool_for_8_teams[pool_for_8_teams[COL_MEMBER_LEVEL] != 1].copy()
                        if DEBUG_MODE: st.write(f"8チーム割り振り対象者 (レベル1除く): {len(pool_for_8_teams)} 名")

                    # 10チーム割り振り用メンバープールは常にレベル1を含む (遅刻者含む)
                    pool_for_10_teams = pool_for_8_10_12_assignment.copy() 

                    # 12チーム割り振り用メンバープールは常にレベル1を含む (遅刻者含む)
                    pool_for_12_teams = pool_for_8_10_12_assignment.copy() 

                    # 3チーム割り振り用メンバープール (遅刻者は含めない)
                    # pool_for_3_team_assignment は既に上で定義されているのでそのまま使う


                    # --- 割り振り実行 ---
                    # 8チーム割り振り
                    assignment_ws_8 = get_worksheet_safe(gspread_client, SPREADSHEET_ID, ASSIGNMENT_SHEET_NAME_8)
                    if assignment_ws_8:
                        if DEBUG_MODE: st.write("--- 8チーム割り振りを実行中 ---")
                        assignments_8 = assign_teams(
                            pool_for_8_teams, # フィルタリング済みプールを渡す
                            late_member_ids_for_rebalance, # 遅刻者IDを渡す (入れ替え対象外判定用)
                            num_teams_8,
                            assignment_type="8チーム" # 割り振りタイプを渡す
                        )
                        if assignments_8: result_output_8 = format_assignment_results(assignments_8, "8チーム", target_date_assign_input); write_results_to_sheet(assignment_ws_8, result_output_8, f"8チーム結果({target_date_assign_input.strftime('%Y-%m-%d')})")
                        else: st.warning("8チーム割り振り結果なし。")
                    else: st.error(f"シート '{ASSIGNMENT_SHEET_NAME_8}' が見つかりません。")
                    
                    # 10チーム割り振り
                    assignment_ws_10 = get_worksheet_safe(gspread_client, SPREADSHEET_ID, ASSIGNMENT_SHEET_NAME_10)
                    if assignment_ws_10:
                        if DEBUG_MODE: st.write("--- 10チーム割り振りを実行中 ---")
                        assignments_10 = assign_teams(
                            pool_for_10_teams, # レベル1を含むプール (遅刻者含む)
                            late_member_ids_for_rebalance, # 遅刻者IDを渡す (入れ替え対象外判定用)
                            num_teams_10,
                            assignment_type="10チーム" # 振りタイプを渡す
                        )
                        if assignments_10: result_output_10 = format_assignment_results(assignments_10, "10チーム", target_date_assign_input); write_results_to_sheet(assignment_ws_10, result_output_10, f"10チーム結果({target_date_assign_input.strftime('%Y-%m-%d')})")
                        else: st.warning("10チーム割り振り結果なし。")
                    else: st.error(f"シート '{ASSIGNMENT_SHEET_NAME_10}' が見つかりません。")

                    # 12チーム割り振り
                    assignment_ws_12 = get_worksheet_safe(gspread_client, SPREADSHEET_ID, ASSIGNMENT_SHEET_NAME_12)
                    if assignment_ws_12:
                        if DEBUG_MODE: st.write("--- 12チーム割り振りを実行中 ---")
                        assignments_12 = assign_teams(
                            pool_for_12_teams, # 遅刻者を含むプールを渡す
                            late_member_ids_for_rebalance, # 遅刻者IDを渡す (入れ替え対象外判定用)
                            num_teams_12,
                            assignment_type="12チーム" # 割り振りタイプを渡す
                        )
                        if assignments_12: result_output_12 = format_assignment_results(assignments_12, "12チーム", target_date_assign_input); write_results_to_sheet(assignment_ws_12, result_output_12, f"12チーム結果({target_date_assign_input.strftime('%Y-%m-%d')})")
                        else: st.warning("12チーム割り振り結果なし。")
                    else: st.error(f"シート '{ASSIGNMENT_SHEET_NAME_12}' が見つかりません。")
                    
                    # --- 3チーム割り振り (遅刻者は含めないように変更) ---
                    assignment_ws_3 = get_worksheet_safe(gspread_client, SPREADSHEET_ID, ASSIGNMENT_SHEET_NAME_3)
                    if assignment_ws_3:
                        if DEBUG_MODE: st.write("--- 3チーム割り振りを実行中 (素振り指導向け - 遅刻者除外) ---")
                        assignments_3 = assign_teams(
                            pool_for_3_team_assignment, # 遅刻者を含まないプールを渡す
                            late_member_ids_for_rebalance, # 遅刻者IDを渡す (入れ替え対象外判定用)
                            num_teams_3,
                            assignment_type="3チーム (素振り指導)" # 割り振りタイプを渡す
                        )
                        if assignments_3: result_output_3 = format_assignment_results(assignments_3, "3チーム (素振り指導)", target_date_assign_input); write_results_to_sheet(assignment_ws_3, result_output_3, f"3チーム結果({target_date_assign_input.strftime('%Y-%m-%d')})")
                        else: st.warning("3チーム割り振り結果なし。")
                    else: st.error(f"シート '{ASSIGNMENT_SHEET_NAME_3}' が見つかりません。")
                    # --- 3チーム割り振りここまで ---

            st.info(f"{target_date_assign_input.strftime('%Y-%m-%d')} の割り振り処理と名簿出力が完了しました。")
    else:
        st.info("コート割り振り実行には部員データが必要です。")
elif st.session_state.authentication_status is True and not st.session_state.is_admin:
    st.info("コート割り振り機能は管理者専用です。")
st.caption("システム管理者向けエリア")