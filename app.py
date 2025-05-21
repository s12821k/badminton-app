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
st.set_page_config(page_title="バドミントン部 連絡システム", layout="centered", page_icon="🏸") # アイコンを絵文字に修正

# === 2. 設定値 (st.secrets からも読み込む) ===
try:
    APP_PASSWORDS_SECRETS = st.secrets.get('app_passwords', {})
    GENERAL_PASSWORD_SECRET = APP_PASSWORDS_SECRETS.get("general_password")
    ADMIN_PASSWORD_SECRET = APP_PASSWORDS_SECRETS.get("admin_password")

    APP_CONFIG = st.secrets.get('app_config', {})
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
# ローカル開発用: 'your_credentials.json' ファイルから読み込む
# Streamlit Cloud デプロイ用: st.secrets から読み込む
# CREDENTIALS_JSON_PATH = 'your_credentials.json' # この行はローカル開発時のみ有効にするか、削除
SCOPES_GSPREAD = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

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

# --- 列名 (ヘッダー名) ---
COL_MEMBER_ID = '学籍番号'; COL_MEMBER_NAME = '名前'; COL_MEMBER_GRADE = '学年';
COL_MEMBER_LEVEL = 'レベル'; COL_MEMBER_GENDER = '性別';
COL_ATTENDANCE_TIMESTAMP = '記録日時';
COL_ATTENDANCE_TARGET_DATE = '対象練習日';
COL_ATTENDANCE_STATUS = '状況';
COL_ATTENDANCE_LATE_TIME = '遅刻開始時刻';
COL_ATTENDANCE_REASON = '遅刻・欠席理由';
OUTPUT_COLUMNS_ORDER = ['記録日時', '対象練習日', '学籍番号', '学年', '名前', '状況', '遅刻・欠席理由', '遅刻開始時刻']
# 連絡確認フォームの表示用列 (学籍番号と遅刻・欠席理由を除外)
LOOKUP_DISPLAY_COLUMNS = ['記録日時', '対象練習日', '学年', '名前', '状況', '遅刻開始時刻']

# --- コート割り振り設定 ---
DEFAULT_PRACTICE_TYPE = 'ノック';
TEAMS_COUNT_MAP = {'ノック': 8, 'ハンドノック': 10, 'その他': 12}
INACTIVITY_TIMEOUT_MINUTES = 10

# === 3. 関数定義 ===
@st.cache_resource
def authenticate_gspread_service_account():
    if DEBUG_MODE: print("Attempting gspread Service Account Authentication...")
    try:
        if 'google_credentials' in st.secrets:
            creds_info = st.secrets['google_credentials']
            creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES_GSPREAD)
            if DEBUG_MODE: print("gspread Service Account Authentication successful (from Secrets).")
            client = gspread.authorize(creds)
            return client
        elif os.path.exists('your_credentials.json'):
            st.warning("警告: ローカルファイルから認証情報を読み込んでいます。本番環境ではSecretsを使用してください。")
            creds = Credentials.from_service_account_file('your_credentials.json', scopes=SCOPES_GSPREAD)
            if DEBUG_MODE: print("gspread Service Account Authentication successful (from File).")
            client = gspread.authorize(creds)
            return client
        else:
            st.error("認証エラー: Google Sheets 認証情報が見つかりません。Secretsに設定するか、your_credentials.jsonを配置してください。")
            print("ERROR: Google Sheets credentials not found.")
            return None
    except FileNotFoundError:
        st.error(f"認証エラー(SA): your_credentials.jsonなし"); print(f"ERROR: SA Credentials file not found: {os.path.abspath('your_credentials.json')}"); return None
    except Exception as e:
        st.error(f"認証エラー(SA): {e}"); print(f"ERROR: SA Authentication error: {e}"); return None

def get_worksheet_safe(gspread_client, spreadsheet_id, sheet_name):
    if not gspread_client: return None
    if DEBUG_MODE: print(f"ワークシート '{sheet_name}' を取得中...")
    try:
        spreadsheet = gspread_client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(sheet_name)
        if DEBUG_MODE: print(f"-> '{sheet_name}' を取得しました。")
        return worksheet
    except Exception as e: st.error(f"ワークシート '{sheet_name}' 取得エラー: {e}"); print(f"Error getting worksheet '{sheet_name}': {e}"); return None

@st.cache_data(ttl=300)
def load_data_to_dataframe(_gspread_client, spreadsheet_id, sheet_name, required_cols=None):
    if DEBUG_MODE: print(f"データを読み込みます: {sheet_name}")
    worksheet = get_worksheet_safe(_gspread_client, spreadsheet_id, sheet_name)
    if worksheet is None: return pd.DataFrame()
    try:
        data = worksheet.get_all_records(); df = pd.DataFrame(data)
        if DEBUG_MODE: print(f"-> {len(df)}件読み込み完了 ({sheet_name})")
        if required_cols:
            missing = [col for col in required_cols if col not in df.columns]
            if missing: st.error(f"シート '{sheet_name}' に必要な列がありません: {missing}"); print(f"ERROR: Missing required columns: {missing}"); return pd.DataFrame()
        if COL_MEMBER_ID in df.columns: df[COL_MEMBER_ID] = df[COL_MEMBER_ID].astype(str).str.strip()
        if COL_MEMBER_NAME in df.columns: df[COL_MEMBER_NAME] = df[COL_MEMBER_NAME].astype(str).str.strip()
        if COL_MEMBER_GRADE in df.columns: df[COL_MEMBER_GRADE] = df[COL_MEMBER_GRADE].astype(str).str.strip()
        if COL_MEMBER_LEVEL in df.columns: df[COL_MEMBER_LEVEL] = pd.to_numeric(df[COL_MEMBER_LEVEL], errors='coerce')
        if COL_MEMBER_GENDER in df.columns: df[COL_MEMBER_GENDER] = df[COL_MEMBER_GENDER].astype(str).str.strip()
        if sheet_name == ATTENDANCE_SHEET_NAME:
            if COL_ATTENDANCE_TIMESTAMP in df.columns: df['dt_timestamp'] = pd.to_datetime(df[COL_ATTENDANCE_TIMESTAMP], errors='coerce')
            if COL_ATTENDANCE_TARGET_DATE in df.columns: df['dt_target_date'] = pd.to_datetime(df[COL_ATTENDANCE_TARGET_DATE], errors='coerce').dt.date
        return df
    except Exception as e: st.error(f"データ読み込みエラー ({sheet_name}): {e}"); print(f"ERROR: Data loading error: {e}"); return pd.DataFrame()

def record_attendance_streamlit(worksheet, data_dict):
    if worksheet is None: st.error("記録用シートが見つかりません。"); return False
    try:
        row_data = [data_dict.get(col_name, "") for col_name in OUTPUT_COLUMNS_ORDER]
        worksheet.append_row(row_data, value_input_option='USER_ENTERED')
        if DEBUG_MODE: print(f"記録成功: {row_data}")
        return True
    except Exception as e: st.error(f"記録エラー: {e}"); print(f"ERROR: Error recording: {e}"); return False

def get_absent_ids_for_date(attendance_df, target_date):
    """
    指定された日付の「欠席」ステータスの学籍番号のセットを返します。
    """
    if DEBUG_MODE: print(f"\n{target_date} の不参加者リストを作成します...")
    absent_ids = set()
    required_attendance_cols = [COL_ATTENDANCE_TIMESTAMP, COL_MEMBER_ID, COL_ATTENDANCE_TARGET_DATE, COL_ATTENDANCE_STATUS]
    if not all(col in attendance_df.columns for col in required_attendance_cols):
        missing = [col for col in required_attendance_cols if col not in attendance_df.columns];
        st.warning(f"警告: 遅刻欠席ログに必要な列が見つかりません: {missing}。"); print(f"WARNING: Missing cols in attendance log: {missing}."); return absent_ids
    try:
        df = attendance_df.copy();
        if COL_ATTENDANCE_TIMESTAMP in df.columns: df['dt_timestamp'] = pd.to_datetime(df[COL_ATTENDANCE_TIMESTAMP], errors='coerce')
        if COL_ATTENDANCE_TARGET_DATE in df.columns: df['dt_target_date'] = pd.to_datetime(df[COL_ATTENDANCE_TARGET_DATE], errors='coerce').dt.date
        df.dropna(subset=['dt_timestamp', 'dt_target_date'], inplace=True)
        df[COL_MEMBER_ID] = df[COL_MEMBER_ID].astype(str).str.strip()
        df.dropna(subset=[COL_MEMBER_ID], inplace=True); df = df[df[COL_MEMBER_ID] != '']

        relevant_logs = pd.DataFrame()

        if not df.empty:
            target_datetime_end = datetime.datetime.combine(target_date, datetime.time.max);
            relevant_logs = df[(df['dt_target_date'] == target_date) & (df['dt_timestamp'] <= target_datetime_end)]

        if relevant_logs.empty:
            if DEBUG_MODE: print(f"{target_date} に関する有効な形式の連絡はありませんでした。"); return absent_ids

        latest_logs = relevant_logs.sort_values(by='dt_timestamp', ascending=False).drop_duplicates(subset=[COL_MEMBER_ID], keep='first')
        for index, row in latest_logs.iterrows():
            status = str(row.get(COL_ATTENDANCE_STATUS, '')).strip(); student_id = str(row.get(COL_MEMBER_ID, '')).strip()
            # 修正箇所: ステータスが「欠席」の場合のみ absent_ids に追加する
            if student_id and status == '欠席':
                absent_ids.add(student_id)
        if DEBUG_MODE: print(f"{target_date} の不参加者 ({len(absent_ids)}名) 特定完了。")
    except KeyError as e: st.error(f"エラー(欠席者特定): 列名 '{e}' 不明"); print(f"KeyError(get_absent_ids): {e}")
    except Exception as e: st.error(f"不参加者特定中にエラー: {e}"); print(f"Error(get_absent_ids): {e}"); return None

    return absent_ids

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
    return max(male_count, female_count) / min(male_count, female_count)

def rebalance_teams_by_gender_and_level(teams, team_stats, late_member_ids, max_iterations=5):
    """
    チーム間の男女比の偏りを、同レベルの部員を交換することで再調整します。
    チームの人数とレベル分布は維持されます。遅刻者は交換の対象外とします。
    """
    if DEBUG_MODE: print("\n性別・レベル均等化のためのチーム再調整を開始...")

    for _ in range(max_iterations): # 複数回イテレーションして、より良いバランスを見つける
        swapped_in_iteration = False
        team_names = list(teams.keys())
        random.shuffle(team_names) # 処理順をランダムにして偏りを防ぐ

        for i in range(len(team_names)):
            team_a_name = team_names[i]
            team_a_members = teams[team_a_name]
            team_a_stats = team_stats[team_a_name]

            # チームAに交換を検討する十分な人数がいるか確認
            if team_a_stats['count'] < 2:
                continue

            current_imbalance_a = calculate_imbalance_score(team_a_stats['male_count'], team_a_stats['female_count'])

            # チームAが「2倍以上」の偏りがある場合にのみ再調整を試みる
            if current_imbalance_a < 2.0:
                continue

            # チームAから交換に出す性別（偏っている方の性別）を決定
            gender_to_swap_out_a = '男性' if team_a_stats['male_count'] > team_a_stats['female_count'] else '女性'
            # チームAに交換で入れる性別（不足している方の性別）を決定
            gender_to_swap_in_a = '女性' if gender_to_swap_out_a == '男性' else '男性'

            # チームAから交換に出す部員候補を探す（偏っている性別の部員、かつ遅刻者ではない）
            member_a_candidate = None
            members_of_gender_to_swap_out_a = [m for m in team_a_members if m.get(COL_MEMBER_GENDER) == gender_to_swap_out_a and m.get(COL_MEMBER_ID) not in late_member_ids]
            if not members_of_gender_to_swap_out_a:
                continue
            member_a_candidate = random.choice(members_of_gender_to_swap_out_a)
            level_a = member_a_candidate.get(COL_MEMBER_LEVEL)

            if level_a is None: # レベル情報がない場合はスキップ
                continue

            # 交換相手となるチームBを探す
            for j in range(len(team_names)):
                if i == j: continue # 同じチームとは交換しない
                team_b_name = team_names[j]
                team_b_members = teams[team_b_name]
                team_b_stats = team_stats[team_b_name]

                # チームBに交換を検討する十分な人数がいるか確認
                if team_b_stats['count'] < 2:
                    continue

                # チームBから交換で入れる部員候補を探す（チームAが必要とする性別で、かつレベルが同じ部員、かつ遅刻者ではない）
                member_b_candidate = None
                members_of_gender_to_swap_in_a_from_b = [m for m in team_b_members if m.get(COL_MEMBER_GENDER) == gender_to_swap_in_a and m.get(COL_MEMBER_LEVEL) == level_a and m.get(COL_MEMBER_ID) not in late_member_ids]
                if members_of_gender_to_swap_in_a_from_b:
                    member_b_candidate = random.choice(members_of_gender_to_swap_in_a_from_b)

                if member_b_candidate:
                    # 交換後のチームAの男女数を予測
                    new_male_a = team_a_stats['male_count'] - (1 if gender_to_swap_out_a == '男性' else 0) + (1 if gender_to_swap_in_a == '男性' else 0)
                    new_female_a = team_a_stats['female_count'] - (1 if gender_to_swap_out_a == '女性' else 0) + (1 if gender_to_swap_in_a == '女性' else 0)
                    new_imbalance_a = calculate_imbalance_score(new_male_a, new_female_a)

                    # 交換後のチームBの男女数を予測
                    new_male_b = team_b_stats['male_count'] - (1 if gender_to_swap_in_a == '男性' else 0) + (1 if gender_to_swap_out_a == '男性' else 0)
                    new_female_b = team_b_stats['female_count'] - (1 if gender_to_swap_in_a == '女性' else 0) + (1 if gender_to_swap_out_a == '女性' else 0)
                    new_imbalance_b = calculate_imbalance_score(new_male_b, new_female_b)

                    # 現在のチームBの偏りスコア
                    current_imbalance_b = calculate_imbalance_score(team_b_stats['male_count'], team_b_stats['female_count'])

                    # 交換を行う条件:
                    # 1. チームAの新しい偏りスコアが現在のスコアよりも改善される AND
                    # 2. チームBの新しい偏りスコアが、現在のスコアの2倍以上に悪化しない
                    # または
                    # 3. 両チームの合計偏りスコアが減少する
                    if (new_imbalance_a < current_imbalance_a and new_imbalance_b < 2.0 * current_imbalance_b) or \
                       (new_imbalance_a + new_imbalance_b < current_imbalance_a + current_imbalance_b):

                        # 交換の実行
                        teams[team_a_name].remove(member_a_candidate)
                        teams[team_a_name].append(member_b_candidate)
                        teams[team_b_name].remove(member_b_candidate)
                        teams[team_b_name].append(member_a_candidate)

                        # 統計情報の更新
                        team_a_stats['male_count'] = new_male_a
                        team_a_stats['female_count'] = new_female_a
                        team_b_stats['male_count'] = new_male_b
                        team_b_stats['female_count'] = new_female_b

                        if DEBUG_MODE:
                            print(f"DEBUG: {member_a_candidate.get(COL_MEMBER_NAME)} (L{level_a}, {gender_to_swap_out_a}) を {team_a_name} から "
                                  f"{member_b_candidate.get(COL_MEMBER_NAME)} (L{level_a}, {gender_to_swap_in_a}) を {team_b_name} と交換しました。")
                            print(f"DEBUG: {team_a_name} の統計: {team_a_stats['male_count']}M/{team_a_stats['female_count']}F (新偏り: {new_imbalance_a:.2f})")
                            print(f"DEBUG: {team_b_name} の統計: {team_b_stats['male_count']}M/{team_b_stats['female_count']}F (新偏り: {new_imbalance_b:.2f})")

                        swapped_in_iteration = True
                        break # 内側のループを抜け、外側のイテレーションを再開して再評価
            if swapped_in_iteration:
                break # 外側のループを抜け、max_iterationsループを再開

        if not swapped_in_iteration:
            break # このイテレーションで交換が行われなかった場合、再調整を停止

    if DEBUG_MODE: print("性別・レベル均等化のためのチーム再調整が完了しました。")
    return teams

def assign_courts_to_teams_v8(present_members_df, attendance_df_all_logs, target_date, num_teams, include_level1_for_8_10_teams=True):
    if DEBUG_MODE: print(f"\nコート割り振り開始 (v8)... 参加者 {len(present_members_df)} 名、{num_teams} チーム")
    if present_members_df.empty:
        if DEBUG_MODE: print("参加者がいないため、割り振りできません。")
        return {}

    required_cols = [COL_MEMBER_ID, COL_MEMBER_NAME, COL_MEMBER_GRADE, COL_MEMBER_LEVEL, COL_MEMBER_GENDER]
    missing_cols = [col for col in required_cols if col not in present_members_df.columns]
    if missing_cols:
        st.error(f"エラー: 部員リストに必要な列が見つかりません: {missing_cols}")
        print(f"ERROR: Missing required columns in member list: {missing_cols}")
        return {}

    valid_members_df = present_members_df.copy()
    valid_members_df[COL_MEMBER_LEVEL] = pd.to_numeric(valid_members_df[COL_MEMBER_LEVEL], errors='coerce').fillna(0).astype(int)
    valid_members_df[COL_MEMBER_ID] = valid_members_df[COL_MEMBER_ID].astype(str).str.strip()
    valid_members_df[COL_MEMBER_GENDER] = valid_members_df[COL_MEMBER_GENDER].astype(str).str.strip()

    if valid_members_df.empty:
        return {}

    total_members = len(valid_members_df)
    actual_num_teams = min(num_teams, total_members)
    if actual_num_teams <= 0:
        if DEBUG_MODE: print("割り当て可能なチーム数が0です。"); return {}
    if actual_num_teams != num_teams:
        print(f"参加者数 ({total_members}名) に基づき、チーム数を {actual_num_teams} に調整。")

    # 欠席者IDの取得 (割り振り対象から除外するため)
    absent_ids = get_absent_ids_for_date(attendance_df_all_logs, target_date)
    present_members_df = valid_members_df[~valid_members_df[COL_MEMBER_ID].astype(str).isin(absent_ids)].copy()

    # 参加者全体の男女比 (再調整関数に渡すため)
    total_male_present = len(present_members_df[present_members_df[COL_MEMBER_GENDER] == '男性'])
    total_present_members = len(present_members_df)
    target_male_ratio_total = total_male_present / total_present_members if total_present_members > 0 else 0.5
    if DEBUG_MODE: print(f"参加者全体の男性比率: {target_male_ratio_total:.2f}")

    teams = defaultdict(list)
    team_stats = {f"チーム {i+1}": {'count': 0, 'lv6_count': 0, 'lv5_count': 0, 'lv4_count': 0, 'lv1_count': 0, 'male_count': 0, 'female_count': 0, 'late_count': 0, 'lv23_count': 0} for i in range(actual_num_teams)} # Initialize lv23_count

    def assign_member(member_series, target_team_name, is_late=False):
        member_dict = member_series.to_dict()
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
            elif level in [2, 3]: stats['lv23_count'] += 1 # Add this line
        if member_dict.get(COL_MEMBER_GENDER) == '男性':
            stats['male_count'] += 1
        else:
            stats['female_count'] += 1
        if is_late:
            stats['late_count'] += 1

    # 割り振り処理
    def assign_members_by_level(members_df, target_level, team_stats, target_male_ratio):
        assigned_count = 0
        members_to_assign = members_df[members_df[COL_MEMBER_LEVEL] == target_level].to_dict('records')
        random.shuffle(members_to_assign)
        for member_data in members_to_assign:
            member_gender = member_data.get(COL_MEMBER_GENDER)
            is_male = (member_gender == '男性')
            candidate_teams = list(team_stats.keys())
            if candidate_teams:
                if target_level in [6, 5, 4, 1]:
                    # 優先度1: レベルが最も少ないチーム
                    min_level_count = min(team_stats[name][f'lv{target_level}_count'] for name in candidate_teams)
                    candidate_teams_by_level_count = [name for name in candidate_teams if team_stats[name][f'lv{target_level}_count'] == min_level_count]
                    # 優先度2: 人数が最も少ないチーム
                    min_count = min(team_stats[name]['count'] for name in candidate_teams_by_level_count)
                    candidate_teams_by_count = [name for name in candidate_teams_by_level_count if team_stats[name]['count'] == min_count]
                elif target_level in [2, 3]:
                    # 優先度1: 人数が最も少ないチーム (レベル2/3 の場合)
                    min_count = min(team_stats[name]['count'] for name in candidate_teams)
                    candidate_teams_by_count = [name for name in candidate_teams if team_stats[name]['count'] == min_count]
                else:
                    candidate_teams_by_count = candidate_teams

                if not candidate_teams_by_count:
                    target_team_name = random.choice(list(team_stats.keys())) if team_stats else None
                elif len(candidate_teams_by_count) == 1:
                    target_team_name = candidate_teams_by_count[0]
                else:
                    # 優先度2 (または 1): 性別比率が目標に最も近いチーム
                    best_gender_diff = float('inf')
                    next_candidates = []
                    for team_name in candidate_teams_by_count:
                        stats = team_stats[team_name]
                        new_count = stats['count'] + 1
                        new_male_count = stats['male_count'] + (1 if is_male else 0)
                        new_male_ratio = new_male_count / new_count if new_count > 0 else 0.5
                        gender_diff = abs(new_male_ratio - target_male_ratio)
                        if gender_diff < best_gender_diff - 1e-9:
                            best_gender_diff = gender_diff
                            next_candidates = [team_name]
                        elif abs(gender_diff - best_gender_diff) < 1e-9:
                            next_candidates.append(team_name)
                    candidate_teams_by_gender = next_candidates
                    target_team_name = candidate_teams_by_gender[0] if len(candidate_teams_by_gender) == 1 else random.choice(candidate_teams_by_gender)

                if target_team_name:
                    assign_member(pd.Series(member_data), target_team_name)
                    assigned_count += 1
        return assigned_count

    # 遅刻者を先に抽出
    late_member_ids = set() # ここで初期化
    if attendance_df_all_logs is not None and not attendance_df_all_logs.empty:
        temp_df_logs = attendance_df_all_logs.copy()
        if 'dt_timestamp' not in temp_df_logs.columns:
            temp_df_logs['dt_timestamp'] = pd.to_datetime(temp_df_logs[COL_ATTENDANCE_TIMESTAMP], errors='coerce')
        if 'dt_target_date' not in temp_df_logs.columns:
            temp_df_logs['dt_target_date'] = pd.to_datetime(temp_df_logs[COL_ATTENDANCE_TARGET_DATE], errors='coerce').dt.date
        relevant_logs_for_target_date = temp_df_logs[temp_df_logs['dt_target_date'] == target_date].copy()
        if DEBUG_MODE: print(f"割り振り対象日({target_date})の関連ログ数: {len(relevant_logs_for_target_date)}")
        if not relevant_logs_for_target_date.empty:
            latest_logs_for_latecomers_on_target_date = relevant_logs_for_target_date.sort_values(by='dt_timestamp', ascending=False).drop_duplicates(subset=[COL_MEMBER_ID], keep='first')
            for index, row in latest_logs_for_latecomers_on_target_date.iterrows():
                student_id = str(row.get(COL_MEMBER_ID, '')).strip()
                status = str(row.get(COL_ATTENDANCE_STATUS, '')).strip()
                if student_id in valid_members_df[COL_MEMBER_ID].values and status == '遅刻':
                    late_member_ids.add(student_id)
    late_members_df = present_members_df[present_members_df[COL_MEMBER_ID].isin(late_member_ids)].copy()
    regular_members_df = present_members_df[~present_members_df[COL_MEMBER_ID].isin(late_member_ids)].copy()

    # 割り振り処理
    assigned_count = 0
    assigned_count += assign_members_by_level(regular_members_df, 6, team_stats, target_male_ratio_total)
    assigned_count += assign_members_by_level(regular_members_df, 4, team_stats, target_male_ratio_total)
    assigned_count += assign_members_by_level(regular_members_df, 1, team_stats, target_male_ratio_total)
    assigned_count += assign_members_by_level(regular_members_df, 5, team_stats, target_male_ratio_total)

    # 遅刻者の割り当て (レベル順)
    late_level_order = [6, 5, 4, 1] # 遅刻者のレベル割り当て順序
    for level in late_level_order:
        assigned_count += assign_members_by_level(late_members_df, level, team_stats, target_male_ratio_total)

    # 残りのメンバーを割り当て (レベル2, 3)
    other_levels_df = regular_members_df[~regular_members_df[COL_MEMBER_LEVEL].isin([6, 5, 4, 1])].copy()
    assigned_count += assign_members_by_level(other_levels_df, 2, team_stats, target_male_ratio_total)
    assigned_count += assign_members_by_level(other_levels_df, 3, team_stats, target_male_ratio_total)

    # --- 修正箇所: 男女比の偏りを再調整 (遅刻者を除外) ---
    teams = rebalance_teams_by_gender_and_level(teams, team_stats, late_member_ids)
    # --- ここまで修正 ---

    if DEBUG_MODE:
        print(f"\n--- チーム割り振り最終結果 (v8) ---")
        total_assigned = 0
        for team_name in sorted(teams.keys(), key=lambda name: int(name.split()[-1])):
            members_in_team = teams[team_name]
            total_assigned += len(members_in_team)
            member_names = [f"{m.get(COL_MEMBER_NAME, '?')} (L{int(m.get(COL_MEMBER_LEVEL, 0))})" for m in members_in_team]
            stats = team_stats[team_name]
            num_lv6 = stats['lv6_count']
            num_lv5 = stats['lv5_count']
            num_lv4 = stats['lv4_count']
            num_lv1 = stats['lv1_count']
            num_male = stats['male_count']
            num_female = stats['female_count']
            num_late = stats['late_count']
            num_lv23 = stats.get('lv23_count', 0)
            print(f" {team_name} ({len(members_in_team)}名, Lv6:{num_lv6}, Lv5:{num_lv5}, Lv4:{num_lv4}, Lv1:{num_lv1}, Lv2/3:{num_lv23}, 男:{num_male}, 女:{num_female}, 遅刻:{num_late}): {', '.join(member_names)}")
        print("---------------------------------")
        print(f"合計割り当て人数: {total_assigned}")
    return dict(teams)

def format_assignment_results(assignments, practice_type_or_teams, target_date):
    if DEBUG_MODE: print(f"\n割り振り結果 ({practice_type_or_teams} - {target_date.strftime('%Y-%m-%d')}) を整形中...")
    if not assignments: return [[f"割り振り結果なし ({practice_type_or_teams} - {target_date.strftime('%Y-%m-%d')})"]]
    if DEBUG_MODE:
        try:
            if assignments:
                first_team_name = sorted(assignments.keys())[0]
                if assignments[first_team_name]:
                    first_member_data = assignments[first_team_name][0]
                    print("--- DEBUG INFO (format_assignment_results START) ---"); print(f"最初のメンバーのデータ: {first_member_data}"); print(f"そのキー: {list(first_member_data.keys())}"); print(f"コードの設定値: Name='{COL_MEMBER_NAME}', Level='{COL_MEMBER_LEVEL}', Gender='{COL_MEMBER_GENDER}'"); print("--- DEBUG INFO END ---")
                else: print("DEBUG: 最初のチームにメンバーがいません。")
            else: print("DEBUG: assignments辞書が空です。")
        except Exception as e: print(f"DEBUG: 中身確認エラー: {e}")
    output_rows = []; output_rows.append([f"コート割り振り結果 ({practice_type_or_teams} - {target_date.strftime('%Y-%m-%d')})"]); output_rows.append([])
    team_names = sorted(assignments.keys(), key=lambda name: int(name.split()[-1])); output_rows.append(team_names)
    max_len = max(len(m) for m in assignments.values()) if assignments else 0
    for i in range(max_len):
        row = []
        for team_name in team_names:
            members = assignments.get(team_name, []); cell_value = ""
            if i < len(members):
                member = members[i]; name = member.get(COL_MEMBER_NAME, '?'); level_val = member.get(COL_MEMBER_LEVEL, '?'); gender = member.get(COL_MEMBER_GENDER, '?'); cell_value = f"{name} (L{level_val}/{gender})"
            else: cell_value = ""
            row.append(cell_value)
        if DEBUG_MODE and i < 2 : print(f"DEBUG: Completed Row {i+1} for format: {row}")
        output_rows.append(row)
    if DEBUG_MODE: print("-> 整形完了")
    return output_rows

def write_results_to_sheet(worksheet, result_data, data_name="データ"):
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
if 'form_member_options' not in st.session_state: st.session_state.form_member_options = ["---"]
if 'show_success_message' not in st.session_state:
    st.session_state.show_success_message = False
if 'success_message_content' not in st.session_state:
    st.session_state.success_message_content = ""

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
    if st.session_state.general_password_input == GENERAL_PASSWORD_SECRET:
        st.session_state.authentication_status = True
        st.session_state.user_name = "部員"
        st.session_state.last_interaction_time = datetime.datetime.now()
    else:
        st.error("共通パスワードが間違っています。")
        st.session_state.authentication_status = False

# --- 管理者ログイン処理 ---
def check_admin_password():
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
    keys_to_clear = ['authentication_status', 'user_name', 'is_admin', 'last_interaction_time',
                    'form_grade_select_key_cb', 'form_name_select_key_cb', 'form_status_key_cb',
                    'form_reason_key_cb', 'form_late_time_key_cb_active', 'form_target_date_key_cb',
                    'name_to_id_map_form', 'form_member_options',
                    'show_success_message', 'success_message_content']
    for key in list(st.session_state.keys()):
        if key in keys_to_clear:
            del st.session_state[key]
    st.rerun()
    st.stop()

st.session_state.last_interaction_time = datetime.datetime.now()

st.caption(f"現在時刻: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

gspread_client = authenticate_gspread_service_account()
if not gspread_client:
    st.error("スプレッドシートサービスへの接続に失敗しました。")
    st.stop()

if st.session_state.member_df.empty:
    required_member_cols_all = [COL_MEMBER_ID, COL_MEMBER_NAME, COL_MEMBER_GRADE, COL_MEMBER_LEVEL, COL_MEMBER_GENDER]
    st.session_state.member_df = load_data_to_dataframe(gspread_client, SPREADSHEET_ID, MEMBER_SHEET_NAME, required_cols=required_member_cols_all)
    if not st.session_state.member_df.empty:
        try:
            unique_grades_raw = st.session_state.member_df[COL_MEMBER_GRADE].astype(str).str.strip().unique()
            st.session_state.grade_options = ["---"] + sorted([g for g in unique_grades_raw if g])
        except KeyError: st.error(f"エラー: '{COL_MEMBER_GRADE}' 列がありません。"); st.session_state.grade_options = ["---"]
    else:
        st.session_state.grade_options = ["---"]
        st.warning("部員データが空か、読み込みに失敗しました。")

# --- 遅刻・欠席連絡フォーム ---
st.header("１．遅刻・欠席連絡")
if not st.session_state.member_df.empty:
    member_df_for_form = st.session_state.member_df

    def update_name_options_for_form_callback():
        grade = st.session_state.get("form_grade_select_key_cb", "---")
        if DEBUG_MODE: print(f"DEBUG (Callback): Grade changed to: {grade}")
        name_options = ["---"]
        id_map = {}
        if grade != "---" and not member_df_for_form.empty:
            try:
                filtered = member_df_for_form[member_df_for_form[COL_MEMBER_GRADE].astype(str).str.strip() == str(grade).strip()]
                if not filtered.empty:
                    name_options = ["---"] + sorted(filtered[COL_MEMBER_NAME].tolist())
                    id_map = pd.Series(filtered[COL_MEMBER_ID].values, index=filtered[COL_MEMBER_NAME]).to_dict()
            except Exception as e:
                if DEBUG_MODE: print(f"Error in update_name_options_for_form_callback: {e}")
        st.session_state.form_member_options = name_options
        st.session_state.name_to_id_map_form = id_map

    target_date_form = st.date_input("対象の練習日:", value=datetime.date.today(), min_value=datetime.date.today(), key="form_target_date_key_cb")
    col_grade, col_name_form_col = st.columns(2)
    with col_grade:
        selected_grade_form = st.selectbox(
            "あなたの学年:",
            st.session_state.get('grade_options', ["---"]),
            key="form_grade_select_key_cb",
            on_change=update_name_options_for_form_callback
        )
    with col_name_form_col:
        selected_name_display_form = st.selectbox(
            f"あなたの名前 ({selected_grade_form if selected_grade_form != '---' else '学年未選択'}):",
            options=st.session_state.get('form_member_options', ["---"]),
            key="form_name_select_key_cb"
        )

    selected_status_form = st.radio("状態:", ["欠席", "遅刻"], horizontal=True, key="form_status_key_cb")
    reason_form = st.text_area("理由:", placeholder="例: 授業のため", key="form_reason_key_cb")

    late_time_input_placeholder = st.empty()
    late_time_form_val = ""
    if selected_status_form == "遅刻":
        with late_time_input_placeholder.container():
            late_time_form_val = st.text_input("参加可能時刻 (例: 17:30):", key="form_late_time_key_cb_active")

    if st.button("連絡内容を送信する", key="form_submit_button_key_cb"):
        st.session_state.last_interaction_time = datetime.datetime.now()
        grade_to_submit = st.session_state.form_grade_select_key_cb
        name_to_submit = st.session_state.form_name_select_key_cb
        student_id_to_submit = st.session_state.get('name_to_id_map_form', {}).get(name_to_submit)
        status_to_submit = st.session_state.form_status_key_cb
        reason_to_submit = st.session_state.form_reason_key_cb
        late_time_to_submit = st.session_state.get("form_late_time_key_cb_active", "") if status_to_submit == "遅刻" else ""
        target_date_to_submit = st.session_state.form_target_date_key_cb

        errors = [];
        if target_date_to_submit is None: errors.append("練習日を選択");
        if grade_to_submit == "---": errors.append("学年を選択");
        if name_to_submit == "---" or not student_id_to_submit: errors.append("名前を選択");
        if not reason_to_submit: errors.append("理由を入力");
        if status_to_submit == "遅刻" and not late_time_to_submit: errors.append("遅刻時刻を入力");

        if errors: st.warning(f"入力エラー: {', '.join(errors)}してください。")
        else:
            user_email_for_record = f"{student_id_to_submit}@oita-u.ac.jp" if student_id_to_submit else "unknown@example.com"
            record_data = {
                '記録日時': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                '対象練習日': target_date_to_submit.strftime('%Y/%m/%d'),
                '学籍番号': student_id_to_submit, '学年': grade_to_submit, '名前': name_to_submit,
                '状況': status_to_submit, '遅刻・欠席理由': reason_to_submit, '遅刻開始時刻': late_time_to_submit,
                'メールアドレス': user_email_for_record
            }
            final_record_data = {col: record_data.get(col, "") for col in OUTPUT_COLUMNS_ORDER}
            attendance_ws = get_worksheet_safe(gspread_client, SPREADSHEET_ID, ATTENDANCE_SHEET_NAME)
            success = record_attendance_streamlit(attendance_ws, final_record_data)
            if success:
                st.session_state.success_message_content = f"{target_date_to_submit.strftime('%Y/%m/%d')} の {grade_to_submit} {name_to_submit} さん ({student_id_to_submit}) の {status_to_submit} 連絡を受け付けました。"
                st.session_state.show_success_message = True
                st.rerun()
            else: st.error("記録に失敗しました。")
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

def update_name_options_for_lookup_callback():
    grade = st.session_state.get("lookup_grade_select_key", "---")
    if DEBUG_MODE: print(f"DEBUG (Lookup Callback): Grade changed to: {grade}")
    name_options = ["---"]
    id_map = {}
    if grade != "---" and not st.session_state.member_df.empty:
        try:
            filtered = st.session_state.member_df[st.session_state.member_df[COL_MEMBER_GRADE].astype(str).str.strip() == str(grade).strip()]
            if not filtered.empty:
                name_options = ["---"] + sorted(filtered[COL_MEMBER_NAME].tolist())
                id_map = pd.Series(filtered[COL_MEMBER_ID].values, index=filtered[COL_MEMBER_NAME]).to_dict()
        except Exception as e:
            if DEBUG_MODE: print(f"Error in update_name_options_for_lookup_callback: {e}")
    st.session_state.lookup_member_options = name_options
    st.session_state.name_to_id_map_lookup = id_map

if st.session_state.authentication_status is True:
    if not st.session_state.member_df.empty:
        member_df_for_lookup = st.session_state.member_df

        col_grade_lookup, col_name_lookup = st.columns(2)
        with col_grade_lookup:
            selected_grade_lookup = st.selectbox(
                "あなたの学年:",
                st.session_state.get('grade_options', ["---"]),
                key="lookup_grade_select_key",
                on_change=update_name_options_for_lookup_callback
            )
        with col_name_lookup:
            selected_name_lookup = st.selectbox(
                f"あなたの名前 ({selected_grade_lookup if selected_grade_lookup != '---' else '学年未選択'}):",
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
                            st.info(f"{name_to_lookup} さん ({student_id_to_lookup}) の過去の連絡記録は見つかりませんでした。")
                        else:
                            user_records_df = user_records_df.sort_values(by=COL_ATTENDANCE_TIMESTAMP, ascending=False)
                            # 変更点: 表示する列を LOOKUP_DISPLAY_COLUMNS に限定
                            valid_display_cols = [col for col in LOOKUP_DISPLAY_COLUMNS if col in user_records_df.columns]

                            if valid_display_cols:
                                st.subheader(f"{name_to_lookup} さんの過去の連絡記録")
                                st.dataframe(user_records_df[valid_display_cols])
                            else:
                                st.warning("表示できる連絡記録の列が見つかりません。")
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
        include_level1 = st.radio(
            "8チーム/10チーム割り振りにレベル1を含めますか？",
            options=["含める", "含めない"],
            key="include_level1_assign_radio",
            horizontal=True
        )

        if st.button("コート割り振りを実行して結果シートを更新", key="assign_button_admin_main"):
            st.session_state.last_interaction_time = datetime.datetime.now()
            with st.spinner(f"{target_date_assign_input.strftime('%Y-%m-%d')} のコート割り振り中..."):
                attendance_df_all_logs = load_data_to_dataframe(gspread_client, SPREADSHEET_ID, ATTENDANCE_SHEET_NAME, required_cols=None)
                if DEBUG_MODE: st.write(f"割り振り対象日: {target_date_assign_input}")

                # 欠席者IDの取得 (「欠席」ステータスの人のみ)
                absent_ids = get_absent_ids_for_date(attendance_df_all_logs, target_date_assign_input)

                member_df_assign = st.session_state.member_df
                present_members_df = member_df_assign[~member_df_assign[COL_MEMBER_ID].astype(str).isin(absent_ids)].copy()
                if DEBUG_MODE: st.write(f"参加予定者総数 (遅刻者含む): {len(present_members_df)} 名")

                absent_members_df = member_df_assign[member_df_assign[COL_MEMBER_ID].astype(str).isin(absent_ids)].copy()
                if DEBUG_MODE: st.write(f"欠席連絡者: {len(absent_members_df)} 名")

                # 遅刻者リストの抽出と出力
                late_members_df = pd.DataFrame()
                late_ids_for_sheet = set() # 遅刻者のIDを格納するセット
                if attendance_df_all_logs is not None and not attendance_df_all_logs.empty:
                    # 対象練習日のログに絞り込み、最新の連絡のみを考慮
                    temp_df_logs_late = attendance_df_all_logs.copy()
                    if 'dt_timestamp' not in temp_df_logs_late.columns:
                        temp_df_logs_late['dt_timestamp'] = pd.to_datetime(temp_df_logs_late[COL_ATTENDANCE_TIMESTAMP], errors='coerce')
                    if 'dt_target_date' not in temp_df_logs_late.columns:
                        temp_df_logs_late['dt_target_date'] = pd.to_datetime(temp_df_logs_late[COL_ATTENDANCE_TARGET_DATE], errors='coerce').dt.date

                    relevant_logs_for_late = temp_df_logs_late[temp_df_logs_late['dt_target_date'] == target_date_assign_input].copy()
                    if not relevant_logs_for_late.empty:
                        latest_logs_for_late = relevant_logs_for_late.sort_values(by='dt_timestamp', ascending=False).drop_duplicates(subset=[COL_MEMBER_ID], keep='first')

                        for index, row in latest_logs_for_late.iterrows():
                            student_id = str(row.get(COL_MEMBER_ID, '')).strip()
                            status = str(row.get(COL_ATTENDANCE_STATUS, '')).strip()
                            if student_id and status == '遅刻':
                                late_ids_for_sheet.add(student_id) # 遅刻者IDをセットに追加

                        # 部員リストから遅刻者情報を取得
                        late_members_df = member_df_assign[member_df_assign[COL_MEMBER_ID].astype(str).isin(late_ids_for_sheet)].copy()

                        # 遅刻連絡の理由と時刻を追加
                        late_members_df = pd.merge(late_members_df, latest_logs_for_late[[COL_MEMBER_ID, COL_ATTENDANCE_LATE_TIME, COL_ATTENDANCE_REASON]],
                                                on=COL_MEMBER_ID, how='left')

                if DEBUG_MODE: st.write(f"遅刻連絡者: {len(late_members_df)} 名")

                # 参加者名簿の出力
                participant_ws = get_worksheet_safe(gspread_client, SPREADSHEET_ID, PARTICIPANT_LIST_SHEET_NAME)
                if participant_ws:
                    if DEBUG_MODE: st.write(f"参加者名簿 ({target_date_assign_input}) を出力...")
                    if not present_members_df.empty:
                        output_cols_p = [COL_MEMBER_ID, COL_MEMBER_NAME, COL_MEMBER_GRADE, COL_MEMBER_LEVEL, COL_MEMBER_GENDER]
                        valid_output_cols_p = [col for col in output_cols_p if col in present_members_df.columns]
                        participant_list_output = [[f"{target_date_assign_input.strftime('%Y-%m-%d')} 参加者リスト"]]
                        participant_list_output.append(valid_output_cols_p); participant_list_output.extend(present_members_df[valid_output_cols_p].values.tolist())
                        write_results_to_sheet(participant_ws, participant_list_output, data_name=f"{target_date_assign_input.strftime('%Y-%m-%d')} 参加者名簿")
                    else: write_results_to_sheet(participant_ws, [[f"{target_date_assign_input.strftime('%Y-%m-%d')} の参加者なし"]], data_name="参加者名簿")
                else: st.error(f"シート '{PARTICIPANT_LIST_SHEET_NAME}' が見つかりません。")

                # 欠席者名簿の出力
                absent_ws = get_worksheet_safe(gspread_client, SPREADSHEET_ID, ABSENT_LIST_SHEET_NAME)
                if absent_ws:
                    if DEBUG_MODE: st.write(f"欠席者名簿 ({target_date_assign_input}) を出力...")
                    if not absent_members_df.empty:
                        absent_output_cols = [COL_MEMBER_ID, COL_MEMBER_NAME, COL_MEMBER_GRADE, COL_ATTENDANCE_REASON] # 理由も表示
                        valid_absent_cols = [col for col in absent_members_df.columns if col in absent_output_cols] # 存在する列のみ
                        absent_list_output = [[f"{target_date_assign_input.strftime('%Y-%m-%d')} 欠席者リスト"]]
                        absent_list_output.append(valid_absent_cols); absent_list_output.extend(absent_members_df[valid_absent_cols].values.tolist())
                        write_results_to_sheet(absent_ws, absent_list_output, data_name=f"{target_date_assign_input.strftime('%Y-%m-%d')} 欠席者名簿")
                    else: absent_list_output = [[f"{target_date_assign_input.strftime('%Y-%m-%d')} の欠席連絡者なし"]]
                    write_results_to_sheet(absent_ws, absent_list_output, data_name=f"欠席者名簿")
                else: st.error(f"シート '{ABSENT_LIST_SHEET_NAME}' が見つかりません。")

                # 遅刻者名簿の出力
                late_ws = get_worksheet_safe(gspread_client, SPREADSHEET_ID, LATE_LIST_SHEET_NAME)
                if late_ws:
                    if DEBUG_MODE: st.write(f"遅刻者名簿 ({target_date_assign_input}) を出力...")
                    if not late_members_df.empty:
                        late_output_cols = [COL_MEMBER_ID, COL_MEMBER_NAME, COL_MEMBER_GRADE, COL_ATTENDANCE_LATE_TIME, COL_ATTENDANCE_REASON]
                        valid_late_cols = [col for col in late_output_cols if col in late_members_df.columns]
                        late_list_output = [[f"{target_date_assign_input.strftime('%Y-%m-%d')} 遅刻者リスト"]]
                        late_list_output.append(valid_late_cols); late_list_output.extend(late_members_df[valid_late_cols].values.tolist())
                        write_results_to_sheet(late_ws, late_list_output, data_name=f"{target_date_assign_input.strftime('%Y-%m-%d')} 遅刻者名簿")
                    else: late_list_output = [[f"{target_date_assign_input.strftime('%Y-%m-%d')} の遅刻連絡者なし"]]
                    write_results_to_sheet(late_ws, late_list_output, data_name=f"遅刻者名簿")
                else: st.error(f"シート '{LATE_LIST_SHEET_NAME}' が見つかりません。")

                if present_members_df.empty: st.warning("参加予定者がいないため、コート割り振りは行いません。")
                else:
                    present_members_for_8_10 = present_members_df.copy()
                    if st.session_state.include_level1_assign_radio == "含めない":
                        present_members_for_8_10 = present_members_df[present_members_df[COL_MEMBER_LEVEL] != 1].copy()
                    if DEBUG_MODE: st.write(f"8/10チーム割り振り対象者 (レベル1除く): {len(present_members_for_8_10)} 名")

                    num_teams_8 = TEAMS_COUNT_MAP.get('ノック', 8); num_teams_10 = TEAMS_COUNT_MAP.get('ハンドノック', 10); num_teams_12 = TEAMS_COUNT_MAP.get('その他', 12)
                    assignment_ws_8 = get_worksheet_safe(gspread_client, SPREADSHEET_ID, ASSIGNMENT_SHEET_NAME_8)
                    if assignment_ws_8:
                        if DEBUG_MODE: st.write("--- 8チーム割り振りを実行中 (v8) ---")
                        assignments_8 = assign_courts_to_teams_v8(present_members_for_8_10, attendance_df_all_logs, target_date_assign_input, num_teams_8, st.session_state.include_level1_assign_radio == "含める")
                        if assignments_8: result_output_8 = format_assignment_results(assignments_8, "8チーム", target_date_assign_input); write_results_to_sheet(assignment_ws_8, result_output_8, f"8チーム結果({target_date_assign_input.strftime('%Y-%m-%d')})")
                        else: st.warning("8チーム割り振り結果なし。")
                    else: st.error(f"シート '{ASSIGNMENT_SHEET_NAME_8}' が見つかりません。")
                    assignment_ws_10 = get_worksheet_safe(gspread_client, SPREADSHEET_ID, ASSIGNMENT_SHEET_NAME_10)
                    if assignment_ws_10:
                        if DEBUG_MODE: st.write("--- 10チーム割り振りを実行中 (v8) ---")
                        assignments_10 = assign_courts_to_teams_v8(present_members_for_8_10, attendance_df_all_logs, target_date_assign_input, num_teams_10, st.session_state.include_level1_assign_radio == "含める")
                        if assignments_10: result_output_10 = format_assignment_results(assignments_10, "10チーム", target_date_assign_input); write_results_to_sheet(assignment_ws_10, result_output_10, f"10チーム結果({target_date_assign_input.strftime('%Y-%m-%d')})")
                        else: st.warning("10チーム割り振り結果なし。")
                    else: st.error(f"シート '{ASSIGNMENT_SHEET_NAME_10}' が見つかりません。")

                    assignment_ws_12 = get_worksheet_safe(gspread_client, SPREADSHEET_ID, ASSIGNMENT_SHEET_NAME_12)
                    if assignment_ws_12:
                        if DEBUG_MODE: st.write("--- 12チーム割り振りを実行中 (v8) ---")
                        assignments_12 = assign_courts_to_teams_v8(present_members_df, attendance_df_all_logs, target_date_assign_input, num_teams_12, True)
                        if assignments_12: result_output_12 = format_assignment_results(assignments_12, "12チーム", target_date_assign_input); write_results_to_sheet(assignment_ws_12, result_output_12, f"12チーム結果({target_date_assign_input.strftime('%Y-%m-%d')})")
                        else: st.warning("12チーム割り振り結果なし。")
                    else: st.error(f"シート '{ASSIGNMENT_SHEET_NAME_12}' が見つかりません。")
                st.info(f"{target_date_assign_input.strftime('%Y-%m-%d')} の割り振り処理と名簿出力が完了しました。")
    else:
        st.info("コート割り振り実行には部員データが必要です。")
elif st.session_state.authentication_status is True and not st.session_state.is_admin:
    st.info("コート割り振り機能は管理者専用です。")
st.caption("システム管理者向けエリア")
