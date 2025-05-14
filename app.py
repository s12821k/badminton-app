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
import math
import os

# === Streamlit のページ設定 (一番最初に呼び出す) ===
st.set_page_config(page_title="バド部 連絡システム", layout="centered")

# === 2. 設定値 (st.secrets からも読み込む) ===
try:
    APP_PASSWORDS_SECRETS = st.secrets.get('app_passwords', {})
    GENERAL_PASSWORD_SECRET = APP_PASSWORDS_SECRETS.get("general_password")
    ADMIN_PASSWORD_SECRET = APP_PASSWORDS_SECRETS.get("admin_password")

    APP_CONFIG = st.secrets.get('app_config', {})
    DEBUG_MODE = APP_CONFIG.get("debug_mode", False)

    if not GENERAL_PASSWORD_SECRET or not ADMIN_PASSWORD_SECRET:
        st.error("重大なエラー: secrets.toml の [app_passwords] に general_password または admin_password が設定されていません。")
        st.stop()
except Exception as e:
    st.error(f"重大なエラー: secrets.toml の読み込みまたは必須設定の取得中に問題が発生しました: {e}")
    DEBUG_MODE = False # フォールバック
    st.stop()

# --- サービスアカウント認証情報 (スプレッドシート操作用) ---
CREDENTIALS_JSON_PATH = 'your_credentials.json' # ★あなたのサービスアカウント認証情報ファイル
SCOPES_GSPREAD = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- スプレッドシート情報 ---
SPREADSHEET_ID = '1jCCxSeECR7NZpCEXwZCDmW_NjcoEzBPg8wqM-IGyIS8' # ★あなたのスプレッドシートID
MEMBER_SHEET_NAME = '部員リスト'
ATTENDANCE_SHEET_NAME = '遅刻欠席連絡'
PARTICIPANT_LIST_SHEET_NAME = '参加者名簿'
ABSENT_LIST_SHEET_NAME = '欠席者名簿'
ASSIGNMENT_SHEET_NAME_8 = '割り振り結果_8チーム'
ASSIGNMENT_SHEET_NAME_12 = '割り振り結果_12チーム'

# --- 列名 (ヘッダー名) ---
COL_MEMBER_ID = '学籍番号'; COL_MEMBER_NAME = '名前'; COL_MEMBER_GRADE = '学年';
COL_MEMBER_LEVEL = 'レベル'; COL_MEMBER_GENDER = '性別';
COL_ATTENDANCE_TIMESTAMP = 'タイムスタンプ';
# COL_ATTENDANCE_EMAIL = 'メールアドレス'; # 共通パスワード方式ではフォームからメールは取得しない
COL_ATTENDANCE_TARGET_DATE = '日付を選択してください';
COL_ATTENDANCE_STATUS = '状況';
COL_ATTENDANCE_LATE_TIME = '遅刻の場合';
COL_ATTENDANCE_REASON = '遅刻・欠席理由';
OUTPUT_COLUMNS_ORDER = ['記録日時', '対象練習日', '学籍番号', '学年', '名前', '状況', '遅刻・欠席理由', '遅刻開始時刻']

# --- コート割り振り設定 ---
DEFAULT_PRACTICE_TYPE = 'ノック'; TEAMS_COUNT_MAP = {'ノック': 8, 'ハンドノック': 12}
INACTIVITY_TIMEOUT_MINUTES = 10 # 非アクティブタイムアウト時間（分）

# === 3. 関数定義 ===
@st.cache_resource
def authenticate_gspread_service_account():
    if DEBUG_MODE: print("Attempting gspread Service Account Authentication...")
    try:
        creds = Credentials.from_service_account_file(CREDENTIALS_JSON_PATH, scopes=SCOPES_GSPREAD)
        client = gspread.authorize(creds)
        if DEBUG_MODE: print("gspread Service Account Authentication successful.")
        return client
    except FileNotFoundError: st.error(f"認証エラー(SA): {CREDENTIALS_JSON_PATH}なし"); print(f"ERROR: SA Credentials file not found: {CREDENTIALS_JSON_PATH}"); return None
    except Exception as e: st.error(f"認証エラー(SA): {e}"); print(f"ERROR: SA Authentication error: {e}"); return None

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
            if COL_ATTENDANCE_TIMESTAMP in df.columns: df[COL_ATTENDANCE_TIMESTAMP] = pd.to_datetime(df[COL_ATTENDANCE_TIMESTAMP], errors='coerce')
            if COL_ATTENDANCE_TARGET_DATE in df.columns: df[COL_ATTENDANCE_TARGET_DATE] = pd.to_datetime(df[COL_ATTENDANCE_TARGET_DATE], errors='coerce').dt.date
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
    if DEBUG_MODE: print(f"\n{target_date} の不参加者リストを作成します...")
    absent_ids = set()
    # 遅刻欠席連絡シートに必要な列名 (学籍番号が直接記録されている前提)
    required_attendance_cols = [COL_ATTENDANCE_TIMESTAMP, COL_MEMBER_ID, COL_ATTENDANCE_TARGET_DATE, COL_ATTENDANCE_STATUS]
    if not all(col in attendance_df.columns for col in required_attendance_cols):
        missing = [col for col in required_attendance_cols if col not in attendance_df.columns];
        st.warning(f"警告: 遅刻欠席ログに必要な列が見つかりません: {missing}。"); print(f"WARNING: Missing cols in attendance log: {missing}."); return absent_ids
    try:
        df = attendance_df.copy()
        if 'dt_timestamp' not in df.columns or df['dt_timestamp'].isnull().all(): df['dt_timestamp'] = pd.to_datetime(df[COL_ATTENDANCE_TIMESTAMP], errors='coerce')
        if 'dt_target_date' not in df.columns or df['dt_target_date'].isnull().all(): df['dt_target_date'] = pd.to_datetime(df[COL_ATTENDANCE_TARGET_DATE], errors='coerce').dt.date
        df.dropna(subset=['dt_timestamp', 'dt_target_date'], inplace=True)
        df[COL_MEMBER_ID] = df[COL_MEMBER_ID].astype(str).str.strip()
        df.dropna(subset=[COL_MEMBER_ID], inplace=True); df = df[df[COL_MEMBER_ID] != '']
        if df.empty:
             if DEBUG_MODE: print(f"{target_date} に関する有効な形式の連絡はありませんでした。"); return absent_ids
        target_datetime_end = datetime.datetime.combine(target_date, datetime.time.max); relevant_logs = df[(df['dt_target_date'] == target_date) & (df['dt_timestamp'] <= target_datetime_end)]
        if relevant_logs.empty:
            if DEBUG_MODE: print(f"{target_date} に該当する連絡はありませんでした。"); return absent_ids
        latest_logs = relevant_logs.sort_values(by='dt_timestamp', ascending=False).drop_duplicates(subset=[COL_MEMBER_ID], keep='first')
        for index, row in latest_logs.iterrows():
            status = str(row.get(COL_ATTENDANCE_STATUS, '')).strip(); student_id = str(row.get(COL_MEMBER_ID, '')).strip()
            if student_id and status in ['欠席', '遅刻']: absent_ids.add(student_id)
        if DEBUG_MODE: print(f"{target_date} の不参加者 ({len(absent_ids)}名) 特定完了。")
    except KeyError as e: st.error(f"エラー(欠席者特定): 列名 '{e}' 不明"); print(f"KeyError(get_absent_ids): {e}")
    except Exception as e: st.error(f"不参加者特定中にエラー: {e}"); print(f"Error(get_absent_ids): {e}")
    return absent_ids

def assign_courts_to_teams_v7(present_members_df, num_teams):
    # (この関数の内容は変更なし - SyntaxError修正済み)
    if DEBUG_MODE: print(f"\nコート割り振り開始 (v7)... 参加者 {len(present_members_df)} 名、{num_teams} チーム")
    if present_members_df.empty:
        if DEBUG_MODE: print("参加者がいないため、割り振りできません。")
        return {}
    required_cols = [COL_MEMBER_ID, COL_MEMBER_NAME, COL_MEMBER_GRADE, COL_MEMBER_LEVEL, COL_MEMBER_GENDER]
    missing_cols = [col for col in required_cols if col not in present_members_df.columns]
    if missing_cols:
        st.error(f"エラー: 部員リストに必要な列が見つかりません: {missing_cols}")
        print(f"ERROR: Missing required columns in member list: {missing_cols}")
        return {}
    valid_members_df = present_members_df.copy(); valid_members_df[COL_MEMBER_LEVEL]=pd.to_numeric(valid_members_df[COL_MEMBER_LEVEL], errors='coerce'); valid_members_df[COL_MEMBER_ID]=valid_members_df[COL_MEMBER_ID].astype(str); valid_members_df[COL_MEMBER_GENDER]=valid_members_df[COL_MEMBER_GENDER].astype(str)
    if valid_members_df.empty: return {}
    total_members = len(valid_members_df); total_males = len(valid_members_df[valid_members_df[COL_MEMBER_GENDER] == '男性']); target_male_ratio = total_males / total_members if total_members > 0 else 0.5;
    if DEBUG_MODE: print(f"参加者の男性比率(目標): {target_male_ratio:.2f}")
    level6_df = valid_members_df[valid_members_df[COL_MEMBER_LEVEL] == 6]; level1_df = valid_members_df[valid_members_df[COL_MEMBER_LEVEL] == 1]; level5_df = valid_members_df[valid_members_df[COL_MEMBER_LEVEL] == 5]; remaining_levels = [2, 3, 4]
    others_df = valid_members_df[valid_members_df[COL_MEMBER_LEVEL].isin(remaining_levels) | valid_members_df[COL_MEMBER_LEVEL].isna()]
    if DEBUG_MODE: print(f"参加者内訳: Lv6={len(level6_df)}, Lv5={len(level5_df)}, Lv1={len(level1_df)}, その他={len(others_df)}")
    actual_num_teams = min(num_teams, total_members);
    if actual_num_teams <= 0:
        if DEBUG_MODE: print("割り当て可能なチーム数が0です。"); return {}
    if actual_num_teams != num_teams: print(f"参加者数に基づき、チーム数を {actual_num_teams} に調整。")
    teams = defaultdict(list); team_stats = {f"チーム {i+1}": {'count': 0, 'lv6_count': 0, 'lv5_count': 0, 'lv1_count': 0, 'male_count': 0, 'female_count': 0} for i in range(actual_num_teams)}
    def assign_member(member_dict, target_team_name):
        teams[target_team_name].append(member_dict); stats = team_stats[target_team_name]; stats['count'] += 1; level = member_dict.get(COL_MEMBER_LEVEL)
        if pd.notna(level):
            level = int(level)
            if level == 6: stats['lv6_count'] += 1
            elif level == 5: stats['lv5_count'] += 1
            elif level == 1: stats['lv1_count'] += 1
        if member_dict.get(COL_MEMBER_GENDER) == '男性': stats['male_count'] += 1
        else: stats['female_count'] += 1
    if DEBUG_MODE: print("ステップ1: レベル6 (ノッカー) を割り振り中...")
    level6_list = level6_df.sample(frac=1).to_dict('records')
    for i, member_data in enumerate(level6_list): team_id_num = (i % actual_num_teams) + 1; assign_member(member_data, f"チーム {team_id_num}")
    if DEBUG_MODE: print("-> レベル6 割り振り完了。")
    if DEBUG_MODE: print("ステップ2: レベル1 (初心者) を割り振り中...")
    level1_list = level1_df.sample(frac=1).to_dict('records')
    for member_data in level1_list: target_team_name = sorted(team_stats.keys(), key=lambda n: (team_stats[n]['lv1_count'], team_stats[n]['count'], int(n.split()[-1])))[0]; assign_member(member_data, target_team_name)
    if DEBUG_MODE: print("-> レベル1 割り振り完了。")
    if DEBUG_MODE: print("ステップ3: レベル5 を割り振り中...")
    level5_list = level5_df.sample(frac=1).to_dict('records')
    for member_data in level5_list: target_team_name = sorted(team_stats.keys(), key=lambda n: (team_stats[n]['lv6_count'] + team_stats[n]['lv5_count'], team_stats[n]['count'], int(n.split()[-1])))[0]; assign_member(member_data, target_team_name)
    if DEBUG_MODE: print("-> レベル5 割り振り完了。")
    if DEBUG_MODE: print("ステップ4: 残りメンバー (Lv2-4, 不明) を割り振り中...")
    others_list = others_df.sort_values(by=COL_MEMBER_LEVEL, ascending=False, na_position='last').to_dict('records'); random.shuffle(others_list)
    for member_data in others_list:
        member_gender = member_data.get(COL_MEMBER_GENDER); is_male = (member_gender == '男性')
        candidate_teams = list(team_stats.keys())
        if candidate_teams: min_count = min(team_stats[name]['count'] for name in candidate_teams); candidate_teams = [name for name in candidate_teams if team_stats[name]['count'] == min_count]
        if len(candidate_teams) > 1:
            best_gender_diff = float('inf'); next_candidates = []
            for team_name in candidate_teams:
                stats = team_stats[team_name]; new_count = stats['count'] + 1; new_male_count = stats['male_count'] + (1 if is_male else 0); new_male_ratio = new_male_count / new_count if new_count > 0 else 0.5; gender_diff = abs(new_male_ratio - target_male_ratio)
                if gender_diff < best_gender_diff - 1e-9: best_gender_diff = gender_diff; next_candidates = [team_name]
                elif abs(gender_diff - best_gender_diff) < 1e-9: next_candidates.append(team_name)
            candidate_teams = next_candidates
        if len(candidate_teams) > 1: candidate_teams.sort(key=lambda name: int(name.split()[-1]))
        target_team_name = None
        if candidate_teams: target_team_name = candidate_teams[0]
        else: print(f"警告: {member_data.get(COL_MEMBER_NAME, '?')} の割当先候補なし。"); target_team_name = random.choice(list(team_stats.keys())) if team_stats else None
        if target_team_name: assign_member(member_data, target_team_name)
        else: print(f"致命的エラー: {member_data.get(COL_MEMBER_NAME, '?')} を割り当てるチームがありません。")
    if DEBUG_MODE: print("-> 残りメンバー割り振り完了。")
    if DEBUG_MODE:
        print(f"\n--- チーム割り振り最終結果 (v7) ---")
        total_assigned = 0
        for team_name in sorted(teams.keys(), key=lambda name: int(name.split()[-1])):
            members_in_team = teams[team_name]; total_assigned += len(members_in_team); member_names = [m.get(COL_MEMBER_NAME, '?') for m in members_in_team]; stats = team_stats[team_name]; num_lv6 = stats['lv6_count']; num_lv5 = stats['lv5_count']; num_lv1 = stats['lv1_count']; num_male = stats['male_count']; num_female = stats['female_count']
            print(f"  {team_name} ({len(members_in_team)}名, Lv6:{num_lv6}, Lv5:{num_lv5}, Lv1:{num_lv1}, 男:{num_male}, 女:{num_female}): {', '.join(member_names)}")
        print("---------------------------------"); print(f"合計割り当て人数: {total_assigned}")
    return dict(teams)

def format_assignment_results(assignments, practice_type_or_teams, target_date):
    # (この関数の内容は変更なし)
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
                member = members[i]; name = member.get(COL_MEMBER_NAME, '?'); level_val = member.get(COL_MEMBER_LEVEL, '?'); gender = member.get(COL_MEMBER_GENDER, '?'); level = int(level_val) if pd.notna(level_val) and isinstance(level_val, (int, float, str)) and str(level_val).isdigit() else '?'; cell_value = f"{name} (L{level}/{gender})"
            else: cell_value = ""
            row.append(cell_value)
        if DEBUG_MODE and i < 2 : print(f"DEBUG: Completed Row {i+1} for format: {row}")
        output_rows.append(row)
    if DEBUG_MODE: print("-> 整形完了")
    return output_rows

def write_results_to_sheet(worksheet, result_data, data_name="データ"):
    # (この関数の内容は変更なし)
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
# if 'form_selected_grade' not in st.session_state: st.session_state.form_selected_grade = "---" # コールバック内で管理
if 'form_member_options' not in st.session_state: st.session_state.form_member_options = ["---"]
# 通知表示用のセッションステートを追加
if 'show_success_message' not in st.session_state: # toastからmessageに変更
    st.session_state.show_success_message = False
if 'success_message_content' not in st.session_state: # toast_messageからmessage_contentに変更
    st.session_state.success_message_content = ""


# ログイン状態を管理するセッション変数 (共通パスワード方式)
if 'authentication_status' not in st.session_state:
    st.session_state.authentication_status = None # None:未試行, True:成功, False:失敗
if 'user_name' not in st.session_state: # 一般ログインユーザー名
    st.session_state.user_name = None
if 'is_admin' not in st.session_state: # 管理者かどうか
    st.session_state.is_admin = False
if 'last_interaction_time' not in st.session_state: # 自動ログアウト用
    st.session_state.last_interaction_time = datetime.datetime.now()


# --- 一般ログイン処理 ---
def check_general_password():
    # st.session_state.general_password_input は st.text_input の key で自動的に設定される
    if st.session_state.general_password_input == GENERAL_PASSWORD_SECRET:
        st.session_state.authentication_status = True
        st.session_state.user_name = "部員" # 固定の表示名
        st.session_state.last_interaction_time = datetime.datetime.now() # ログイン時刻を記録
    else:
        st.error("共通パスワードが間違っています。")
        st.session_state.authentication_status = False # ログイン失敗を記録

# --- 管理者ログイン処理 ---
def check_admin_password():
    # st.session_state.admin_password_input_key は st.text_input の key で自動的に設定される
    if st.session_state.admin_password_input_key == ADMIN_PASSWORD_SECRET:
        st.session_state.is_admin = True
        st.session_state.last_interaction_time = datetime.datetime.now() # 操作時刻を更新
        st.success("管理者として認証されました。")
    else:
        st.error("管理者パスワードが間違っています。")
        st.session_state.is_admin = False


# --- ログインフォームの表示 ---
if st.session_state.authentication_status is not True:
    st.subheader("アプリ利用のための共通パスワードを入力してください")
    # st.form を使わずに直接パスワード入力とボタンを配置
    st.text_input("共通パスワード", type="password", key="general_password_input", on_change=check_general_password)
    # ボタンは押された時だけ処理するので、on_change は text_input につける
    # if st.button("ログイン", key="general_login_button"):
    #     check_general_password() # ボタンが押されたらパスワードチェック
    #     if st.session_state.authentication_status: st.rerun()
    st.stop() # ログイン前はここまで

# --- メインコンテンツ (一般ログイン済みユーザー向け) ---

# 自動ログアウトチェック
if datetime.datetime.now() - st.session_state.last_interaction_time > datetime.timedelta(minutes=INACTIVITY_TIMEOUT_MINUTES):
    st.warning(f"{INACTIVITY_TIMEOUT_MINUTES}分間操作がなかったため、自動的にログアウトしました。再度ログインしてください。")
    # セッション状態をクリアして最初のログイン画面に戻る
    keys_to_clear = ['authentication_status', 'user_name', 'is_admin', 'last_interaction_time',
                     'form_grade_select_key_cb', 'form_name_select_key_cb', 'form_status_key_cb',
                     'form_reason_key_cb', 'form_late_time_key_cb_active', 'form_target_date_key_cb', # form_late_time_key_cb_active に変更
                     'name_to_id_map_form', 'form_member_options',
                     'show_success_message', 'success_message_content'] # 通知関連のキーもクリア
    for key in keys_to_clear:
        if key in st.session_state:
            del st.session_state[key]
    st.rerun()
    st.stop()

# ユーザーが何か操作をするたびに最終操作時刻を更新する (簡易版)
# より正確には、各インタラクティブなウィジェットのon_changeやボタンのコールバックで更新する
st.session_state.last_interaction_time = datetime.datetime.now()


# ログアウトボタン (サイドバーに表示) - 一般ユーザーログイン中は表示しないのでコメントアウト
# if st.sidebar.button("ログアウト", key="main_logout_button"):
#     for key in list(st.session_state.keys()):
#         if key not in ['member_df', 'grade_options']: # 残したいものを指定
#             del st.session_state[key]
#     st.session_state.authentication_status = None
#     st.session_state.is_admin = False
#     st.rerun()

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

    # コールバック関数
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
        # 学年変更時に名前の選択をリセット (session_stateを直接変更しない)
        # st.session_state.form_name_select_key_cb = st.session_state.form_member_options[0] # この行は削除またはコメントアウト


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
            options=st.session_state.get('form_member_options', ["---"]), # セッションから取得
            key="form_name_select_key_cb"
        )

    selected_status_form = st.radio("状態:", ["欠席", "遅刻"], horizontal=True, key="form_status_key_cb")
    reason_form = st.text_area("理由:", placeholder="例: 授業のため", key="form_reason_key_cb")

    late_time_input_placeholder = st.empty() # 遅刻時間入力欄のプレースホルダー
    late_time_form_val = "" # 値を保持する変数
    if selected_status_form == "遅刻":
        with late_time_input_placeholder.container():
             late_time_form_val = st.text_input("参加可能時刻 (例: 17:30):", key="form_late_time_key_cb_active") # キーをアクティブ時のみに

    if st.button("連絡内容を送信する", key="form_submit_button_key_cb"):
        st.session_state.last_interaction_time = datetime.datetime.now()
        grade_to_submit = st.session_state.form_grade_select_key_cb
        name_to_submit = st.session_state.form_name_select_key_cb
        student_id_to_submit = st.session_state.get('name_to_id_map_form', {}).get(name_to_submit)
        status_to_submit = st.session_state.form_status_key_cb
        reason_to_submit = st.session_state.form_reason_key_cb
        # 遅刻時間は、遅刻が選択されている場合のみセッション状態から取得
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
                # 成功メッセージの内容をセッションステートに保存し、表示フラグを立てる
                st.session_state.success_message_content = f"{target_date_to_submit.strftime('%Y/%m/%d')} の {grade_to_submit} {name_to_submit} さん ({student_id_to_submit}) の {status_to_submit} 連絡を受け付けました。"
                st.session_state.show_success_message = True
                st.rerun() # 再描画によってウィジェットが初期値に戻る
            else: st.error("記録に失敗しました。")
else:
    st.warning("部員データを読み込めないため連絡フォームを表示できません。")
st.caption("連絡フォーム終了")

# --- 成功メッセージ表示の処理 ---
# show_success_message フラグが True ならメッセージを表示し、フラグを False に戻す
if st.session_state.get('show_success_message', False):
    st.success(st.session_state.success_message_content)
    st.session_state.show_success_message = False
    st.session_state.success_message_content = "" # メッセージの内容もクリア


# --- コート割り振りセクション (管理者向け) ---
st.header("２．コート割り振り (管理者向け)")
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
        if st.button("コート割り振りを実行して結果シートを更新", key="assign_button_admin_main"):
            st.session_state.last_interaction_time = datetime.datetime.now()
            with st.spinner(f"{target_date_assign_input.strftime('%Y-%m-%d')} のコート割り振り中..."):
                attendance_df = load_data_to_dataframe(gspread_client, SPREADSHEET_ID, ATTENDANCE_SHEET_NAME, required_cols=None)
                if DEBUG_MODE: st.write(f"割り振り対象日: {target_date_assign_input}")
                absent_ids = get_absent_ids_for_date(attendance_df, target_date_assign_input)
                member_df_assign = st.session_state.member_df
                present_members_df = member_df_assign[~member_df_assign[COL_MEMBER_ID].astype(str).isin(absent_ids)].copy()
                if DEBUG_MODE: st.write(f"参加予定者: {len(present_members_df)} 名")
                absent_members_df = member_df_assign[member_df_assign[COL_MEMBER_ID].astype(str).isin(absent_ids)].copy()
                if DEBUG_MODE: st.write(f"欠席/遅刻連絡者: {len(absent_members_df)} 名")

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

                absent_ws = get_worksheet_safe(gspread_client, SPREADSHEET_ID, ABSENT_LIST_SHEET_NAME)
                if absent_ws:
                    if DEBUG_MODE: st.write(f"欠席者名簿 ({target_date_assign_input}) を出力...")
                    if not absent_members_df.empty:
                        absent_output_cols = [COL_MEMBER_ID, COL_MEMBER_NAME, COL_MEMBER_GRADE]
                        valid_absent_cols = [col for col in absent_output_cols if col in absent_members_df.columns]
                        absent_list_output = [[f"{target_date_assign_input.strftime('%Y-%m-%d')} 欠席/遅刻者リスト"]]
                        absent_list_output.append(valid_absent_cols); absent_list_output.extend(absent_members_df[valid_absent_cols].values.tolist())
                    else: absent_list_output = [[f"{target_date_assign_input.strftime('%Y-%m-%d')} の欠席/遅刻連絡者なし"]]
                    write_results_to_sheet(absent_ws, absent_list_output, data_name=f"{target_date_assign_input.strftime('%Y-%m-%d')} 欠席者名簿")
                else: st.error(f"シート '{ABSENT_LIST_SHEET_NAME}' が見つかりません。")

                if present_members_df.empty: st.warning("参加予定者がいないため、コート割り振りは行いません。")
                else:
                    num_teams_8 = TEAMS_COUNT_MAP.get('ノック', 8); num_teams_12 = TEAMS_COUNT_MAP.get('ハンドノック', 12)
                    assignment_ws_8 = get_worksheet_safe(gspread_client, SPREADSHEET_ID, ASSIGNMENT_SHEET_NAME_8)
                    if assignment_ws_8:
                        if DEBUG_MODE: st.write("--- 8チーム割り振りを実行中 (v7) ---")
                        assignments_8 = assign_courts_to_teams_v7(present_members_df, num_teams_8)
                        if assignments_8: result_output_8 = format_assignment_results(assignments_8, "8チーム", target_date_assign_input); write_results_to_sheet(assignment_ws_8, result_output_8, f"8チーム結果({target_date_assign_input.strftime('%Y-%m-%d')})")
                        else: st.warning("8チーム割り振り結果なし。")
                    else: st.error(f"シート '{ASSIGNMENT_SHEET_NAME_8}' が見つかりません。")
                    assignment_ws_12 = get_worksheet_safe(gspread_client, SPREADSHEET_ID, ASSIGNMENT_SHEET_NAME_12)
                    if assignment_ws_12:
                        if DEBUG_MODE: st.write("--- 12チーム割り振りを実行中 (v7) ---")
                        assignments_12 = assign_courts_to_teams_v7(present_members_df, num_teams_12)
                        if assignments_12: result_output_12 = format_assignment_results(assignments_12, "12チーム", target_date_assign_input); write_results_to_sheet(assignment_ws_12, result_output_12, f"12チーム結果({target_date_assign_input.strftime('%Y-%m-%d')})")
                        else: st.warning("12チーム割り振り結果なし。")
                    else: st.error(f"シート '{ASSIGNMENT_SHEET_NAME_12}' が見つかりません。")
                    st.info(f"{target_date_assign_input.strftime('%Y-%m-%d')} の割り振り処理と名簿出力が完了しました。")
    else:
        st.info("コート割り振り実行には部員データが必要です。")
elif st.session_state.authentication_status is True and not st.session_state.is_admin:
    st.info("コート割り振り機能は管理者専用です。")
st.caption("システム管理者向けエリア")
