#!/usr/bin/env python3
"""
過去実績検索Agent v3 - Streamlit Webアプリ版

起動方法:
    streamlit run streamlit_app_past_performance.py

アクセス:
    http://localhost:8501
"""

import streamlit as st
import json
from past_performance_agent_v3 import PastPerformanceAgentV3
import pandas as pd

# ────────────────────────────────────────────────────────────────────
# ページ設定
# ────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="過去実績検索 v3",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ────────────────────────────────────────────────────────────────────
# セッション状態の初期化
# ────────────────────────────────────────────────────────────────────

if 'agent' not in st.session_state:
    # Secrets から API キーを取得
    api_key = st.secrets.get('ANTHROPIC_API_KEY')
    if not api_key:
        st.error("❌ Secrets に ANTHROPIC_API_KEY が設定されていません")
        st.stop()

    try:
        st.session_state.agent = PastPerformanceAgentV3()
    except Exception as e:
        st.error(f"❌ Agent の初期化に失敗しました: {e}")
        st.stop()

if 'search_result' not in st.session_state:
    st.session_state.search_result = None

if 'search_history' not in st.session_state:
    st.session_state.search_history = []

# ────────────────────────────────────────────────────────────────────
# ヘッダー
# ────────────────────────────────────────────────────────────────────

st.title("🔍 過去実績検索Agent v3")
st.markdown("""
**商材内容ベースで類似案件を発掘する、次世代の過去実績検索システム**

v3の新機能:
- ✨ 商材内容ベースの類似判定（業種を超えた検索）
- 🧠 Web情報活用（商材の詳細情報を自動取得）
- 🎨 クリエイティブ分析（広告クリエイティブ内容を考慮）
- 📊 類似度スコア（0-100点で客観的評価）
- 🎯 ベンチマーク機能（特定アカウントを必ず結果に含める）
- 📅 参照期間カスタマイズ（180日〜537日）
""")

st.divider()

# ────────────────────────────────────────────────────────────────────
# サイドバー：ヒアリング入力
# ────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("📋 顧客ヒアリング情報")

    with st.form("hearing_form"):
        st.subheader("基本情報")
        project_name = st.text_input(
            "案件名 *",
            placeholder="例: 2026年春キャンペーン"
        )
        advertiser_name = st.text_input(
            "広告主名 *",
            placeholder="例: 〇〇株式会社"
        )
        agency_name = st.text_input(
            "代理店名",
            value="直接",
            placeholder="例: 電通デジタル"
        )
        campaign_type = st.selectbox(
            "案件タイプ *",
            ["Performance", "Brand"]
        )

        st.divider()
        st.subheader("ヒアリング項目")

        q01 = st.text_area(
            "重点施策・商材内容 *",
            placeholder="例: GUMペーストの認知拡大と購入促進。歯周病予防・口臭対策がメイン訴求",
            help="v3では商材内容の記述が重要です（歯磨き粉、サプリメント等）",
            height=100
        )

        q02 = st.text_input(
            "広告予算の規模感",
            placeholder="例: 月間300万円、Q1-Q2集中投下"
        )

        q03 = st.text_input(
            "主要KPI（CV数/CPA/ROAS等）",
            placeholder="例: 購入CV数200件/月、CPA 8,000円以下"
        )

        q07 = st.text_input(
            "ターゲットユーザー",
            placeholder="例: 30-60代男女、全国、歯周病・口臭を気にする層"
        )

        q13 = st.text_area(
            "困っていること・改善したいこと",
            placeholder="例: Google検索広告のCPAが高騰。新規チャネル開拓が急務",
            height=80
        )

        st.divider()
        st.subheader("⚙️ オプション設定")

        q_benchmark = st.text_input(
            "ベンチマーク広告アカウント名",
            placeholder="例: RENOSY, SUUMO（部分一致）",
            help="参考にしたいアカウント名を入力すると、必ず結果に含まれます"
        )

        q_data_period = st.number_input(
            "データ参照期間（日数）",
            min_value=30,
            max_value=537,
            value=180,
            help="デフォルト: 180日（約6ヶ月）、最大: 537日（約18ヶ月）"
        )

        st.divider()

        submitted = st.form_submit_button(
            "🔍 検索実行",
            type="primary",
            use_container_width=True
        )

# ────────────────────────────────────────────────────────────────────
# メイン：検索実行と結果表示
# ────────────────────────────────────────────────────────────────────

if submitted:
    # バリデーション
    if not project_name or not advertiser_name or not q01:
        st.error("❌ 必須項目を入力してください（案件名、広告主名、商材内容）")
    else:
        # ヒアリングデータ構築
        hearing_data = {
            'basic_info': {
                'project_name': project_name,
                'advertiser_name': advertiser_name,
                'agency_name': agency_name,
                'campaign_type': campaign_type
            }
        }

        if q01: hearing_data['q01'] = q01
        if q02: hearing_data['q02'] = q02
        if q03: hearing_data['q03'] = q03
        if q07: hearing_data['q07'] = q07
        if q13: hearing_data['q13'] = q13
        if q_benchmark: hearing_data['q_benchmark'] = q_benchmark
        if q_data_period: hearing_data['q_data_period'] = str(q_data_period)

        # 検索実行
        with st.spinner('🔍 過去実績を検索中... 数分かかる場合があります'):
            try:
                result = st.session_state.agent.search(hearing_data)
                st.session_state.search_result = result

                # 履歴に追加
                st.session_state.search_history.append({
                    'project_name': project_name,
                    'advertiser_name': advertiser_name,
                    'product_category': result.product_category,
                    'timestamp': pd.Timestamp.now()
                })

                st.success(f"✅ 検索完了！ {len(result.similar_campaigns)}件の類似案件が見つかりました")

            except Exception as e:
                st.error(f"❌ エラーが発生しました: {str(e)}")
                import traceback
                with st.expander("詳細エラー情報"):
                    st.code(traceback.format_exc())

# ────────────────────────────────────────────────────────────────────
# 結果表示
# ────────────────────────────────────────────────────────────────────

if st.session_state.search_result:
    result = st.session_state.search_result

    # サマリー
    st.header("📊 検索結果サマリー")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("商材カテゴリー", result.product_category)
    with col2:
        st.metric("推察CV地点", result.cv_point)
    with col3:
        st.metric("類似キャンペーン数", f"{len(result.similar_campaigns)}件")
    with col4:
        st.metric("成功事例", f"{len(result.success_stories)}件")

    st.divider()

    # タブで情報を整理
    tab1, tab2, tab3, tab4 = st.tabs([
        "🏆 成功事例TOP3",
        "📋 案件一覧",
        "📝 分析レポート",
        "💾 データダウンロード"
    ])

    # タブ1: 成功事例TOP3
    with tab1:
        st.subheader("🏆 成功事例 TOP3")

        if result.success_stories:
            for i, story in enumerate(result.success_stories[:3], 1):
                with st.container():
                    st.markdown(f"### {i}. {story['advertiser_name']}")

                    # メトリクス
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("類似度", f"{story.get('similarity_score', 0)}点")
                    with col2:
                        st.metric("売上", f"¥{story['total_sales']:,.0f}")
                    with col3:
                        st.metric(f"{result.cv_point} CV", f"{story.get('main_cv_count', 0):,}件")
                    with col4:
                        st.metric("CPA", f"¥{story.get('main_cv_cpa', 0):,.0f}")

                    # 詳細情報
                    period = f"{story.get('start_month', '')}〜{story.get('end_month', '')}"
                    st.write(f"**期間:** {period}")

                    if story.get('creative_title'):
                        st.write(f"**クリエイティブ:** {story['creative_title'][:100]}...")

                    if story.get('similarity_reason'):
                        with st.expander("💡 選定理由"):
                            st.info(story['similarity_reason'])

                    st.divider()
        else:
            st.info("成功事例が見つかりませんでした")

    # タブ2: 案件一覧
    with tab2:
        st.subheader("📋 案件一覧表（類似度スコア付き）")

        # テーブル表示
        st.text(result.campaigns_summary_table)

        # データフレーム表示（ソート・フィルタ可能）
        with st.expander("📊 インタラクティブ表（ソート・フィルタ可能）"):
            df_data = []
            for camp in result.similar_campaigns:
                df_data.append({
                    '広告主名': camp['advertiser_name'],
                    '類似度': camp.get('similarity_score', 0),
                    '売上': camp['total_sales'],
                    '主CV': camp.get('main_cv_count', 0),
                    'CPA': camp.get('main_cv_cpa', 0),
                    '期間': f"{camp.get('start_month', '')}〜{camp.get('end_month', '')}",
                    'ベンチマーク': '🎯' if camp.get('is_benchmark', False) else ''
                })

            df = pd.DataFrame(df_data)
            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True
            )

    # タブ3: 分析レポート
    with tab3:
        st.subheader("📝 過去実績分析レポート")
        st.markdown(result.insights)

    # タブ4: ダウンロード
    with tab4:
        st.subheader("💾 データダウンロード")

        # JSON形式
        json_data = {
            'project_name': st.session_state.search_history[-1]['project_name'] if st.session_state.search_history else '',
            'result': {
                'product_category': result.product_category,
                'cv_point': result.cv_point,
                'similar_campaigns_count': len(result.similar_campaigns),
                'success_stories': result.success_stories[:3],
                'campaigns_summary': [
                    {
                        'advertiser_name': c['advertiser_name'],
                        'similarity_score': c.get('similarity_score', 0),
                        'total_sales': c['total_sales'],
                        'main_cv_count': c.get('main_cv_count', 0),
                        'main_cv_cpa': c.get('main_cv_cpa', 0)
                    }
                    for c in result.similar_campaigns
                ],
                'insights': result.insights
            }
        }

        col1, col2 = st.columns(2)

        with col1:
            st.download_button(
                label="📥 JSON形式でダウンロード",
                data=json.dumps(json_data, ensure_ascii=False, indent=2, default=str),
                file_name=f"past_performance_result_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json"
            )

        with col2:
            # CSV形式（案件一覧のみ）
            if df_data:
                df_csv = pd.DataFrame(df_data)
                csv = df_csv.to_csv(index=False, encoding='utf-8-sig')
                st.download_button(
                    label="📥 CSV形式でダウンロード（案件一覧）",
                    data=csv,
                    file_name=f"campaigns_list_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )

# ────────────────────────────────────────────────────────────────────
# フッター
# ────────────────────────────────────────────────────────────────────

st.divider()
st.caption("過去実績検索Agent v3 | Powered by Claude Haiku 4.5 + BigQuery")

# サイドバー下部：検索履歴
with st.sidebar:
    st.divider()
    if st.session_state.search_history:
        st.subheader("📜 検索履歴")
        for i, hist in enumerate(reversed(st.session_state.search_history[-5:]), 1):
            st.caption(f"{i}. {hist['project_name']} ({hist['product_category']})")
