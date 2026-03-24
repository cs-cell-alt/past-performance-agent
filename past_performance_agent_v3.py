#!/usr/bin/env python3
"""
① 過去実績検索Agent v3

【v3の改善内容】
- 業種を特定しすぎない検索（複数業種または業種横断）
- クリエイティブ情報から商材内容を判定
- 商材の類似度をClaudeで判定（本当に類似した案件のみ抽出）
- 類似度スコア順に並び替え

【v2からの変更点】
- get_creative_info(): クリエイティブ情報取得
- extract_product_name(): 商材名抽出
- judge_product_similarity(): 類似度判定（0-100点）
- search_similar_campaigns_v3(): 2段階検索（広く取得→類似度フィルタ）
"""
import os
import json
import base64
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from anthropic import Anthropic
from google.cloud import bigquery
from google.oauth2 import service_account

# グローバル変数（デフォルト値）
ANTHROPIC_API_KEY = None
SERVICE_ACCOUNT_KEY = None
SERVICE_ACCOUNT_INFO = None
PROJECT_ID = 'jp-sales-enablement'

def _get_config():
    """認証情報を取得（遅延評価）"""
    global ANTHROPIC_API_KEY, SERVICE_ACCOUNT_KEY, SERVICE_ACCOUNT_INFO, PROJECT_ID

    # すでに設定済みなら再取得しない
    if ANTHROPIC_API_KEY is not None:
        return ANTHROPIC_API_KEY, SERVICE_ACCOUNT_KEY, SERVICE_ACCOUNT_INFO, PROJECT_ID

    try:
        import streamlit as st
        # Streamlit環境での実行
        if hasattr(st, 'secrets') and len(st.secrets) > 0:
            api_key = st.secrets.get('ANTHROPIC_API_KEY')
            if not api_key:
                raise ValueError("Secrets に ANTHROPIC_API_KEY が設定されていません")

            # サービスアカウント認証情報の取得
            if "service_account" in st.secrets:
                sa_info = dict(st.secrets["service_account"])
            elif "service_account_base64" in st.secrets:
                decoded = base64.b64decode(st.secrets["service_account_base64"])
                sa_info = json.loads(decoded)
            else:
                raise ValueError("Secrets に service_account または service_account_base64 が設定されていません")

            ANTHROPIC_API_KEY = api_key
            SERVICE_ACCOUNT_KEY = None
            SERVICE_ACCOUNT_INFO = sa_info
            return api_key, None, sa_info, PROJECT_ID
    except (ImportError, AttributeError):
        pass

    # ローカル環境
    api_key = os.getenv('ANTHROPIC_API_KEY')
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY 環境変数が設定されていません")

    sa_key = os.path.expanduser('~/service-account-key.json')
    if not os.path.exists(sa_key):
        sa_key = './service-account-key.json'
    if not os.path.exists(sa_key):
        raise ValueError(f"サービスアカウントキーが見つかりません: {sa_key}")

    ANTHROPIC_API_KEY = api_key
    SERVICE_ACCOUNT_KEY = sa_key
    SERVICE_ACCOUNT_INFO = None
    return api_key, sa_key, None, PROJECT_ID

# CV地点の定義
CV_POINTS = {
    'AddToCart': 'カート追加',
    'Purchase': '購入完了',
    'Install': 'アプリインストール',
    'WebConversion': 'Web CV',
    'CompleteRegistration': '会員登録完了',
    'ViewContent': 'コンテンツ閲覧'
}


@dataclass
class PerformanceSearchResult:
    """検索結果"""
    industry: str
    product_category: str  # 追加: 商材カテゴリー（例：歯磨き粉、サプリメント）
    cv_point: str
    similar_campaigns: List[Dict]
    industry_benchmarks: Dict
    success_stories: List[Dict]
    campaigns_summary_table: str
    insights: str
    query_metadata: Dict


class PastPerformanceAgentV3:
    """過去実績検索Agent v3"""

    def __init__(self, anthropic_api_key: str = None,
                 bq_project_id: str = None,
                 service_account_key: str = None):
        """初期化"""
        print("=" * 80)
        print("🔍 過去実績検索Agent v3 初期化中...")
        print("=" * 80)

        # 認証情報を取得（遅延評価）
        api_key, sa_key, sa_info, project_id = _get_config()

        # 引数が指定されていない場合はデフォルト値を使用
        if anthropic_api_key is None:
            anthropic_api_key = api_key
        if bq_project_id is None:
            bq_project_id = project_id
        if service_account_key is None:
            service_account_key = sa_key

        # Anthropic Claude初期化
        if not anthropic_api_key:
            raise ValueError("ANTHROPIC_API_KEY が設定されていません")
        self.claude = Anthropic(api_key=anthropic_api_key)
        print("✅ Claude Haiku 4.5 接続完了")

        # BigQuery初期化
        if sa_info:
            # Streamlit Cloud環境: dict から認証情報を作成
            credentials = service_account.Credentials.from_service_account_info(
                sa_info,
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
        elif service_account_key:
            # ローカル環境: ファイルから認証情報を読み込み
            credentials = service_account.Credentials.from_service_account_file(
                service_account_key,
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
        else:
            raise ValueError("サービスアカウント認証情報が見つかりません")

        self.bq_client = bigquery.Client(credentials=credentials, project=bq_project_id)
        self.project_id = bq_project_id
        print(f"✅ BigQuery 接続完了 (Project: {bq_project_id})")
        print()

    def get_available_industries(self) -> List[str]:
        """BigQueryから利用可能な業種一覧を取得"""
        query = f"""
        SELECT DISTINCT industry
        FROM `{self.project_id}.best_practices_dev.daily_std_ad_account_performance`
        WHERE dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
          AND industry IS NOT NULL
        ORDER BY industry
        """
        try:
            results = self.bq_client.query(query).result()
            return [row['industry'] for row in results]
        except:
            return []

    def extract_product_info(self, hearing_data: Dict) -> Dict:
        """
        ヒアリング情報から商材情報を抽出
        - 商材カテゴリー（歯磨き粉、サプリメント、化粧品等）
        - 商材の特徴・用途
        """
        hearing_text = self._format_hearing_data(hearing_data)

        prompt = f"""あなたは営業提案支援のデータアナリストです。
以下の顧客ヒアリング情報から、商材の情報を抽出してください。

# 顧客ヒアリング情報
{hearing_text}

# 抽出してほしい項目
1. **商材カテゴリー**: 具体的な商品分類（例：歯磨き粉、サプリメント、化粧品、アプリ、不動産等）
2. **商材の特徴**: 商材の用途や特徴を簡潔に（1-2文）
3. **関連キーワード**: 類似商材を探すためのキーワード（3-5個）

# 出力形式
JSONフォーマットで出力してください。

{{
  "category": "商材カテゴリー",
  "description": "商材の特徴・用途",
  "keywords": ["キーワード1", "キーワード2", "キーワード3"]
}}

例：
- GUMペースト → {{"category": "歯磨き粉", "description": "歯周病予防・口臭対策のオーラルケア商品", "keywords": ["歯磨き粉", "歯ブラシ", "マウスウォッシュ", "オーラルケア", "歯周病"]}}
- サプリメント → {{"category": "サプリメント", "description": "健康維持・栄養補助食品", "keywords": ["サプリ", "健康食品", "栄養補助", "ビタミン", "ミネラル"]}}
"""

        response = self.claude.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=800,
            messages=[{"role": "user", "content": prompt}]
        )

        product_info = self._extract_json(response.content[0].text)

        print(f"✅ 商材情報抽出完了")
        print(f"   カテゴリー: {product_info.get('category', '不明')}")
        print(f"   特徴: {product_info.get('description', '')[:50]}...")
        print()

        return product_info

    def enrich_product_info_with_web(self, hearing_data: Dict, product_info: Dict) -> Dict:
        """
        Claudeの知識ベースで商材情報を補強

        Args:
            hearing_data: ヒアリング情報
            product_info: 既に抽出された商材情報

        Returns:
            Web情報で補強された商材情報
        """
        print("【Web検索】検索対象商材の詳細情報を取得中...")

        # ヒアリング情報から商材名/広告主名を取得
        product_name = hearing_data.get('basic_info', {}).get('project_name', '')
        advertiser_name = hearing_data.get('basic_info', {}).get('advertiser_name', '')
        category = product_info.get('category', '')
        description = product_info.get('description', '')

        prompt = f"""あなたは商材リサーチの専門家です。以下の商材について、あなたの知識から詳細情報を提供してください。

# 商材情報
- 広告主: {advertiser_name}
- 商材名: {product_name}
- カテゴリー: {category}
- 説明: {description}

# 提供してほしい情報
1. この商材の正式名称と企業情報（知っている場合）
2. 商材の主な特徴・機能・訴求ポイント
3. ターゲット顧客層
4. 類似商品や競合商品の例
5. 業界内での位置づけ

もしこの商材について直接的な知識がない場合は、カテゴリー（{category}）から推測される一般的な特徴を記載してください。

JSON形式で出力:
{{
  "official_name": "正式名称（不明な場合は入力された名前）",
  "company_info": "企業情報（分かる範囲で）",
  "features": ["特徴1", "特徴2", "特徴3"],
  "target_customers": "ターゲット層の説明",
  "competitors": ["競合商品1", "競合商品2"],
  "market_position": "業界内での位置づけ"
}}
"""

        try:
            response = self.claude.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=1500,
                messages=[{"role": "user", "content": prompt}]
            )

            web_info = self._extract_json(response.content[0].text)

            # product_infoに追加
            product_info['web_info'] = web_info
            product_info['enriched_description'] = f"{description}。{web_info.get('company_info', '')} 主な特徴: {', '.join(web_info.get('features', [])[:3])}。ターゲット: {web_info.get('target_customers', '')}"

            print(f"✅ Web情報取得完了")
            print(f"   正式名称: {web_info.get('official_name', '不明')}")
            print(f"   競合商品: {', '.join(web_info.get('competitors', [])[:3])}")
            print()

        except Exception as e:
            print(f"⚠️  Web情報取得エラー: {e}")
            product_info['web_info'] = {}
            product_info['enriched_description'] = description

        return product_info

    def get_creative_info(self, advertiser_name: str, limit: int = 5) -> List[Dict]:
        """
        cr_rawテーブルから該当案件のクリエイティブ情報を複数取得

        Args:
            advertiser_name: 広告主名
            limit: 取得件数（デフォルト5件）

        Returns:
            クリエイティブ情報のリスト
        """
        query = f"""
        SELECT
            creative_title,
            creative_body_text,
            SUM(cv) as total_cv
        FROM `{self.project_id}.best_practices_dev.cr_raw`
        WHERE advertiser_name = '{advertiser_name}'
          AND dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
          AND creative_title IS NOT NULL
        GROUP BY creative_title, creative_body_text
        ORDER BY total_cv DESC
        LIMIT {limit}
        """

        try:
            results = list(self.bq_client.query(query).result())
            creatives = []
            for row in results:
                creatives.append({
                    'title': row['creative_title'],
                    'body': row['creative_body_text'] or '',
                    'cv': row['total_cv']
                })
            return creatives
        except Exception as e:
            pass

        return []

    def format_creative_list(self, creatives: List[Dict]) -> str:
        """
        複数のクリエイティブ情報を整形してテキスト化
        """
        if not creatives:
            return "（クリエイティブ情報なし）"

        lines = []
        for i, creative in enumerate(creatives[:5], 1):  # 最大5件
            title = creative.get('title', '')[:100]
            body = creative.get('body', '')[:100]
            lines.append(f"   [{i}] {title}")
            if body:
                lines.append(f"       {body}")

        return "\n".join(lines)

    def extract_product_name_from_advertiser(self, advertiser_name: str) -> str:
        """
        広告主名から商材名を抽出
        例：「サンスター株式会社(GUM)_CARTA ZERO[電通デジタル]」→ 「GUM」
        """
        import re
        # カッコ内の商材名を抽出
        match = re.search(r'\(([^)]+)\)', advertiser_name)
        if match:
            return match.group(1)
        # カッコがない場合は会社名の後の部分
        parts = advertiser_name.split('_')
        if len(parts) > 1:
            return parts[0]
        return advertiser_name

    def judge_product_similarity(self, target_product: Dict, candidate_advertiser: str,
                                  candidate_creative: Dict) -> Dict:
        """
        商材の類似度を判定（0-100点）

        Args:
            target_product: 検索対象の商材情報
            candidate_advertiser: 候補案件の広告主名
            candidate_creative: 候補案件のクリエイティブ情報

        Returns:
            {'similarity_score': 85, 'reason': '理由'}
        """
        # 広告主名から商材名抽出
        product_name = self.extract_product_name_from_advertiser(candidate_advertiser)

        # クリエイティブ情報がない場合は商材名のみで判定
        creative_info = ""
        if candidate_creative and candidate_creative.get('title'):
            title = candidate_creative.get('title', '')
            body = candidate_creative.get('body', '')[:200]  # 長すぎる場合は切る
            creative_info = f"\nクリエイティブタイトル: {title}\nクリエイティブ本文: {body}"

        prompt = f"""あなたは商材の類似性を判定する専門家です。

# 検索対象の商材
カテゴリー: {target_product.get('category', '不明')}
説明: {target_product.get('description', '')}
関連キーワード: {', '.join(target_product.get('keywords', []))}

# 候補案件の商材
広告主名: {candidate_advertiser}
商材名（推定）: {product_name}{creative_info}

# タスク
上記2つの商材の類似度を0-100点で評価してください。

# 評価基準
- 100点：完全に同一カテゴリーで用途も同じ（例：歯磨き粉 vs 歯磨き粉）
- 80-90点：同一カテゴリーで関連商品（例：歯磨き粉 vs 歯ブラシ、マウスウォッシュ）
- 60-70点：カテゴリーは異なるが用途が類似（例：歯磨き粉 vs 口臭対策ガム）
- 40-50点：カテゴリーも用途も異なるが同じ業界（例：歯磨き粉 vs シャンプー）
- 20-30点：まったく関連性が薄い（例：歯磨き粉 vs 消臭剤）
- 0-10点：全く無関係（例：歯磨き粉 vs 靴下）

# 出力形式
JSONフォーマットで出力してください。

{{
  "similarity_score": 85,
  "reason": "両方ともオーラルケア商品であり、ターゲット層・用途が類似している"
}}
"""

        try:
            response = self.claude.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=300,
                messages=[{"role": "user", "content": prompt}]
            )

            result = self._extract_json(response.content[0].text)
            return {
                'similarity_score': result.get('similarity_score', 0),
                'reason': result.get('reason', '')
            }
        except Exception as e:
            # エラー時はスコア0を返す
            return {'similarity_score': 0, 'reason': f'判定エラー: {str(e)}'}

    def judge_product_similarity_batch(self, target_product: Dict,
                                        candidates_with_creative: List[Dict]) -> List[Dict]:
        """
        商材の類似度をバッチ判定（複数案件を1回のAPI呼び出しで判定）

        Args:
            target_product: 検索対象の商材情報
            candidates_with_creative: 候補案件リスト（advertiser_name, creative_info含む）

        Returns:
            各候補にsimilarity_score, similarity_reasonを追加したリスト
        """
        if not candidates_with_creative:
            return []

        # 候補をテキスト化
        candidates_text = []
        for i, cand in enumerate(candidates_with_creative, 1):
            advertiser = cand.get('advertiser_name', '')
            product_name = self.extract_product_name_from_advertiser(advertiser)
            creatives = cand.get('creative_info', [])

            creative_info = ""
            if creatives and len(creatives) > 0:
                # 複数クリエイティブを表示（最大3件）
                creative_info = "\n   クリエイティブ:"
                for j, creative in enumerate(creatives[:3], 1):
                    title = creative.get('title', '')[:100]
                    creative_info += f"\n     {j}. {title}"

            candidates_text.append(f"{i}. 広告主: {advertiser}\n   商材名(推定): {product_name}{creative_info}")

        candidates_str = "\n\n".join(candidates_text)

        prompt = f"""あなたは商材の類似性を判定する専門家です。

# 検索対象の商材
カテゴリー: {target_product.get('category', '不明')}
説明: {target_product.get('description', '')}
関連キーワード: {', '.join(target_product.get('keywords', []))}

# 候補案件リスト（{len(candidates_with_creative)}件）
{candidates_str}

# タスク
各候補案件と検索対象商材の類似度を0-100点で評価してください。

# 評価基準
- 100点：完全に同一カテゴリーで用途も同じ
- 80-90点：同一カテゴリーで関連商品
- 60-70点：カテゴリーは異なるが用途が類似
- 40-50点：カテゴリーも用途も異なるが同じ業界
- 20-30点：まったく関連性が薄い
- 0-10点：全く無関係

# 出力形式
JSON配列で、各候補のスコアと理由を返してください。

[
  {{"index": 1, "similarity_score": 85, "reason": "理由"}},
  {{"index": 2, "similarity_score": 20, "reason": "理由"}},
  ...
]
"""

        try:
            response = self.claude.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}]
            )

            # JSON配列を抽出
            response_text = response.content[0].text
            start = response_text.find('[')
            end = response_text.rfind(']') + 1
            if start >= 0 and end > start:
                json_str = response_text[start:end]
                results = json.loads(json_str)

                # 結果を候補に反映
                for result in results:
                    idx = result.get('index', 0) - 1
                    if 0 <= idx < len(candidates_with_creative):
                        candidates_with_creative[idx]['similarity_score'] = result.get('similarity_score', 0)
                        candidates_with_creative[idx]['similarity_reason'] = result.get('reason', '')

        except Exception as e:
            print(f"   ⚠️  バッチ判定エラー: {e}")
            # エラー時は全てスコア0
            for cand in candidates_with_creative:
                cand['similarity_score'] = 0
                cand['similarity_reason'] = f'判定エラー'

        return candidates_with_creative

    def infer_related_industries(self, target_product: Dict) -> List[str]:
        """
        商材情報から関連する業種を1-2つ推定

        Args:
            target_product: 商材情報（category, description, keywords）

        Returns:
            関連業種のリスト（最大2つ）
        """
        # BigQueryから利用可能な業種リストを取得
        available_industries = self.get_available_industries()
        # 全業種をClaudeに提供（最大150件程度に制限してトークン節約）
        industries_list = '\n'.join([f"  - {ind}" for ind in available_industries[:150]])

        # Web検索で補強された説明があればそれを使用
        description = target_product.get('enriched_description', target_product.get('description', ''))
        web_info = target_product.get('web_info', {})
        competitors = web_info.get('competitors', [])

        prompt = f"""あなたは営業提案支援のデータアナリストです。

# 検索対象の商材
カテゴリー: {target_product.get('category', '不明')}
説明: {description}
関連キーワード: {', '.join(target_product.get('keywords', []))}
競合商品（参考）: {', '.join(competitors[:3]) if competitors else 'なし'}

# BigQueryで利用可能な業種リスト
{industries_list}

# タスク
上記の商材に最も関連する業種を、利用可能な業種リストから1-2つ選んでください。
Web検索で取得した商材の詳細情報（説明、競合商品）も参考にしてください。

# 選定基準
- 第1候補: 最も直接的に関連する業種
- 第2候補: 間接的に関連する業種（あれば）

# 例
- 歯磨き粉 → 第1候補「歯磨」、第2候補「その他トイレタリー」
- サプリメント → 第1候補「サプリメント」、第2候補「健康・美容食品」
- 化粧品 → 第1候補「化粧品」、第2候補「健康・美容食品」

# 出力形式
JSON配列で、最大2つの業種名を返してください。

{{
  "industries": ["業種名1", "業種名2"]
}}

※必ず上記のリストから選択してください（完全一致）
"""

        try:
            response = self.claude.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=300,
                messages=[{"role": "user", "content": prompt}]
            )

            result = self._extract_json(response.content[0].text)
            industries = result.get('industries', [])

            # 最大2つに制限
            related = industries[:2] if industries else []

            print(f"✅ 関連業種推定完了")
            print(f"   第1候補: {related[0] if len(related) > 0 else '（なし）'}")
            if len(related) > 1:
                print(f"   第2候補: {related[1]}")
            print()

            return related

        except Exception as e:
            print(f"⚠️  関連業種推定エラー: {e}")
            # エラー時は空リストを返す
            return []

    def fetch_candidates_by_industry(self, industry: str, cv_point: str,
                                      limit: int = 50, offset: int = 0,
                                      data_period_days: int = 180) -> List[Dict]:
        """
        指定業種内で候補案件を取得

        Args:
            industry: 業種名
            cv_point: CV地点
            limit: 取得件数
            offset: オフセット（追加取得時に使用）
            data_period_days: データ参照期間（日数、デフォルト180日）

        Returns:
            候補案件リスト
        """
        cv_column_map = {
            'AddToCart': 'AddToCart',
            'Purchase': 'Purchase',
            'Install': 'Install',
            'WebConversion': 'WebConversion',
            'CompleteRegistration': 'CompleteRegistration',
            'ViewContent': 'ViewContent'
        }
        cv_column = cv_column_map.get(cv_point, 'cv')

        query = f"""
        SELECT
            advertiser_name,
            ad_account_id,
            agency_name,
            industry,
            FORMAT_DATE('%Y年%m月', MIN(dt)) as start_month,
            FORMAT_DATE('%Y年%m月', MAX(dt)) as end_month,
            SUM(sales) as total_sales,
            SUM(vimp) as total_vimp,
            SUM(click) as total_clicks,
            AVG(cpc) as avg_cpc,
            SUM(cv) as total_cv,
            AVG(CPA) as avg_cpa,
            COUNT(DISTINCT dt) as active_days,
            SUM(AddToCart) as cv_addtocart,
            SUM(Install) as cv_install,
            SUM(WebConversion) as cv_webconversion,
            SUM(CompleteRegistration) as cv_registration,
            SUM(Purchase) as cv_purchase,
            SUM(ViewContent) as cv_viewcontent
        FROM `{self.project_id}.best_practices_dev.daily_std_ad_account_performance`
        WHERE dt >= DATE_SUB(CURRENT_DATE(), INTERVAL {data_period_days} DAY)
          AND industry = '{industry}'
          AND sales > 0
          AND cv > 0
        GROUP BY advertiser_name, ad_account_id, agency_name, industry
        HAVING total_sales > 10000 AND total_cv > 10
        ORDER BY total_sales DESC
        LIMIT {limit}
        OFFSET {offset}
        """

        try:
            results = self.bq_client.query(query).result()
            candidates = []

            for row in results:
                candidates.append({
                    'advertiser_name': row['advertiser_name'],
                    'ad_account_id': row['ad_account_id'],
                    'agency_name': row['agency_name'],
                    'industry': row['industry'],
                    'start_month': row['start_month'],
                    'end_month': row['end_month'],
                    'total_sales': float(row['total_sales']) if row['total_sales'] else 0,
                    'total_vimp': int(row['total_vimp']) if row['total_vimp'] else 0,
                    'total_clicks': int(row['total_clicks']) if row['total_clicks'] else 0,
                    'avg_cpc': float(row['avg_cpc']) if row['avg_cpc'] else 0,
                    'total_cv': int(row['total_cv']) if row['total_cv'] else 0,
                    'avg_cpa': float(row['avg_cpa']) if row['avg_cpa'] else 0,
                    'active_days': int(row['active_days']) if row['active_days'] else 0,
                    'cv_addtocart': int(row['cv_addtocart']) if row['cv_addtocart'] else 0,
                    'cv_install': int(row['cv_install']) if row['cv_install'] else 0,
                    'cv_webconversion': int(row['cv_webconversion']) if row['cv_webconversion'] else 0,
                    'cv_registration': int(row['cv_registration']) if row['cv_registration'] else 0,
                    'cv_purchase': int(row['cv_purchase']) if row['cv_purchase'] else 0,
                    'cv_viewcontent': int(row['cv_viewcontent']) if row['cv_viewcontent'] else 0
                })

            return candidates

        except Exception as e:
            print(f"   ⚠️  業種 '{industry}' の候補取得エラー: {e}")
            return []

    def fetch_candidates_by_industry_fallback(self, cv_point: str, limit: int = 50,
                                               data_period_days: int = 180) -> List[Dict]:
        """
        業種を指定せず全業種から候補を取得（フォールバック用）

        Args:
            cv_point: CV地点
            limit: 取得件数
            data_period_days: データ参照期間（日数、デフォルト180日）

        Returns:
            候補案件リスト
        """
        cv_column_map = {
            'AddToCart': 'AddToCart',
            'Purchase': 'Purchase',
            'Install': 'Install',
            'WebConversion': 'WebConversion',
            'CompleteRegistration': 'CompleteRegistration',
            'ViewContent': 'ViewContent'
        }
        cv_column = cv_column_map.get(cv_point, 'cv')

        query = f"""
        SELECT
            advertiser_name,
            ad_account_id,
            agency_name,
            industry,
            FORMAT_DATE('%Y年%m月', MIN(dt)) as start_month,
            FORMAT_DATE('%Y年%m月', MAX(dt)) as end_month,
            SUM(sales) as total_sales,
            SUM(vimp) as total_vimp,
            SUM(click) as total_clicks,
            AVG(cpc) as avg_cpc,
            SUM(cv) as total_cv,
            AVG(CPA) as avg_cpa,
            COUNT(DISTINCT dt) as active_days,
            SUM(AddToCart) as cv_addtocart,
            SUM(Install) as cv_install,
            SUM(WebConversion) as cv_webconversion,
            SUM(CompleteRegistration) as cv_registration,
            SUM(Purchase) as cv_purchase,
            SUM(ViewContent) as cv_viewcontent
        FROM `{self.project_id}.best_practices_dev.daily_std_ad_account_performance`
        WHERE dt >= DATE_SUB(CURRENT_DATE(), INTERVAL {data_period_days} DAY)
          AND sales > 0
          AND {cv_column} > 0
        GROUP BY advertiser_name, ad_account_id, agency_name, industry
        HAVING total_sales > 10000 AND total_cv > 10
        ORDER BY total_sales DESC
        LIMIT {limit}
        """

        try:
            results = self.bq_client.query(query).result()
            candidates = []

            for row in results:
                candidates.append({
                    'advertiser_name': row['advertiser_name'],
                    'ad_account_id': row['ad_account_id'],
                    'agency_name': row['agency_name'],
                    'industry': row['industry'],
                    'start_month': row['start_month'],
                    'end_month': row['end_month'],
                    'total_sales': float(row['total_sales']) if row['total_sales'] else 0,
                    'total_vimp': int(row['total_vimp']) if row['total_vimp'] else 0,
                    'total_clicks': int(row['total_clicks']) if row['total_clicks'] else 0,
                    'avg_cpc': float(row['avg_cpc']) if row['avg_cpc'] else 0,
                    'total_cv': int(row['total_cv']) if row['total_cv'] else 0,
                    'avg_cpa': float(row['avg_cpa']) if row['avg_cpa'] else 0,
                    'active_days': int(row['active_days']) if row['active_days'] else 0,
                    'cv_addtocart': int(row['cv_addtocart']) if row['cv_addtocart'] else 0,
                    'cv_install': int(row['cv_install']) if row['cv_install'] else 0,
                    'cv_webconversion': int(row['cv_webconversion']) if row['cv_webconversion'] else 0,
                    'cv_registration': int(row['cv_registration']) if row['cv_registration'] else 0,
                    'cv_purchase': int(row['cv_purchase']) if row['cv_purchase'] else 0,
                    'cv_viewcontent': int(row['cv_viewcontent']) if row['cv_viewcontent'] else 0
                })

            return candidates

        except Exception as e:
            print(f"   ⚠️  候補取得エラー: {e}")
            return []

    def fetch_benchmark_account(self, advertiser_name: str,
                                 data_period_days: int = 180) -> Optional[Dict]:
        """
        ベンチマーク指定された広告アカウントを取得

        Args:
            advertiser_name: 広告主名（部分一致）
            data_period_days: データ参照期間（日数、デフォルト180日）

        Returns:
            アカウント情報（見つからない場合はNone）
        """
        query = f"""
        SELECT
            advertiser_name,
            ad_account_id,
            agency_name,
            industry,
            FORMAT_DATE('%Y年%m月', MIN(dt)) as start_month,
            FORMAT_DATE('%Y年%m月', MAX(dt)) as end_month,
            SUM(sales) as total_sales,
            SUM(vimp) as total_vimp,
            SUM(click) as total_clicks,
            AVG(cpc) as avg_cpc,
            SUM(cv) as total_cv,
            AVG(CPA) as avg_cpa,
            COUNT(DISTINCT dt) as active_days,
            SUM(AddToCart) as cv_addtocart,
            SUM(Install) as cv_install,
            SUM(WebConversion) as cv_webconversion,
            SUM(CompleteRegistration) as cv_registration,
            SUM(Purchase) as cv_purchase,
            SUM(ViewContent) as cv_viewcontent
        FROM `{self.project_id}.best_practices_dev.daily_std_ad_account_performance`
        WHERE dt >= DATE_SUB(CURRENT_DATE(), INTERVAL {data_period_days} DAY)
          AND advertiser_name LIKE '%{advertiser_name}%'
          AND sales > 0
        GROUP BY advertiser_name, ad_account_id, agency_name, industry
        HAVING total_sales > 1000
        ORDER BY total_sales DESC
        LIMIT 1
        """

        try:
            results = list(self.bq_client.query(query).result())
            if not results:
                return None

            row = results[0]
            return {
                'advertiser_name': row['advertiser_name'],
                'ad_account_id': row['ad_account_id'],
                'agency_name': row['agency_name'],
                'industry': row['industry'],
                'start_month': row['start_month'],
                'end_month': row['end_month'],
                'total_sales': float(row['total_sales']) if row['total_sales'] else 0,
                'total_vimp': int(row['total_vimp']) if row['total_vimp'] else 0,
                'total_clicks': int(row['total_clicks']) if row['total_clicks'] else 0,
                'avg_cpc': float(row['avg_cpc']) if row['avg_cpc'] else 0,
                'total_cv': int(row['total_cv']) if row['total_cv'] else 0,
                'avg_cpa': float(row['avg_cpa']) if row['avg_cpa'] else 0,
                'active_days': int(row['active_days']) if row['active_days'] else 0,
                'cv_addtocart': int(row['cv_addtocart']) if row['cv_addtocart'] else 0,
                'cv_install': int(row['cv_install']) if row['cv_install'] else 0,
                'cv_webconversion': int(row['cv_webconversion']) if row['cv_webconversion'] else 0,
                'cv_registration': int(row['cv_registration']) if row['cv_registration'] else 0,
                'cv_purchase': int(row['cv_purchase']) if row['cv_purchase'] else 0,
                'cv_viewcontent': int(row['cv_viewcontent']) if row['cv_viewcontent'] else 0,
                'is_benchmark': True  # ベンチマークフラグ
            }

        except Exception as e:
            print(f"   ⚠️  ベンチマークアカウント取得エラー: {e}")
            return None

    def infer_cv_point(self, hearing_data: Dict) -> str:
        """ヒアリング情報からCV地点を推察（キーワードベース+Claude）"""
        hearing_text = self._format_hearing_data(hearing_data)
        hearing_text_lower = hearing_text.lower()

        # キーワードベースの優先判定（高精度）
        if any(kw in hearing_text_lower for kw in ['購入', '売上', 'ec売上', '購買', '買上', 'purchase']):
            return 'Purchase'
        if any(kw in hearing_text_lower for kw in ['カート追加', 'カート', 'add to cart', 'addtocart']):
            return 'AddToCart'
        if any(kw in hearing_text_lower for kw in ['インストール', 'ダウンロード', 'dl', 'install', 'アプリ']):
            return 'Install'
        if any(kw in hearing_text_lower for kw in ['会員登録', '登録完了', '申込完了', 'registration']):
            return 'CompleteRegistration'
        if any(kw in hearing_text_lower for kw in ['閲覧', '視聴', 'view', 'content']):
            return 'ViewContent'

        # キーワードで判定できない場合はClaudeで判定
        prompt = f"""あなたは営業提案支援のデータアナリストです。
以下の顧客ヒアリング情報から、最適化すべきCV地点（コンバージョンポイント）を推察してください。

# 顧客ヒアリング情報
{hearing_text}

# CV地点の選択肢
- **Purchase** (購入完了): EC・通販で商品購入を最終CVとする場合
- **AddToCart** (カート追加): 購入前の中間CVとして、カート追加を重視する場合
- **Install** (アプリインストール): アプリダウンロード・インストールが目的
- **WebConversion** (Web CV): 資料請求、問い合わせ、申込など汎用的なCV
- **CompleteRegistration** (会員登録完了): 会員登録・アカウント作成が目的
- **ViewContent** (コンテンツ閲覧): 記事閲覧、動画視聴などエンゲージメント重視

# 重要な判定ルール
- 「売上」「購入」「EC」のいずれかが含まれる場合は必ず Purchase を選択
- 「資料請求」「問い合わせ」「申込」の場合は WebConversion または CompleteRegistration
- 明確な判断材料がない場合のみ WebConversion（汎用）

# 出力形式
JSON形式で、最も適切なCV地点を1つ返してください。

{{
  "cv_point": "Purchase"
}}
"""

        response = self.claude.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}]
        )

        result = self._extract_json(response.content[0].text)
        return result.get('cv_point', 'WebConversion')

    def search_similar_campaigns_v3(self, target_product: Dict, cv_point: str,
                                     min_similarity: int = 60, min_good_results: int = 5,
                                     benchmark_account: Optional[Dict] = None,
                                     data_period_days: int = 180) -> List[Dict]:
        """
        v3の段階的業種フォーカス検索

        第1段階: 関連業種1-2つから各50件取得 → 類似度判定
        第2段階: 結果が不十分なら、追加取得 or 別業種追加

        Args:
            target_product: 検索対象の商材情報
            cv_point: CV地点
            min_similarity: 最低類似度スコア（デフォルト60点以上）
            min_good_results: 最低限必要な結果件数（デフォルト5件）
            benchmark_account: ベンチマーク指定された広告アカウント（必ず結果に含める）
            data_period_days: データ参照期間（日数、デフォルト180日）
        """
        print("【2】類似キャンペーン検索中（v3: 段階的業種フォーカス検索）...")
        print(f"   検索対象商材: {target_product.get('category', '不明')}")
        print(f"   CV地点: {cv_point}")
        print(f"   📅 参照期間: 過去{data_period_days}日間")
        if benchmark_account:
            print(f"   🎯 ベンチマーク指定: {benchmark_account['advertiser_name']}")
        print()

        # Step 1: 関連業種を推定
        print("   Step 1: 関連業種を推定中...")
        related_industries = self.infer_related_industries(target_product)

        # ベンチマークアカウントの業種を関連業種に追加
        if benchmark_account and benchmark_account.get('industry'):
            benchmark_industry = benchmark_account['industry']
            if benchmark_industry not in related_industries:
                related_industries.append(benchmark_industry)
                print(f"   ✅ ベンチマーク業種「{benchmark_industry}」を追加")

        if not related_industries:
            print("   ⚠️  関連業種の推定に失敗しました。全業種から検索します。")
            # フォールバック: 全業種から取得
            related_industries = ['']  # 空文字列で全業種検索

        # Step 2: 第1段階 - 関連業種から候補取得
        print(f"   Step 2: 第1段階 - 関連業種から候補取得中...")
        candidates = []

        for i, industry in enumerate(related_industries, 1):
            if industry:
                print(f"      業種{i} 「{industry}」から50件取得中...")
                industry_candidates = self.fetch_candidates_by_industry(
                    industry, cv_point, limit=50, data_period_days=data_period_days
                )
                print(f"      → {len(industry_candidates)}件取得")
                candidates.extend(industry_candidates)
            else:
                # 全業種検索（フォールバック）
                print(f"      全業種から50件取得中...")
                industry_candidates = self.fetch_candidates_by_industry_fallback(
                    cv_point, limit=50, data_period_days=data_period_days
                )
                print(f"      → {len(industry_candidates)}件取得")
                candidates.extend(industry_candidates)

        print(f"   ✅ 第1段階合計: {len(candidates)}件を取得")
        print()

        # Step 3: クリエイティブ情報を取得し、類似度判定（バッチ処理）
        print("   Step 3: 商材内容を分析し、類似度判定中（第1段階）...")
        print(f"   候補{len(candidates)}件を10件ずつ処理...")
        print(f"   各候補から上位5件のクリエイティブを取得...")
        print()

        # まずクリエイティブ情報を全件取得（各5件）
        for campaign in candidates:
            campaign['creative_info'] = self.get_creative_info(campaign['advertiser_name'], limit=5)

        # 10件ずつバッチ判定
        batch_size = 10
        all_campaigns_with_scores = []

        for i in range(0, len(candidates), batch_size):
            batch = candidates[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(candidates) + batch_size - 1) // batch_size
            print(f"   バッチ {batch_num}/{total_batches} 処理中...")

            # バッチ判定
            scored_batch = self.judge_product_similarity_batch(target_product, batch)
            all_campaigns_with_scores.extend(scored_batch)

        print()

        # 閾値以上のものを選定
        filtered_campaigns = self._filter_and_enrich_campaigns(
            all_campaigns_with_scores, cv_point, min_similarity
        )

        print(f"   ✅ 第1段階結果: 類似度{min_similarity}点以上が{len(filtered_campaigns)}件")
        print()

        # Step 4: 結果が不十分なら第2段階実行
        if len(filtered_campaigns) < min_good_results:
            print(f"   ⚠️  結果が{min_good_results}件未満のため、第2段階を実行...")
            print()

            # 第2段階の戦略を決定
            if len(related_industries) > 0 and related_industries[0]:
                # 同じ業種からさらに50件追加
                print(f"   戦略: 同じ業種からさらに候補を追加...")
                additional_candidates = []

                for i, industry in enumerate(related_industries, 1):
                    print(f"      業種{i} 「{industry}」からさらに50件取得中（51-100件目）...")
                    add_cands = self.fetch_candidates_by_industry(
                        industry, cv_point, limit=50, offset=50, data_period_days=data_period_days
                    )
                    print(f"      → {len(add_cands)}件取得")
                    additional_candidates.extend(add_cands)

            else:
                # フォールバックとして全業種からさらに取得
                print(f"   戦略: 全業種からさらに候補を追加...")
                additional_candidates = self.fetch_candidates_by_industry_fallback(
                    cv_point, limit=50, data_period_days=data_period_days
                )
                print(f"      → {len(additional_candidates)}件取得")

            if additional_candidates:
                print(f"   ✅ 第2段階: {len(additional_candidates)}件を追加取得")
                print()

                # 追加候補のクリエイティブ取得と類似度判定
                print("   追加候補の類似度判定中...")
                for campaign in additional_candidates:
                    campaign['creative_info'] = self.get_creative_info(
                        campaign['advertiser_name'], limit=5
                    )

                # バッチ判定
                add_scored = []
                for i in range(0, len(additional_candidates), batch_size):
                    batch = additional_candidates[i:i+batch_size]
                    batch_num = (i // batch_size) + 1
                    total_batches = (len(additional_candidates) + batch_size - 1) // batch_size
                    print(f"   追加バッチ {batch_num}/{total_batches} 処理中...")

                    scored_batch = self.judge_product_similarity_batch(target_product, batch)
                    add_scored.extend(scored_batch)

                print()

                # 追加分をフィルタ
                add_filtered = self._filter_and_enrich_campaigns(
                    add_scored, cv_point, min_similarity
                )

                print(f"   ✅ 第2段階結果: 類似度{min_similarity}点以上が{len(add_filtered)}件追加")
                print()

                # 第1段階と第2段階を統合
                filtered_campaigns.extend(add_filtered)

        # ベンチマークアカウントを結果に追加（類似度100点として）
        if benchmark_account:
            # クリエイティブ情報を取得
            benchmark_account['creative_info'] = self.get_creative_info(
                benchmark_account['advertiser_name'], limit=5
            )
            # 類似度情報を設定
            benchmark_account['similarity_score'] = 100
            benchmark_account['similarity_reason'] = 'ベンチマーク指定アカウント（必ず含める）'
            # CV地点データを付与
            benchmark_account = self._enrich_campaign_with_cv_data(benchmark_account, cv_point)
            # 先頭に追加
            filtered_campaigns.insert(0, benchmark_account)
            print(f"   🎯 ベンチマークアカウント「{benchmark_account['advertiser_name']}」を結果に追加（類似度100点）")
            print()

        # 類似度スコア順に並び替え
        filtered_campaigns.sort(key=lambda x: x.get('similarity_score', 0), reverse=True)

        # 上位20件に絞る
        final_campaigns = filtered_campaigns[:20]

        print(f"   ✅ 最終選定: {len(final_campaigns)}件")
        print()

        if final_campaigns:
            print("   【選定された案件TOP5】")
            for i, camp in enumerate(final_campaigns[:5], 1):
                score = camp.get('similarity_score', 0)
                is_benchmark = camp.get('is_benchmark', False)
                benchmark_mark = " 🎯" if is_benchmark else ""
                print(f"   {i}. {camp['advertiser_name'][:50]}{benchmark_mark}")
                print(f"      類似度: {score}点, 売上: ¥{camp['total_sales']:,.0f}")
                print(f"      理由: {camp.get('similarity_reason', '')[:60]}...")
                print()

        return final_campaigns

    def _enrich_campaign_with_cv_data(self, campaign: Dict, cv_point: str) -> Dict:
        """
        単一のキャンペーンにCV地点データを付与

        Args:
            campaign: キャンペーンデータ
            cv_point: CV地点

        Returns:
            エンリッチされたキャンペーンデータ
        """
        # クリエイティブ情報を展開（複数ある場合は1つ目を代表として使用）
        creatives = campaign.get('creative_info', [])
        if creatives and len(creatives) > 0:
            campaign['creative_title'] = creatives[0].get('title', '')
            campaign['creative_body'] = creatives[0].get('body', '')
        else:
            campaign['creative_title'] = ''
            campaign['creative_body'] = ''

        # CV地点ごとのデータ
        cv_counts = {
            'AddToCart': campaign.get('cv_addtocart', 0),
            'Install': campaign.get('cv_install', 0),
            'Purchase': campaign.get('cv_purchase', 0),
            'WebConversion': campaign.get('cv_webconversion', 0),
            'CompleteRegistration': campaign.get('cv_registration', 0),
            'ViewContent': campaign.get('cv_viewcontent', 0)
        }
        main_cv_count = cv_counts.get(cv_point, campaign.get('total_cv', 0))
        main_cv_cpa = campaign['total_sales'] / main_cv_count if main_cv_count > 0 else 0

        campaign['main_cv_point'] = cv_point
        campaign['main_cv_count'] = main_cv_count
        campaign['main_cv_cpa'] = main_cv_cpa

        return campaign

    def _filter_and_enrich_campaigns(self, campaigns: List[Dict], cv_point: str,
                                       min_similarity: int) -> List[Dict]:
        """
        キャンペーンを類似度でフィルタし、CV地点データを付与

        Args:
            campaigns: スコア付き候補リスト
            cv_point: CV地点
            min_similarity: 最低類似度

        Returns:
            フィルタ＆エンリッチされたキャンペーンリスト
        """
        filtered = []
        for campaign in campaigns:
            similarity_score = campaign.get('similarity_score', 0)

            if similarity_score >= min_similarity:
                # CV地点データを付与
                campaign = self._enrich_campaign_with_cv_data(campaign, cv_point)
                filtered.append(campaign)

        return filtered

    def get_industry_benchmarks(self, industry: str, data_period_days: int = 180) -> Dict:
        """業種別ベンチマークデータを取得（v2と同じ）"""
        print("【3】業種別ベンチマーク取得中...")

        query = f"""
        SELECT
            industry,
            COUNT(DISTINCT ad_account_id) as total_accounts,
            AVG(cpc) as avg_cpc,
            AVG(SAFE_DIVIDE(click, vimp)) as avg_ctr,
            APPROX_QUANTILES(cpc, 100)[OFFSET(50)] as median_cpc,
            APPROX_QUANTILES(SAFE_DIVIDE(click, vimp), 100)[OFFSET(50)] as median_ctr
        FROM `{self.project_id}.best_practices_dev.daily_std_ad_account_performance`
        WHERE industry = '{industry}'
          AND dt >= DATE_SUB(CURRENT_DATE(), INTERVAL {data_period_days} DAY)
          AND vimp > 0
        GROUP BY industry
        """

        try:
            query_job = self.bq_client.query(query)
            results = list(query_job.result())

            if results:
                row = results[0]
                benchmarks = {
                    'industry': row['industry'],
                    'total_accounts': int(row['total_accounts']) if row['total_accounts'] else 0,
                    'avg_cpc': float(row['avg_cpc']) if row['avg_cpc'] else 0,
                    'avg_ctr': float(row['avg_ctr']) * 100 if row['avg_ctr'] else 0,
                    'median_cpc': float(row['median_cpc']) if row['median_cpc'] else 0,
                    'median_ctr': float(row['median_ctr']) * 100 if row['median_ctr'] else 0
                }

                print(f"✅ 業種別ベンチマーク取得完了")
                print(f"   平均CPC: ¥{benchmarks['avg_cpc']:.2f}")
                print(f"   平均CTR: {benchmarks['avg_ctr']:.3f}%")
                print()

                return benchmarks
            else:
                print(f"⚠️  業種 '{industry}' のベンチマークデータが見つかりませんでした")
                print()
                return {}

        except Exception as e:
            print(f"⚠️  ベンチマーク取得エラー: {e}")
            print()
            return {}

    def create_campaigns_summary_table(self, campaigns: List[Dict], cv_point: str) -> str:
        """案件一覧を表形式で作成（v3: 類似度スコア付き）"""
        if not campaigns:
            return "（該当案件なし）"

        lines = []
        lines.append("\n" + "=" * 150)
        lines.append("📋 参考案件一覧（過去6ヶ月実績・類似度スコア順）")
        lines.append("=" * 150)
        lines.append("")

        # ヘッダー
        header = f"{'No':<4} {'広告主名':<35} {'代理店':<20} {'期間':<20} {'売上(円)':<15} {'類似度':<8} {'主CV地点':<15} {'CV数':<10} {'CPA(円)':<12}"
        lines.append(header)
        lines.append("-" * 150)

        # データ行
        total_sales = 0
        total_cpa = 0

        for i, camp in enumerate(campaigns, 1):
            adv_name = camp['advertiser_name'][:35]
            agency = camp['agency_name'][:20]
            period = f"{camp.get('start_month', '')}〜{camp.get('end_month', '')}"
            sales = camp['total_sales']
            similarity = camp.get('similarity_score', 0)
            cv_count = camp.get('main_cv_count', camp['total_cv'])
            cpa = camp.get('main_cv_cpa', camp['avg_cpa'])
            reason = camp.get('similarity_reason', '')
            is_benchmark = camp.get('is_benchmark', False)

            # 各案件の実際の主要CV地点を特定
            cv_data = {
                'Purchase': camp.get('cv_purchase', 0),
                'AddToCart': camp.get('cv_addtocart', 0),
                'Install': camp.get('cv_install', 0),
                'WebConversion': camp.get('cv_webconversion', 0),
                'CompleteRegistration': camp.get('cv_registration', 0),
                'ViewContent': camp.get('cv_viewcontent', 0)
            }
            # CV数が最も多い地点を特定
            actual_cv_point = max(cv_data, key=cv_data.get) if max(cv_data.values()) > 0 else cv_point
            actual_cv_count = cv_data[actual_cv_point]

            # 検索CV地点と実際のCV地点が異なる場合は実際のCV地点を使用
            if actual_cv_count > 0 and cv_data[cv_point] == 0:
                display_cv_point = actual_cv_point
                display_cv_count = actual_cv_count
            else:
                display_cv_point = cv_point
                display_cv_count = cv_count

            total_sales += sales
            total_cpa += cpa

            # ベンチマークマーク
            benchmark_mark = " 🎯" if is_benchmark else ""

            line = f"{i:<4} {adv_name:<35} {agency:<20} {period:<20} {sales:<15,.0f} {similarity:<8} {display_cv_point:<15} {display_cv_count:<10,} {cpa:<12,.0f}"
            lines.append(line + benchmark_mark)

            # 選定理由を次の行に表示（インデント付き）
            if reason:
                # 理由が長い場合は適度に改行
                reason_wrapped = reason[:120] + "..." if len(reason) > 120 else reason
                lines.append(f"     └ 理由: {reason_wrapped}")

        lines.append("-" * 150)
        avg_sales = total_sales / len(campaigns) if campaigns else 0
        avg_cpa = total_cpa / len(campaigns) if campaigns else 0
        lines.append(f"合計: {len(campaigns)}件 | 平均売上: ¥{avg_sales:,.0f} | 平均CPA: ¥{avg_cpa:,.0f}")
        lines.append("=" * 150)

        return "\n".join(lines)

    def analyze_with_claude(self, target_product: Dict, campaigns: List[Dict],
                            benchmarks: Dict, cv_point: str) -> str:
        """Claude Haiku 4.5で分析・インサイト生成（v3: 商材類似度を考慮）"""
        print("【4】Claude Haiku 4.5で分析中...")

        # データを整形
        campaigns_summary = self._format_campaigns_for_analysis_v3(campaigns[:10], cv_point)
        benchmarks_summary = self._format_benchmarks(benchmarks)

        prompt = f"""あなたは営業提案支援のデータアナリストです。
以下の過去実績データを分析し、顧客提案に活用できるインサイトを生成してください。

# 検索対象の商材
カテゴリー: {target_product.get('category', '不明')}
説明: {target_product.get('description', '')}

# 選定された類似案件（上位10件）
※各案件は商材内容を分析し、類似度60点以上のもののみを選定済み
{campaigns_summary}

# 業種別ベンチマーク
{benchmarks_summary}

# CV地点
{cv_point} ({CV_POINTS.get(cv_point, '')})

# 分析してほしいこと
1. **成功事例の特徴**: 類似商材で成果を出している案件の共通点
2. **商材特性による違い**: 商材内容によるパフォーマンスの違い
3. **代理店の傾向**: どの代理店が類似商材で成果を出しているか
4. **注意点**: 避けるべきパターンや懸念事項

# 出力形式
## 📊 過去実績分析レポート（商材類似度重視）
**検索商材**: {target_product.get('category', '不明')}
**最適化CV地点**: {cv_point} ({CV_POINTS.get(cv_point, '')})

---

### 1. 成功事例の特徴（商材類似度60点以上）
- （箇条書きで3-5点、具体的な数値と月情報を含める）

### 2. 商材特性による違い
- （類似商材でのパフォーマンス傾向を分析）

### 3. 代理店の傾向
- （箇条書きで2-3点）

### 4. 注意点
- （リスクや懸念事項を2-3点）

---

**分析日**: {self._get_today()}
**データ期間**: 過去180日
**サンプル数**: {len(campaigns)}件（類似度60点以上）
"""

        # Claude Haiku 4.5で分析
        response = self.claude.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2500,
            messages=[{"role": "user", "content": prompt}]
        )

        insights = response.content[0].text

        print("✅ 分析完了")
        print()

        return insights

    def search(self, hearing_data: Dict) -> PerformanceSearchResult:
        """
        メイン実行: v3の検索フロー
        """
        print("\n" + "=" * 80)
        print("🚀 過去実績検索Agent v3 実行開始")
        print("=" * 80)
        print()

        # データ参照期間の取得（デフォルト180日）
        data_period_days = 180
        if 'q_data_period' in hearing_data:
            try:
                data_period_days = int(hearing_data['q_data_period'])
                print(f"📅 カスタム参照期間: 過去{data_period_days}日間")
            except (ValueError, TypeError):
                print(f"⚠️  無効な参照期間指定、デフォルト180日を使用")
                data_period_days = 180
        elif 'data_period_days' in hearing_data:
            try:
                data_period_days = int(hearing_data['data_period_days'])
                print(f"📅 カスタム参照期間: 過去{data_period_days}日間")
            except (ValueError, TypeError):
                print(f"⚠️  無効な参照期間指定、デフォルト180日を使用")
                data_period_days = 180
        print()

        # ステップ0: ベンチマークアカウントの取得（指定がある場合）
        benchmark_account = None
        benchmark_name = hearing_data.get('q_benchmark') or hearing_data.get('benchmark')
        if benchmark_name and benchmark_name.strip():
            print(f"🎯 ベンチマークアカウント指定あり: {benchmark_name}")
            benchmark_account = self.fetch_benchmark_account(
                benchmark_name.strip(), data_period_days=data_period_days
            )
            if benchmark_account:
                print(f"✅ ベンチマークアカウント取得成功: {benchmark_account['advertiser_name']}")
                print(f"   業種: {benchmark_account['industry']}, 売上: ¥{benchmark_account['total_sales']:,.0f}")
            else:
                print(f"⚠️  ベンチマークアカウント「{benchmark_name}」が見つかりませんでした")
            print()

        # ステップ1: 商材情報抽出
        product_info = self.extract_product_info(hearing_data)

        # ステップ1.5: Web検索で商材情報を補強
        product_info = self.enrich_product_info_with_web(hearing_data, product_info)

        # ステップ2: CV地点推察
        cv_point = self.infer_cv_point(hearing_data)
        print(f"   CV地点: {cv_point} ({CV_POINTS.get(cv_point, '')})")
        print()

        # ステップ3: v3検索（2段階: 広く取得→類似度フィルタ）
        similar_campaigns = self.search_similar_campaigns_v3(
            product_info,
            cv_point,
            min_similarity=60,  # 60点以上
            benchmark_account=benchmark_account,  # ベンチマークアカウントを渡す
            data_period_days=data_period_days  # 参照期間を渡す
        )

        # ステップ3.5: Claudeの知識で類似度を再評価（バッチ処理）
        if similar_campaigns:
            similar_campaigns = self.reevaluate_similarity_with_web_batch(
                product_info,
                similar_campaigns,
                top_n=20  # 上位20件を再評価
            )

        # ステップ4: 業種別ベンチマーク取得（代表業種）
        representative_industry = similar_campaigns[0]['industry'] if similar_campaigns else '不明'
        benchmarks = self.get_industry_benchmarks(
            representative_industry, data_period_days=data_period_days
        ) if representative_industry != '不明' else {}

        # ステップ5: 成功事例抽出（類似度TOP3）
        success_stories = similar_campaigns[:3] if similar_campaigns else []

        # ステップ6: 案件一覧表作成
        campaigns_table = self.create_campaigns_summary_table(similar_campaigns, cv_point)

        # ステップ7: Claude Haiku 4.5で分析
        insights = self.analyze_with_claude(product_info, similar_campaigns, benchmarks, cv_point)

        # 結果をまとめる
        result = PerformanceSearchResult(
            industry=representative_industry,
            product_category=product_info.get('category', '不明'),
            cv_point=cv_point,
            similar_campaigns=similar_campaigns,
            industry_benchmarks=benchmarks,
            success_stories=success_stories,
            campaigns_summary_table=campaigns_table,
            insights=insights,
            query_metadata={
                'product_info': product_info,
                'cv_point': cv_point,
                'total_campaigns_found': len(similar_campaigns),
                'min_similarity_score': 60,
                'data_period_days': data_period_days,
                'timestamp': self._get_today()
            }
        )

        print("=" * 80)
        print("✅ 過去実績検索Agent v3 実行完了")
        print("=" * 80)

        return result

    # ────────────────────────────────────────────────────
    # ヘルパー関数
    # ────────────────────────────────────────────────────

    def _format_hearing_data(self, hearing_data: Dict) -> str:
        """ヒアリングデータをテキスト化"""
        lines = []

        if 'basic_info' in hearing_data:
            basic = hearing_data['basic_info']
            lines.append(f"## 基本情報")
            lines.append(f"- 案件名: {basic.get('project_name', '不明')}")
            lines.append(f"- 広告主名: {basic.get('advertiser_name', '不明')}")
            lines.append(f"- 代理店名: {basic.get('agency_name', '不明')}")
            lines.append(f"- 案件タイプ: {basic.get('campaign_type', '不明')}")
            lines.append("")

        lines.append("## ヒアリング内容")
        for key, value in hearing_data.items():
            if key.startswith('q') and value:
                lines.append(f"- {value}")

        return '\n'.join(lines)

    def _extract_json(self, text: str) -> Dict:
        """テキストからJSON抽出"""
        try:
            start = text.find('{')
            end = text.rfind('}') + 1
            if start >= 0 and end > start:
                json_str = text[start:end]
                return json.loads(json_str)
        except:
            pass
        return {}

    def _format_campaigns_for_analysis_v3(self, campaigns: List[Dict], cv_point: str) -> str:
        """キャンペーンデータを分析用に整形（v3: 類似度情報+複数クリエイティブ）"""
        lines = []
        for i, camp in enumerate(campaigns, 1):
            lines.append(f"{i}. {camp['advertiser_name']}")
            lines.append(f"   - 類似度スコア: {camp.get('similarity_score', 0)}点")
            lines.append(f"   - 理由: {camp.get('similarity_reason', '')}")
            lines.append(f"   - 代理店: {camp['agency_name']}")
            lines.append(f"   - 期間: {camp.get('start_month', '')}〜{camp.get('end_month', '')}")
            lines.append(f"   - 売上: ¥{camp['total_sales']:,.0f}")
            lines.append(f"   - CV({cv_point}): {camp.get('main_cv_count', 0):,}件")
            lines.append(f"   - CPA({cv_point}): ¥{camp.get('main_cv_cpa', 0):,.0f}")

            # 複数クリエイティブを表示（最大3件）
            creatives = camp.get('creative_info', [])
            if creatives and len(creatives) > 0:
                lines.append(f"   - クリエイティブ:")
                for j, creative in enumerate(creatives[:3], 1):
                    title = creative.get('title', '')[:70]
                    lines.append(f"     {j}. {title}")

            lines.append("")
        return '\n'.join(lines)

    def _format_benchmarks(self, benchmarks: Dict) -> str:
        """ベンチマークデータを整形"""
        if not benchmarks:
            return "（データなし）"

        lines = [
            f"- 業種: {benchmarks.get('industry', '不明')}",
            f"- 総アカウント数: {benchmarks.get('total_accounts', 0):,}件",
            f"- 平均CPC: ¥{benchmarks.get('avg_cpc', 0):.2f}",
            f"- 平均CTR: {benchmarks.get('avg_ctr', 0):.3f}%"
        ]
        return '\n'.join(lines)

    def parse_advertiser_name(self, advertiser_name: str) -> Tuple[str, str]:
        """
        アカウント名から広告主名と商材名を抽出

        フォーマット: 広告主名(商材名)_代理店名

        例:
        - "株式会社GA technologies(RENOSY ASSET)_CA"
          -> ("株式会社GA technologies", "RENOSY ASSET")
        """
        import re
        match = re.match(r'([^(]+)\(([^)]+)\)', advertiser_name)
        if match:
            advertiser = match.group(1).strip()
            product = match.group(2).strip()
            return advertiser, product
        else:
            # パースできない場合は全体を広告主名として返す
            return advertiser_name, ""

    def fetch_campaign_info_from_web(self, advertiser: str, product: str) -> str:
        """
        Claudeの知識ベースで案件の商材情報を取得

        Args:
            advertiser: 広告主名
            product: 商材名

        Returns:
            商材情報の説明文
        """
        if not product:
            return ""

        prompt = f"""あなたは商材リサーチの専門家です。以下の商材について、あなたの知識から情報を提供してください。

# 商材情報
- 広告主: {advertiser}
- 商材名: {product}

# 提供してほしい情報
この商材の主な特徴、用途、ターゲット層を簡潔に（3-5文）記載してください。
もし知識がない場合は、商材名から推測される内容を記載してください。

出力は説明文のみで、JSONなどの形式は不要です。
"""

        try:
            response = self.claude.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=500,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text.strip()
        except Exception as e:
            print(f"⚠️  Web情報取得エラー ({product}): {e}")
            return ""

    def reevaluate_similarity_with_web_batch(self, target_product: Dict,
                                               campaigns: List[Dict],
                                               top_n: int = 20) -> List[Dict]:
        """
        Claudeの知識ベースを使って類似度をバッチ再評価（高速版）
        作品タイトルがある場合は、クリエイティブに使われている作品ジャンルも考慮

        Args:
            target_product: 検索対象商材（Web情報含む）
            campaigns: 候補案件リスト
            top_n: 再評価する上位件数

        Returns:
            再評価後の案件リスト
        """
        print("\n【最終確認】Claudeの知識で類似度を再評価中...")
        print(f"   上位{min(top_n, len(campaigns))}件をバッチ処理...")

        # 上位N件のみ再評価
        campaigns_to_reevaluate = campaigns[:top_n]
        other_campaigns = campaigns[top_n:]

        # アカウント名をパースして候補リストを作成
        candidates_info = []
        for i, campaign in enumerate(campaigns_to_reevaluate):
            advertiser, product = self.parse_advertiser_name(campaign['advertiser_name'])

            if product:
                # クリエイティブ情報を取得（既に取得済み）
                creatives = campaign.get('creative_info', [])
                creative_titles = [c.get('title', '') for c in creatives[:3]]  # 上位3件

                candidates_info.append({
                    'index': i,
                    'advertiser': advertiser,
                    'product': product,
                    'original_score': campaign.get('similarity_score', 0),
                    'original_reason': campaign.get('similarity_reason', '')[:100],
                    'creative_titles': creative_titles
                })
            else:
                # パースできない場合はスキップ
                campaign['web_checked'] = False
                campaign['reevaluated'] = False

        if not candidates_info:
            print("   ⚠️  再評価対象なし")
            return campaigns

        # 検索対象に作品タイトルが含まれているかチェック
        target_category = target_product.get('category', '')
        target_description = target_product.get('enriched_description', target_product.get('description', ''))

        # バッチで再評価
        candidates_text = "\n".join([
            f"{i+1}. 広告主: {c['advertiser']}, 商材: {c['product']}, 元スコア: {c['original_score']}点\n" +
            (f"   クリエイティブ例: {', '.join([t[:30] for t in c['creative_titles'] if t][:2])}" if c['creative_titles'] else "")
            for i, c in enumerate(candidates_info)
        ])

        # 作品タイトルベースの評価が必要かどうか判定
        has_content_title = any(keyword in target_category.lower() for keyword in ['漫画', 'アニメ', '動画', 'ゲーム', '小説', '作品'])

        if has_content_title:
            # コンテンツ型商材の場合、作品ジャンルも考慮
            prompt = f"""あなたは商材分析の専門家です。以下の検索対象商材と候補商材の類似度を、あなたの知識を使って再評価してください。

# 検索対象商材
カテゴリー: {target_category}
説明: {target_description}

**重要**: この商材は漫画/アニメ/ゲーム等のコンテンツ商材です。
各候補案件のクリエイティブに使われている作品タイトルとジャンルを確認し、
検索対象の作品と**ジャンルが一致しているか**を重視して再評価してください。

例：
- 検索対象が「異世界転生ファンタジー」の場合
  → クリエイティブに「異世界転生」「ファンタジー」系の作品が使われていればスコアアップ
  → クリエイティブに「恋愛」「スポーツ」系の作品が使われていればスコアダウン

# 候補商材リスト（{len(candidates_info)}件）
{candidates_text}

各候補について：
1. クリエイティブから使用されている作品タイトルとジャンルを推定
2. 検索対象の作品ジャンルとの一致度を判定
3. 元のスコアと大きく異なる場合（±15点以上）のみ、新スコアと理由を記載

JSON配列で出力（スコア変更が必要な候補のみ）:
[
  {{
    "index": 候補番号（1から始まる）,
    "new_score": 新しい類似度（0-100）,
    "reason": "再評価理由（使用作品のジャンルを明記、100文字以内）"
  }}
]

スコア変更が不要な場合は空配列 [] を返してください。
"""
        else:
            # 通常の商材の場合
            prompt = f"""あなたは商材分析の専門家です。以下の検索対象商材と候補商材の類似度を、あなたの知識を使って再評価してください。

# 検索対象商材
カテゴリー: {target_category}
説明: {target_description}

# 候補商材リスト（{len(candidates_info)}件）
{candidates_text}

各候補について、あなたの知識から：
1. その商材が実際に何かを判断
2. 検索対象との類似度を0-100点で再評価
3. 元のスコアと大きく異なる場合（±15点以上）のみ、新スコアと理由を記載

JSON配列で出力（スコア変更が必要な候補のみ）:
[
  {{
    "index": 候補番号（1から始まる）,
    "new_score": 新しい類似度（0-100）,
    "reason": "再評価理由（100文字以内）"
  }}
]

スコア変更が不要な場合は空配列 [] を返してください。
"""

        try:
            response = self.claude.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}]
            )

            # JSON配列を抽出
            response_text = response.content[0].text
            # JSONの開始位置を探す
            start = response_text.find('[')
            end = response_text.rfind(']') + 1

            if start >= 0 and end > start:
                json_str = response_text[start:end]
                updates = json.loads(json_str)

                # スコアを更新
                update_count = 0
                for update in updates:
                    idx = update.get('index', 0) - 1  # 1-indexedから0-indexedに変換
                    if 0 <= idx < len(candidates_info):
                        campaign_idx = candidates_info[idx]['index']
                        campaign = campaigns_to_reevaluate[campaign_idx]

                        old_score = campaign.get('similarity_score', 0)
                        new_score = update.get('new_score', old_score)
                        reason = update.get('reason', '')

                        # スコア更新
                        campaign['similarity_score'] = new_score
                        campaign['similarity_reason'] = f"[再評価] {reason}"
                        campaign['reevaluated'] = True
                        campaign['web_checked'] = True
                        update_count += 1

                        print(f"   {campaign_idx+1}. {candidates_info[idx]['product'][:30]}: {old_score}点 → {new_score}点")

                if update_count == 0:
                    print("   ✅ 全候補が妥当と判断（スコア変更なし）")
                else:
                    print(f"   ✅ {update_count}件のスコアを更新")

                # 未変更の案件にフラグを設定
                for i, campaign in enumerate(campaigns_to_reevaluate):
                    if not campaign.get('reevaluated', False) and not campaign.get('web_checked', False):
                        campaign['web_checked'] = True
                        campaign['reevaluated'] = False

            else:
                print("   ⚠️  JSON解析失敗、元のスコアを維持")
                for campaign in campaigns_to_reevaluate:
                    campaign['web_checked'] = False
                    campaign['reevaluated'] = False

        except Exception as e:
            print(f"   ⚠️  再評価エラー: {e}")
            for campaign in campaigns_to_reevaluate:
                campaign['web_checked'] = False
                campaign['reevaluated'] = False

        # 再評価後のスコアで並び替え
        all_campaigns = campaigns_to_reevaluate + other_campaigns
        all_campaigns.sort(key=lambda x: x.get('similarity_score', 0), reverse=True)

        print(f"✅ 再評価完了\n")

        return all_campaigns

    def _get_today(self) -> str:
        """今日の日付を取得"""
        return datetime.now().strftime('%Y-%m-%d')


if __name__ == "__main__":
    print("過去実績検索Agent v3")
    print("test_past_performance_agent_v3.py から実行してください")
