# 類似実績探索 Agent v3

商材内容ベースで類似案件を発掘する、次世代の類似実績探索システム。

## 機能

### v3 の新機能
- ✨ **商材内容ベースの類似判定**: 業種を超えた検索が可能
- 🧠 **Web情報活用**: 商材の詳細情報を自動取得
- 🎨 **クリエイティブ分析**: 広告クリエイティブ内容を考慮
- 📊 **類似度スコア**: 0-100点で客観的評価
- 🎯 **ベンチマーク機能**: 特定アカウントを必ず結果に含める
- 📅 **参照期間カスタマイズ**: 180日〜537日

### 検索プロセス
1. **ヒアリング情報収集**: 商材内容、予算、KPI、ターゲット等
2. **商材カテゴリ判定**: Claude による商材分類
3. **類似案件検索**: BigQuery で広告実績データを検索
4. **類似度判定**: Claude で商材の類似度をスコアリング
5. **結果ランキング**: スコア順に並び替えて提示

## 開発環境での実行

```bash
# 依存関係のインストール
pip install -r requirements.txt

# サービスアカウントキーを配置
# service-account-key.json を同じディレクトリに配置

# ANTHROPIC_API_KEY を環境変数に設定
export ANTHROPIC_API_KEY="your-api-key"

# 実行
streamlit run app.py
```

## Streamlit Cloud へのデプロイ

1. GitHubリポジトリにプッシュ
2. [Streamlit Cloud](https://streamlit.io/cloud) でアプリを作成
3. Secrets に以下を設定:

```toml
ANTHROPIC_API_KEY = "your-anthropic-api-key"
service_account_base64 = "base64-encoded-service-account-key"
```

### サービスアカウントキーのBase64エンコード

```bash
base64 -i service-account-key.json | pbcopy
```

## データソース

- **BigQuery**: `jp-sales-enablement.best_practices_dev.daily_std_ad_account_performance`
- **Claude Haiku 4.5**: 商材分類・類似度判定
- **Web検索**: 商材の詳細情報取得（オプション）

## 技術スタック

- **Python 3.11**
- **Streamlit**: Web UI
- **Anthropic Claude Haiku 4.5**: LLM推論
- **Google BigQuery**: 広告実績データ取得
- **pandas**: データ処理

## ライセンス

Internal use only - SmartNews Inc.
