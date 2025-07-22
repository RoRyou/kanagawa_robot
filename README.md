# Robot Product Research Pipeline

指定した **企業名リスト** を入力すると、関連する Web／PDF 記事を自動で収集・解析し、  
ロボット製品情報を CSV（`robot_analysis_result.csv`）として出力するパイプラインです。  
競合調査・市場分析・PoC 支援に活用できます。

---

## 特長

1. **Google カスタム検索** で企業 × ロボット分類キーワードの URL を一括取得  
2. HTML／PDF からテキストを抽出し、日本語向けにクリーンアップ  
3. **Azure OpenAI (GPT‑4o)** により  
   - ロボット分類  
   - 使用用途  
   - 技術領域  
   - 協業実績  
   - 製品情報／実証実験  
   を JSON 形式で抽出  
4. pandas で整形・表記揺れ正規化・要約生成  
5. 企業 × 製品単位で集計し、CSV へ保存  

---

## セットアップ

```bash
git clone <YOUR_REPO_URL>
cd <YOUR_REPO_DIR>
python -m venv .venv
source .venv/bin/activate       # Windows は .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env            # API キーを編集
