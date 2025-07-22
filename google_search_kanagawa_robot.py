# Package imports
import os
import pandas as pd
import re
import json
from time import sleep
from googleapiclient.discovery import build
import requests
from bs4 import BeautifulSoup
from markdownify import markdownify as md
import pdfplumber
from openai import AzureOpenAI
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import warnings
from typing import List, Tuple, Optional

warnings.simplefilter(action='ignore', category=FutureWarning)

# Config
os.environ["azure_api_key"] = "e87c4053cc814a338a2090130c2424c7"
os.environ["azure_api_version"] = "2023-06-01-preview"
os.environ["azure_azure_endpoint"] = "https://cog-openai-test-dev.openai.azure.com/"
os.environ["GOOGLE_API_KEY"] = "AIzaSyBkaSb5MwHK9gG-Q90xeRhuRAUq3p990Ts"
os.environ["CUSTOM_SEARCH_ENGINE_ID"] = "d057b4dee799c420d"

client = AzureOpenAI(
    api_version="2023-06-01-preview",
    api_key="e87c4053cc814a338a2090130c2424c7",
    azure_endpoint="https://cog-openai-test-dev.openai.azure.com/"
)

GOOGLE_API_KEY = "AIzaSyBkaSb5MwHK9gG-Q90xeRhuRAUq3p990Ts"
CUSTOM_SEARCH_ENGINE_ID = "d057b4dee799c420d"
DATA_DIR = 'data'

# =============================================================================
# 基本ユーティリティ関数
# =============================================================================

def clean_text(text):
    """テキストをクリーニング"""
    cleaned_text = re.sub(r'\n{2,}', '\n', text)
    cleaned_text = re.sub(r'\s{2,}', ' ', cleaned_text)
    cleaned_text = '\n'.join([line.strip() for line in cleaned_text.split('\n') if line.strip()])
    return cleaned_text

# =============================================================================
# 検索・データ取得関数
# =============================================================================

def get_search_results(keyword, site_url=None, pages=5):
    """
    統一的なGoogle検索関数
    
    Args:
        keyword: 検索キーワード
        site_url: 限定サイトURL（オプション）
        pages: 返すページ数（site_urlがNoneの場合に使用）
    
    Returns:
        list: URLリスト
    """
    service = build("customsearch", "v1", developerKey=GOOGLE_API_KEY)
    urls = []
    
    if site_url:
        # site付き検索（元getSearchResponse）
        page_limit = 10
        start_index = 1
        
        for n_page in range(0, page_limit):
            try:
                sleep(1)
                query = f'{keyword} site:{site_url}'
                res = service.cse().list(
                    q=query,
                    cx=CUSTOM_SEARCH_ENGINE_ID,
                    lr='lang_ja',
                    num=10,
                    start=start_index
                ).execute()
                
                urls.extend(item['link'] for item in res.get('items', []))
                
                # 次のページがあるかチェック
                if "nextPage" in res.get("queries", {}):
                    start_index = res.get("queries").get("nextPage")[0].get("startIndex")
                else:
                    break
                    
            except Exception as e:
                print(f"検索エラー: {e}")
                break
    else:
        # siteなし検索（元getSearchResponse_no_url）
        try:
            sleep(1)
            res = service.cse().list(
                q=keyword,
                cx=CUSTOM_SEARCH_ENGINE_ID,
                lr='lang_ja',
                num=pages,
                start=1
            ).execute()
            
            urls.extend(item['link'] for item in res.get('items', []))
            
        except Exception as e:
            print(f"検索エラー: {e}")
    
    return urls

def getTextFromUrl(url, timeout=10):
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        for element in soup(['header', 'footer', 'nav', 'aside', 'script', 'style']):
            element.decompose()
        
        text = soup.get_text(separator='\n')
        return clean_text(text.strip())
    except requests.exceptions.Timeout:
        print(f'Timeout occurred while fetching {url}')
        return None
    except Exception as e:
        print(f'Error fetching {url}: {e}')
        return None

def get_pdf_text(url, pagenum=5, timeout=10):
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        with open('temp.pdf', 'wb') as temp_pdf:
            temp_pdf.write(response.content)
        with pdfplumber.open('temp.pdf') as pdf:
            text = ''
            for num, page in enumerate(pdf.pages):
                if num >= pagenum:
                    break
                text += page.extract_text() + '\n'
        os.remove('temp.pdf')
        return text
    except requests.exceptions.Timeout:
        print(f'Timeout occurred while fetching PDF {url}')
        return None
    except Exception as e:
        print(f'Error extracting text from PDF {url}: {e}')
        return None

def getAllTextFromUrls(urls):
    all_text = {}
    for url in urls:
        print("データ取得:", url)
        if url.endswith('.pdf'):
            text = get_pdf_text(url, 5)
        else:
            text = getTextFromUrl(url)
        if text:
            all_text[url] = clean_text(text)
    return all_text

# =============================================================================
# GPT処理関数
# =============================================================================

def make_gpt_prompt(input_text, company, product=None):
    """
    GPTプロンプトを生成する統一関数
    
    Args:
        input_text: 入力テキスト
        company: 会社名
        product: 製品名（オプション、Phase 3で使用）
    
    Returns:
        str: フォーマットされたプロンプト
    """
    base_prompt = f"""/system

あなたはプロのリサーチ・アシスタントで、私の仕事を手伝うことです。
私の仕事は、論点を整理したきれいなデータセットを作成することです。
これから、ロボットの製品に関する記事を挙げます。"""

    if product is None:
        # Phase 1: 製品識別
        prompt = base_prompt + f"""
記事の中から、{company}のロボット製品について、ロボットが以下の#分類項目に当てはまるか確認してください。
#分類項目
[移動作業型ロボット,人間装着型ロボット,搭乗型ロボット,コミュニケーション型ロボット,汎用型ロボット,産業用ロボット]
あてはまる場合、ロボットの分類、使用用途と技術領域、協業実績、製品情報をきちんとフォーマットされた文字列形式（strings）のJSONで、返してください。
使用用途の例は以下になります。
[清掃、警備、案内、配膳、搬送、自動運転車、ドローン、マッスルスーツ、アシストスーツ、車いす、モビリティ、コミュニケーション、ペット、人型ロボット、その他]
記事に該当するものがない場合は空白で返してください
出力はJSONのみにしてください

出力の例です。
{{
    "分類": "移動作業型ロボット",
    "使用用途": "配膳",
    "技術領域": "医療",
    "協業実績": "株式会社トヨタ",
    "製品情報": "小型不整地移動クローラユニット"
}}"""
    else:
        # Phase 3: 製品詳細
        prompt = base_prompt + f"""
記事の中に、{company}の{product}のロボット製品に関する情報が存在するかどうか確認してください。
存在する場合、使用用途と技術領域、協業実績、製品情報、実証実験、製品の説明をきちんとフォーマットされた文字列形式（strings）のJSONで、返してください。
使用用途の例は以下になります。
[清掃、警備、案内、配膳、搬送、自動運転車、ドローン、マッスルスーツ、アシストスーツ、車いす、モビリティ、コミュニケーション、ペット、人型ロボット、その他]
技術領域の例は以下になります。
[医療、インフラ、航空・宇宙、介護・福祉、物流・運送、農林水産業、商業施設・宿泊施設、その他]
記事に該当するものがない場合は空白で返してください
出力はJSONのみにしてください

出力の例です。
{{
    "企業名": "{company}",
    "製品名": "{product}",
    "分類": "移動作業型ロボット",
    "使用用途": "配膳",
    "技術領域": "医療",
    "協業実績": "株式会社トヨタ",
    "製品情報": "小型不整地移動クローラユニット",
    "実証実験": "医療法人XYZ病院にて、2024年4月から6月までの3ヶ月間、病院内での食事や医薬品の配達を行う実証実験を実施",
    "製品の説明": "これは最新の医療用配膳ロボットで、不整地でも安定した移動が可能です。"
}}"""
    
    return prompt + f"\n\n/記事\n\n{input_text}"

def get_json_from_response_gpt4o(response):
    try:
        obj = json.loads(response)
        return obj, None
    except:
        try:
            out = json.loads(response.strip('```json\n').strip('\n```'))
            return out, None
        except:
            try:
                text = response.replace('json', '').replace('`', '')
                json_array_str = f'[{text}]'
                json_array_str = re.sub(r'\n', '', json_array_str)
                out = json.loads(json_array_str)
                return out, None
            except:
                print("Response was:", response)
                return [], response

def extract_arguments_gpt(input_text, company, product=None, return_raw=False):
    """
    統一的なGPT引数抽出関数
    
    Args:
        input_text: 入力テキスト
        company: 会社名
        product: 製品名（オプション）
        return_raw: 生のレスポンス文字列を返すかどうか
    
    Returns:
        return_raw=Trueの場合: 生のレスポンス文字列
        それ以外: (解析済みオブジェクト, エラーメッセージ)のタプル
    """
    prompt = make_gpt_prompt(input_text, company, product)
    
    result_llm = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "system", "content": prompt}]
    )
    
    response = result_llm.choices[0].message.content
    
    if return_raw:
        return response
    else:
        return get_json_from_response_gpt4o(response)

# =============================================================================
# Phase 1-4 パイプライン関数
# =============================================================================

def phase1_collect_urls(company_list, max_workers=3):
    """段階1: 全企業のURL収集"""
    print("📡 段階1: 全企業のURL収集中...")
    stage1_start = time.time()
    
    robo_category_list = ["移動作業型ロボット", "人間装着型ロボット", "搭乗型ロボット",
                         "コミュニケーション型ロボット", "汎用型ロボット", "産業用ロボット"]
    
    all_company_urls = {}
    
    def collect_urls_for_company(company):
        try:
            print(f"URL収集中: {company}")
            urls = set()
            for robo in robo_category_list:
                try:
                    keyword = robo + '　' + company
                    urls_1 = get_search_results(keyword)  # 統一関数を使用
                    urls.update(urls_1)
                except Exception as e:
                    print(f"  ✗ {company} - {robo}: {e}")
                    continue
            return company, list(urls)
        except Exception as e:
            print(f"✗ {company} URL収集エラー: {e}")
            return company, []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(collect_urls_for_company, company) for company in company_list]
        for future in tqdm(as_completed(futures), total=len(futures), desc="企業URL収集"):
            company, urls = future.result()
            all_company_urls[company] = urls
    
    # 統計処理
    total_urls = sum(len(urls) for urls in all_company_urls.values())
    stage1_time = time.time() - stage1_start
    
    print(f"✅ 段階1完了: {stage1_time:.1f}秒")
    print(f"合計: {total_urls} URLs")
    
    # URL DataFrame作成
    url_list_data = []
    for company, urls in all_company_urls.items():
        for url in urls:
            url_list_data.append({'企業名': company, 'URL': url})
    
    url_df = pd.DataFrame(url_list_data) if url_list_data else pd.DataFrame(columns=['企業名', 'URL'])
    
    stats = {
        'total_companies': len(company_list),
        'total_urls': total_urls,
        'processing_time': stage1_time,
        'urls_per_company': {company: len(urls) for company, urls in all_company_urls.items()}
    }
    
    return all_company_urls, url_df, stats

def phase2_get_texts(all_company_urls, max_workers=3):
    """段階2: 全URLのテキスト取得"""
    print("📄 段階2: 全URLのテキスト取得中...")
    stage2_start = time.time()
    
    total_urls = sum(len(urls) for urls in all_company_urls.values())
    if total_urls == 0:
        print("❌ URLが存在しません")
        return {}, {'total_urls': 0, 'total_texts': 0, 'processing_time': 0}
    
    all_company_texts = {}
    
    def get_texts_for_company(company_data):
        company, urls = company_data
        try:
            print(f"テキスト取得中: {company} ({len(urls)} URLs)")
            all_text = getAllTextFromUrls(urls)
            print(f"  ✓ {company}: {len(all_text)}/{len(urls)} 成功取得")
            return company, all_text
        except Exception as e:
            print(f"✗ {company} テキスト取得エラー: {e}")
            return company, {}
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(get_texts_for_company, (company, urls))
            for company, urls in all_company_urls.items()
            if len(urls) > 0
        ]
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="テキスト取得"):
            company, texts = future.result()
            all_company_texts[company] = texts
    
    # 統計処理
    total_texts = sum(len(texts) for texts in all_company_texts.values())
    stage2_time = time.time() - stage2_start
    
    print(f"✅ 段階2完了: {stage2_time:.1f}秒")
    print(f"合計: {total_texts} テキスト")
    
    stats = {
        'total_urls': total_urls,
        'total_texts': total_texts,
        'processing_time': stage2_time,
        'texts_per_company': {company: len(texts) for company, texts in all_company_texts.items()}
    }
    
    return all_company_texts, stats

def phase3_gpt_processing(all_company_texts, max_workers=3):
    """段階3: 全テキストのGPT処理"""
    print("🤖 段階3: 全テキストのGPT処理中...")
    stage3_start = time.time()
    
    total_texts = sum(len(texts) for texts in all_company_texts.values())
    if total_texts == 0:
        print("❌ テキストが存在しません")
        return [], {'total_texts': 0, 'successful_gpt': 0, 'processing_time': 0}
    
    def process_single_url_gpt(url_data):
        company, url, text = url_data
        try:
            if len(text) > 12000:
                text = text[:12000]
            
            # 統一関数を使用
            result, error = extract_arguments_gpt(text, company)
            
            return {
                'company': company,
                'url': url,
                'result': result,
                'error': error,
                'status': 'success' if result else 'no_result'
            }
        except Exception as e:
            print(f"GPT処理例外エラー: {url} - {e}")
            return {
                'company': company,
                'url': url,
                'result': None,
                'error': str(e),
                'status': 'failed'
            }
    
    # タスク作成
    all_gpt_tasks = []
    for company, texts in all_company_texts.items():
        for url, text in texts.items():
            if text:
                all_gpt_tasks.append((company, url, text))
    
    print(f"GPT処理タスク数: {len(all_gpt_tasks)}")
    
    # 並列GPT処理
    gpt_results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_single_url_gpt, task) for task in all_gpt_tasks]
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="GPT処理"):
            result = future.result()
            gpt_results.append(result)
    
    # 統計処理
    successful_gpt = [r for r in gpt_results if r['status'] == 'success' and r['result']]
    stage3_time = time.time() - stage3_start
    
    print(f"✅ 段階3完了: {stage3_time:.1f}秒")
    print(f"  成功: {len(successful_gpt)}")
    
    stats = {
        'total_texts': total_texts,
        'total_tasks': len(all_gpt_tasks),
        'successful_gpt': len(successful_gpt),
        'processing_time': stage3_time,
        'success_rate': len(successful_gpt) / len(all_gpt_tasks) * 100 if all_gpt_tasks else 0
    }
    
    return gpt_results, stats

def phase4_organize_data(gpt_results):
    """段階4: データ整理"""
    print("📋 段階4: データ整理中...")
    stage4_start = time.time()
    
    successful_gpt = [r for r in gpt_results if r['status'] == 'success' and r['result']]
    
    if not successful_gpt:
        print("❌ 整理するデータが存在しません")
        return pd.DataFrame(), {'total_records': 0, 'processing_time': 0}
    
    # データ平坦化
    flattened_data = []
    for gpt_result in successful_gpt:
        url = gpt_result['url']
        company = gpt_result['company']
        result = gpt_result['result']
        
        try:
            if isinstance(result, list):
                for entry in result:
                    if isinstance(entry, dict):
                        entry['URL'] = url
                        entry['企業名'] = company
                        flattened_data.append(entry)
            elif isinstance(result, dict):
                result['URL'] = url
                result['企業名'] = company
                flattened_data.append(result)
        except Exception as e:
            print(f"データ平坦化エラー: {url} - {e}")
    
    df_result = pd.DataFrame(flattened_data) if flattened_data else pd.DataFrame()
    
    stage4_time = time.time() - stage4_start
    
    print(f"✅ 段階4完了: {stage4_time:.1f}秒")
    print(f"📊 最終レコード数: {len(df_result)}")
    
    stats = {
        'total_records': len(df_result),
        'companies_with_data': df_result['企業名'].nunique() if not df_result.empty else 0,
        'processing_time': stage4_time,
        'records_per_company': df_result['企業名'].value_counts().to_dict() if not df_result.empty else {}
    }
    
    return df_result, stats

# =============================================================================
# Phase 3 特別処理関数
# =============================================================================

def process_single_row_final_df3(row_data):
    """単一行データの処理関数"""
    index = row_data['index']
    product = row_data['製品情報']
    company = row_data['企業名']
    
    try:
        search_query = f"{product} {company}"
        urls = get_search_results(search_query, pages=1)  # 統一関数を使用
        
        if len(urls) == 0:
            return {
                'index': index,
                'url': "",
                'text': "",
                'status': 'no_urls',
                'error': "No URLs found"
            }
        
        all_text = getAllTextFromUrls(urls)
        url = urls[0]
        
        if url not in all_text or not all_text[url]:
            return {
                'index': index,
                'url': url,
                'text': "",
                'status': 'no_text',
                'error': "No text content found"
            }
        
        text_content = all_text[url]
        
        # GPT処理
        max_attempts = 2
        for attempt in range(max_attempts):
            try:
                df_text = extract_arguments_gpt(text_content, company, product, return_raw=True)  # 統一関数を使用
                df_text_1 = json.loads(df_text.replace("```json", "").replace("```", "").strip())
                
                return {
                    'index': index,
                    'url': url,
                    'text': df_text_1,
                    'status': 'success',
                    'attempt': attempt + 1
                }
            except Exception as gpt_error:
                if attempt < max_attempts - 1:
                    print(f"  Row {index}: {attempt+1}回目の試行に失敗。再試行します...")
                    continue
                else:
                    return {
                        'index': index,
                        'url': url,
                        'text': "",
                        'status': 'gpt_failed',
                        'error': str(gpt_error),
                        'attempt': attempt + 1
                    }
    except Exception as e:
        return {
            'index': index,
            'url': "",
            'text': "",
            'status': 'failed',
            'error': str(e)
        }

def process_final_df3_parallel(final_df3, max_workers=3):
    """final_df3の並列処理"""
    print(f"🚀 final_df3 並列処理開始 (workers: {max_workers})")
    start_time = time.time()
    
    final_df3_copy = final_df3.copy()
    final_df3_copy["url"] = ""
    final_df3_copy["text"] = ""
    
    # 有効行データの収集
    valid_rows = []
    for n in range(len(final_df3_copy)):
        product = final_df3_copy["製品情報"].iloc[n]
        company = final_df3_copy["企業名"].iloc[n]
        
        if not (pd.isna(product) or pd.isna(company)):
            valid_rows.append({
                'index': n,
                '製品情報': product,
                '企業名': company
            })
    
    print(f"📊 処理対象: {len(valid_rows)}/{len(final_df3_copy)} 行")
    
    if len(valid_rows) == 0:
        print("❌ 処理可能なデータがありません")
        return final_df3_copy, [], {'total_rows': len(final_df3_copy), 'processed': 0}
    
    # 並列処理
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_single_row_final_df3, row_data) for row_data in valid_rows]
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing rows"):
            result = future.result()
            results.append(result)
    
    # 結果の統計
    successful_results = [r for r in results if r['status'] == 'success']
    failed_results = [r for r in results if r['status'] != 'success']
    
    # DataFrameの更新
    for result in successful_results:
        index = result['index']
        final_df3_copy.at[index, "url"] = result['url']
        final_df3_copy.at[index, "text"] = result['text']
    
    # 失敗ケースの収集
    failed_cases = []
    for result in failed_results:
        failed_cases.append((result['index'], result.get('error', 'Unknown error')))
    
    processing_time = time.time() - start_time
    
    # 統計情報
    statistics = {
        'total_rows': len(final_df3_copy),
        'valid_rows': len(valid_rows),
        'successful': len(successful_results),
        'failed': len(failed_results),
        'success_rate': len(successful_results) / len(valid_rows) * 100 if valid_rows else 0,
        'processing_time': processing_time,
        'rows_per_second': len(valid_rows) / processing_time if processing_time > 0 else 0
    }
    
    print(f"✅ 処理完了: {processing_time:.1f}秒")
    print(f"📊 処理統計: 成功: {statistics['successful']}, 失敗: {statistics['failed']}")
    
    return final_df3_copy, failed_cases, statistics

# =============================================================================
# JSON展開・データ処理関数
# =============================================================================

def expand_json_simple(df, text_column='text', prefix='json_'):
    """シンプルなJSON展開"""
    column_list = ['企業名', '製品名', '分類', '使用用途', '技術領域',
                   '協業実績', '製品情報', '実証実験', '製品の説明']
    
    df_result = df.copy()
    
    # 新しい列を初期化
    for col in column_list:
        col_name = f"{prefix}{col}" if prefix else col
        df_result[col_name] = ''
    
    # 各行を処理
    for idx, row in tqdm(df_result.iterrows(), total=len(df_result), desc="JSON展開中"):
        data = row[text_column]
        
        if pd.isna(data) or data == '':
            continue
            
        # 辞書または文字列を処理
        if isinstance(data, dict):
            parsed_data = data
        elif isinstance(data, str):
            try:
                parsed_data = json.loads(data.replace("'", '"').strip())
            except:
                continue
        else:
            continue
        
        # 各列の値を設定
        for col in column_list:
            col_name = f"{prefix}{col}" if prefix else col
            if col in parsed_data:
                df_result.at[idx, col_name] = parsed_data[col]
    
    return df_result

def clean_product_batch_simple(product_list):
    """シンプルな製品名バッチクリーニング"""
    if not product_list:
        return []
    
    indexed_items = {str(i): item for i, item in enumerate(product_list)}
    
    prompt = f"""以下の製品名の表記揺れを統一してください。
入力: {indexed_items}
出力: JSON形式で返してください。
例: {{"0": "統一された製品名1", "1": "統一された製品名2"}}
注意: 必ず同じインデックス番号で返してください。"""
    
    try:
        result_llm = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0
        )
        
        response = result_llm.choices[0].message.content.strip()
        if response.startswith('```json'):
            response = response.replace('```json', '').replace('```', '').strip()
        
        result_dict = json.loads(response)
        
        cleaned_list = []
        for i in range(len(product_list)):
            cleaned_list.append(result_dict.get(str(i), product_list[i]))
        
        return cleaned_list
        
    except:
        return product_list

def clean_dataframe_products(df, column_name='json_製品名', batch_size=10):
    """DataFrameの製品名列をクリーニング"""
    print(f"🧹 {column_name} をクリーニング中...")
    
    non_empty_mask = df[column_name].notna() & (df[column_name] != '') & (df[column_name] != '{}')
    non_empty_products = df.loc[non_empty_mask, column_name].tolist()
    
    if len(non_empty_products) == 0:
        print("❌ クリーニング対象なし")
        df[f'{column_name}_cleaned'] = df[column_name]
        return df
    
    unique_products = list(set(non_empty_products))
    print(f"📊 {len(unique_products)} ユニーク製品名を処理")
    
    cleaned_mapping = {}
    for i in tqdm(range(0, len(unique_products), batch_size), desc="バッチ処理"):
        batch = unique_products[i:i + batch_size]
        cleaned_batch = clean_product_batch_simple(batch)
        
        for original, cleaned in zip(batch, cleaned_batch):
            cleaned_mapping[original] = cleaned
    
    df[f'{column_name}_cleaned'] = df[column_name].map(cleaned_mapping).fillna(df[column_name])
    
    changed_count = sum(1 for orig, clean in cleaned_mapping.items() if orig != clean)
    print(f"✅ 完了: {changed_count}/{len(unique_products)} 件変更")
    
    return df

def extract_datazora_categories(input_list):
    """技術領域カテゴリー分類"""
    if not input_list:
        return []
        
    valid_categories = {
        "医療", "インフラ", "航空・宇宙", "介護・福祉", 
        "物流・搬送", "農林水産業", "商業施設・宿泊施設"
    }
    
    categories = [
        "医療", "インフラ", "航空・宇宙", "介護・福祉",
        "物流・搬送", "農林水産業", "商業施設・宿泊施設", "その他"
    ]
    
    prompt = f"""以下のリストの各項目を、指定されたカテゴリーに分類してください。

入力: {input_list}

カテゴリー:
{chr(10).join(f'{i+1}. {cat}' for i, cat in enumerate(categories))}

分類ルール:
1. 各項目は複数のカテゴリーに分類可能です
2. 複数のカテゴリーに当てはまる場合は、"/"で区切って列挙してください
3. どのカテゴリーにも当てはまらない場合は「その他」に分類してください

必ず入力と同じ長さのPythonリスト形式で返してください。
例: ['医療/介護・福祉', 'インフラ', 'その他']"""
    
    try:
        result_llm = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system", 
                    "content": "Pythonのリスト形式でのみ返答してください。説明文は不要です。"
                },
                {
                    "role": "user", 
                    "content": prompt
                }
            ]
        )
        
        response = result_llm.choices[0].message.content.strip()
        categorized_list = eval(response)
        
        cleaned_list = []
        for item in categorized_list:
            if pd.isna(item) or not item:
                cleaned_list.append('その他')
                continue
                
            categories = item.split('/')
            valid_cats = [cat for cat in categories if cat in valid_categories]
            
            if valid_cats:
                cleaned_list.append('/'.join(valid_cats))
            else:
                cleaned_list.append('その他')
        
        return cleaned_list
        
    except Exception as e:
        print(f"Error parsing response: {e}")
        return ['その他'] * len(input_list)

def add_skill_categories_to_dataframe(df, target_column='技術領域', batch_size=10):
    """DataFrameに技術領域カテゴリーを追加"""
    print(f"🔖 技術領域カテゴリー分類開始...")
    
    df_result = df.copy()
    
    if target_column not in df_result.columns:
        print(f"❌ 列 '{target_column}' が存在しません")
        return df_result
    
    valid_data = df_result[target_column].dropna().tolist()
    unique_data = list(set(valid_data))
    
    print(f"📊 処理対象: {len(unique_data)} ユニーク技術領域")
    
    if len(unique_data) == 0:
        df_result[f'{target_column}_category'] = 'その他'
        return df_result
    
    category_mapping = {}
    
    for i in tqdm(range(0, len(unique_data), batch_size), desc="カテゴリー分類"):
        batch = unique_data[i:i + batch_size]
        categorized_batch = extract_datazora_categories(batch)
        
        for original, category in zip(batch, categorized_batch):
            category_mapping[original] = category
    
    df_result[f'{target_column}_category'] = df_result[target_column].map(category_mapping).fillna('その他')
    
    category_counts = df_result[f'{target_column}_category'].value_counts()
    print(f"✅ カテゴリー分類完了:")
    for category, count in category_counts.items():
        print(f"  {category}: {count} 件")
    
    return df_result

def combine_rows(x):
    """行の結合"""
    unique_values = set(str(val) for val in x.dropna() if str(val).lower() != 'nan')
    return '@'.join(unique_values) if unique_values else None

def create_comprehensive_summaries(input_texts):
    """複数のテキストを一括で要約する関数"""
    if not input_texts:
        return []

    summaries = [None] * len(input_texts)
    messages = []
    valid_indices = []
    
    for i, item in enumerate(input_texts):
        if item['text'] and not pd.isna(item['text']):
            text_part = f"テキスト{len(valid_indices)+1}: {item['text']}\n種類: {item['content_type']}\n---\n"
            messages.append(text_part)
            valid_indices.append(i)
    
    if not messages:
        return summaries

    prompt = f"""以下の複数のテキストを要約してください。

{''.join(messages)}

要約ルール:
1. '@'で区切られた各項目から事実情報のみを抽出
2. 記載されていない情報の推測は避ける
3. 説明的な装飾語は省く

返答形式：
テキスト1の要約: [要約内容]
テキスト2の要約: [要約内容]
...
（必ず入力テキストと同じ数の要約を返してください）"""

    try:
        result = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "複数のテキストから事実情報のみを抽出し、簡潔な要約を作成してください。"
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ]
        )
        
        response_text = result.choices[0].message.content.strip()
        summary_parts = response_text.split('\n')
        
        summary_count = 0
        for part in summary_parts:
            if part.startswith('テキスト') and ': ' in part:
                if summary_count < len(valid_indices):
                    original_index = valid_indices[summary_count]
                    summary = part.split(': ', 1)[1].strip()
                    summaries[original_index] = summary
                    summary_count += 1
    
    except Exception as e:
        print(f"要約処理エラー: {e}")
    
    return summaries

def format_to_dict_list(input_list, content_type):
    """リストを辞書形式に変換"""
    return [
        {
            'text': item,
            'content_type': content_type
        }
        for item in input_list
    ]

def summarize_dataframe_column(df, column_name, content_type, batch_size=5):
    """DataFrameの特定列を要約処理"""
    print(f"📝 {column_name} の要約処理開始...")
    
    df_result = df.copy()
    
    if column_name not in df_result.columns:
        print(f"❌ 列 '{column_name}' が存在しません")
        return df_result
    
    data_list = df_result[column_name].tolist()
    dict_list = format_to_dict_list(data_list, content_type)
    
    total_batches = (len(dict_list) + batch_size - 1) // batch_size
    
    all_summaries = []
    
    for i in tqdm(range(0, len(dict_list), batch_size), 
                  total=total_batches, 
                  desc=f"{column_name}要約処理"):
        batch = dict_list[i:i + batch_size]
        batch_summaries = create_comprehensive_summaries(batch)
        all_summaries.extend(batch_summaries)
    
    df_result[f'{column_name}_summary'] = all_summaries
    
    non_null_summaries = sum(1 for s in all_summaries if s is not None)
    print(f"✅ 要約完了: {non_null_summaries}/{len(all_summaries)} 件")
    
    return df_result

# =============================================================================
# メイン実行部分
# =============================================================================

def run_full_pipeline(company_list, max_workers=3):
    """完全なパイプラインの実行"""
    total_start_time = time.time()
    
    # 段階1: URL収集
    all_company_urls, url_df, stats1 = phase1_collect_urls(company_list, max_workers)
    
    # 段階2: テキスト取得
    all_company_texts, stats2 = phase2_get_texts(all_company_urls, max_workers)
    
    # 段階3: GPT処理
    gpt_results, stats3 = phase3_gpt_processing(all_company_texts, max_workers)
    
    # 段階4: データ整理
    df_result, stats4 = phase4_organize_data(gpt_results)
    
    total_time = time.time() - total_start_time
    
    all_stats = {
        'phase1': stats1,
        'phase2': stats2,
        'phase3': stats3,
        'phase4': stats4,
        'total_time': total_time
    }
    
    print("🎉 全段階処理完了!")
    print(f"⏱️  総時間: {total_time:.1f} 秒")
    print(f"📊 最終レコード数: {len(df_result)}")
    
    return df_result, url_df, all_stats

if __name__ == "__main__":
    company_list = ["マクニカ", "三菱電機", "エルエーピー", "ケアボット", "NTTドコモ"]

    print("=== 段階別実行 ===")

    # 段階1: URL収集
    all_company_urls, url_df, stats1 = phase1_collect_urls(company_list, max_workers=2)
    print(f"段階1完了: {stats1['total_urls']} URLs収集")

    # 段階2: テキスト取得
    all_company_texts, stats2 = phase2_get_texts(all_company_urls, max_workers=2)
    print(f"段階2完了: {stats2['total_texts']} テキスト取得")

    # 段階3: GPT処理
    gpt_results, stats3 = phase3_gpt_processing(all_company_texts, max_workers=2)
    print(f"段階3完了: {stats3['successful_gpt']} 成功処理")

    # 段階4: データ整理
    final_df, stats4 = phase4_organize_data(gpt_results)
    print(f"段階4完了: {stats4['total_records']} レコード")

    print("=== 並列処理版 ===")

    updated_df_parallel, failed_cases_parallel, stats = process_final_df3_parallel(
        final_df, 
        max_workers=3  
    )

    print(f"\n📋 並列処理結果:")
    print(f"  成功更新: {stats['successful']} 行")
    print(f"  失敗: {len(failed_cases_parallel)} 行")

    final_df_3 = updated_df_parallel

    print("=== JSON展開処理 ===")
    expanded_df = expand_json_simple(
        df=final_df_3,
        text_column='text'
    )

    print("=== 製品名クリーニング ===")
    expanded_df_cleaned = clean_dataframe_products(expanded_df, 'json_製品名', batch_size=10)
    
    # カラム選択と名前変更
    expanded_df_cleaned = expanded_df_cleaned[['企業名','json_製品名_cleaned','分類', 'URL',  'url',   
                                             '使用用途', '技術領域', '協業実績', '製品情報',  
                                             'json_使用用途', 'json_技術領域','json_協業実績', 
                                             'json_製品情報', 'json_実証実験', 'json_製品の説明']]
    
    expanded_df_cleaned.columns = ['企業名', '製品名', '分類', 'URL', 'url', '使用用途', '技術領域', '協業実績',
                                  '製品情報',  'json_使用用途', 'json_技術領域', 'json_協業実績', 'json_製品情報',
                                  '実証実験', '製品の説明']

    # カラム結合
    expanded_df_colconcat = expanded_df_cleaned.copy()

    expanded_df_colconcat['使用用途'] = expanded_df_colconcat['使用用途'].astype(str) + "、" + expanded_df_colconcat['json_使用用途'].astype(str)
    expanded_df_colconcat['技術領域'] = expanded_df_colconcat['技術領域'].astype(str) + "、" + expanded_df_colconcat['json_技術領域'].astype(str)
    expanded_df_colconcat['協業実績'] = expanded_df_colconcat['協業実績'].astype(str) + "、" + expanded_df_colconcat['json_協業実績'].astype(str)
    expanded_df_colconcat['製品情報'] = expanded_df_colconcat['製品情報'].astype(str) + "、" + expanded_df_colconcat['json_製品情報'].astype(str)

    expanded_df_colconcat = expanded_df_colconcat.drop(["json_使用用途","json_技術領域","json_協業実績","json_製品情報"], axis=1)

    # グループ化
    expanded_df_groupby = expanded_df_colconcat.groupby(['企業名', '製品名'], as_index=False).agg({
        col: lambda x: combine_rows(x) for col in ['分類', '使用用途', 'URL', 'url', '技術領域', '協業実績','製品情報', '実証実験', '製品の説明']
    })

    # 空の製品名を除外
    expanded_df_groupby = expanded_df_groupby[expanded_df_groupby["製品名"] != ""]

    print("=== 技術領域カテゴリー分類 ===")
    df_with_categories = add_skill_categories_to_dataframe(
        df=expanded_df_groupby,
        target_column='技術領域',
        batch_size=10
    )
    
    df_with_categories = df_with_categories.drop(['技術領域'], axis=1)
    df_with_categories.columns = ['企業名', '製品名', '分類', '使用用途', 'URL', 'url', '協業実績', '製品情報', '実証実験',
                                 '製品の説明', '技術領域']

    print("=== 要約処理 ===")
    target_columns = {
        '使用用途': '使用用途',
        '協業実績': '協業実績',
        '製品情報': '製品情報',
        '実証実験': '実証実験',
        '製品の説明': '製品の説明'
    }

    df_final = df_with_categories.copy()
    for column, content_type in target_columns.items():
        if column in df_final.columns:
            df_final = summarize_dataframe_column(df_final, column, content_type)

    print("✅ 全処理完了!")
    print(f"最終データフレーム形状: {df_final.shape}")
    print("\n主要カラム:")
    for col in df_final.columns:
        print(f"  - {col}")
    df_final = df_final.drop(['使用用途','協業実績', '製品情報', '実証実験','製品の説明'], axis=1)
    
    # _summaryを削除して列名をクリーン化
    rename_dict = {}
    for col in df_final.columns:
        if col.endswith('_summary'):
            new_name = col.replace('_summary', '')
            rename_dict[col] = new_name

    df_final = df_final.rename(columns=rename_dict)

    # 結果の保存例
    df_final.to_csv('./robot_analysis_result.csv', index=False, encoding='utf-8-sig')