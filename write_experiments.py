import glob
import pandas as pd

def aggregate_logs_to_csv():
    # 1. 搜尋所有 logs 目錄下的 mission_summary.csv
    log_files = glob.glob('logs/logs/**/mission_summary.csv', recursive=True)
    records = []

    # 2. 讀取並解析每個檔案
    for filepath in log_files:
        data = {}
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split(',')
                if len(parts) >= 2:
                    k, v = parts[0].strip(), parts[1].strip()
                    # 排除掉標籤如 [Scenario Info], [Results] 等
                    if k and not k.startswith('['):
                        data[k] = v

        if not data:
            continue

        # 提取情境與數值
        try:
            records.append({
                'Order Scenario': data.get('order_scenario', ''),
                'Inv Scenario': data.get('inv_scenario', ''),
                'Batch Time Window': float(data.get('batch_time_window', 0)),
                'Batch Algo. Ver.': data.get('batch_algo_ver', ''),
                'Selection Algo. Ver.': data.get('selection_algo_ver', ''),
                # 擷取需要加總的數值內容
                'Number of Tasks': float(data.get('Number of Tasks', 0)),
                'Makespan': float(data.get('Makespan (s)', 0)),
                'CPU time': float(data.get('CPU Time (s)', 0))
            })
        except Exception as e:
            print(f"解析檔案 {filepath} 時發生錯誤: {e}")

    if not records:
        print("沒有找到任何有效的 log 資料。")
        return

    # 將資料轉換為 DataFrame
    df_logs = pd.DataFrame(records)

    # 3. 依照指定的情境條件進行群組，並使用 .sum() 將數值加總
    group_cols = [
        'Order Scenario', 
        'Inv Scenario', 
        'Batch Time Window', 
        'Batch Algo. Ver.', 
        'Selection Algo. Ver.'
    ]
    
    # 將相同情境的數值相加
    agg_df = df_logs.groupby(group_cols)[['Number of Tasks', 'Makespan', 'CPU time']].sum().reset_index()

    # 4. 輸出為 CSV 檔案
    output_csv_path = 'aggregated_sum_results.csv'
    
    print(f"開始產生 CSV 檔案: {output_csv_path} ...")
    
    # 使用 utf-8-sig 編碼，確保用 Windows Excel 打開 CSV 時不會有亂碼問題
    agg_df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')
    
    print(f"所有資料已成功加總並匯出至: {output_csv_path}")

if __name__ == "__main__":
    aggregate_logs_to_csv()