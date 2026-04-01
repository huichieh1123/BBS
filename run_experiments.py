import csv
import os
import subprocess
import time

def run_batch_experiments():
    master_file = 'DB/cur_cmd_master.csv'
    main_script = 'main.py'
    
    # 1. 檢查必要檔案是否存在
    if not os.path.exists(master_file):
        print(f"錯誤：找不到資料庫主檔 {master_file}")
        return
    if not os.path.exists(main_script):
        print(f"錯誤：找不到主程式 {main_script}")
        return

    # 定義實驗過濾條件
    TARGET_ORDER_SCENARIOS = ['small_uni', 'small_peak', 'large_uni', 'large_peak']
    TARGET_INV_SCENARIOS = [
        "2x2_slot_random_place_random",
        "2x2_slot_inorder_place_inorder",
        "2x2_slot_inorder_place_cluster",
        "2x2_slot_inorder_place_random"
    ]
    TARGET_BATCH_WINDOWS = ["1", "5", "10", "15"]
    TARGET_BATCH_ALGOS = ["grasp_ver3", "greedy_ver3"]
    TARGET_SELECT_ALGOS = ["grasp_ver3", "greedy_ver3"]

    # 2. 掃描 Master 檔，收集所有唯一的 selection_run_id
    print(f"正在掃描 {master_file} 以收集實驗 ID...")
    run_ids = set()
    try:
        cnt = 0
        with open(master_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # 去除欄位名稱可能的空白
                clean_row = {str(k).strip(): str(v).strip() for k, v in row.items() if k}
                cnt+=1 
                rid = clean_row.get('selection_run_id')
                o_scenario = clean_row.get('order_scenario')
                i_scenario = clean_row.get('inv_scenario')
                b_window = clean_row.get('batch_time_window')
                b_algo_ver = clean_row.get('batch_algo_ver')
                s_algo_ver = clean_row.get('selection_algo_ver')
                
                # 嚴格過濾條件：必須完全符合清單內的設定
                if (rid and rid != 'N/A' and 
                    o_scenario in TARGET_ORDER_SCENARIOS and
                    i_scenario in TARGET_INV_SCENARIOS and
                    b_window in TARGET_BATCH_WINDOWS and
                    b_algo_ver in TARGET_BATCH_ALGOS and
                    s_algo_ver in TARGET_SELECT_ALGOS):
                    
                    run_ids.add(rid)
            print(f'count rows:{cnt}')
                    
    except Exception as e:
        print(f"讀取 CSV 失敗: {e}")
        return
    
    # 將 set 轉為 list 並排序，確保實驗順序一致
    sorted_run_ids = sorted(list(run_ids))
    total_count = len(sorted_run_ids)
    print(f"成功收集 {total_count} 個符合條件的唯一 selection_run_id。")
    print("--------------------------------------------------")

    # 如果沒有找到符合條件的 ID，提早結束
    if total_count == 0:
        print("未找到符合條件的實驗，程式結束。")
        return

    # 3. 循環執行實驗
    success_count = 0
    fail_count = 0
    fail_ids = []
    start_time = time.time()

    for idx, rid in enumerate(sorted_run_ids, 1):
        print(f"[{idx}/{total_count}] 正在執行實驗 ID: {rid}")
        
        # 建立執行指令：帶入 run_id
        command = [
            "python", main_script,
            "--run_id", rid
        ]
        
        try:
            # 使用 subprocess.run 執行，會等待該任務結束才繼續下一個
            result = subprocess.run(command, check=True)
            
            if result.returncode == 0:
                success_count += 1
            else:
                fail_count += 1
                fail_ids.append(rid)
                
        except subprocess.CalledProcessError:
            print(f"實驗 ID {rid} 執行失敗 (Process Error)。")
            fail_count += 1
            fail_ids.append(rid)
        except Exception as e:
            print(f"發生未知錯誤: {e}")
            fail_count += 1
            fail_ids.append(rid)

        print(f"--- 完成 ID: {rid} ---\n")

    # 4. 最終總結
    total_duration = time.time() - start_time
    print("==================================================")
    print(" 批量實驗執行完畢！")
    print(f" 總耗時: {total_duration:.2f} 秒")
    print(f" 成功: {success_count}")
    print(f" 失敗: {fail_count}")
    if fail_ids:
        print(f" 失敗的 ID 清單: {fail_ids}")
    print("==================================================")

if __name__ == "__main__":
    run_batch_experiments()