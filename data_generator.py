import argparse
import csv
import random
import time
import os

# ==========================================
# 1. 隨機生成模式 (Random Generator Mode)
# ==========================================
def generate_random(max_row, max_bay, max_level, total_boxes, mission_count, workstation_count):
    capacity = max_row * max_bay * max_level
    if total_boxes > capacity:
        raise ValueError(f"Error: Total boxes exceeds yard capacity!")
    if mission_count > total_boxes:
        raise ValueError(f"Error: Mission count cannot be larger than total boxes!")

    heights = [0] * (max_row * max_bay)
    all_boxes = []

    for i in range(1, total_boxes + 1):
        placed = False
        attempts = 0
        while not placed:
            if attempts < 1000:
                r, b = random.randint(0, max_row - 1), random.randint(0, max_bay - 1)
                idx = r * max_bay + b
                attempts += 1
            else:
                found_slot = False
                for tr in range(max_row):
                    for tb in range(max_bay):
                        tidx = tr * max_bay + tb
                        if heights[tidx] < max_level:
                            r, b, idx = tr, tb, tidx
                            found_slot = True
                            break
                    if found_slot: break

            if heights[idx] < max_level:
                all_boxes.append({'id': i, 'row': r, 'bay': b, 'level': heights[idx]})
                heights[idx] += 1
                placed = True

    candidates = all_boxes.copy()
    random.shuffle(candidates)
    
    max_possible_stages = min(3, workstation_count)
    base_time = 1705363200

    parent_quantity_map = {}
    commands = [] # [NEW] 建立記憶體內的 commands 陣列

    # 寫入 CSV 的同時，也建立記憶體資料
    with open("mock_commands.csv", "w", newline="") as f:
        f.write("cmd_no,batch_id,cmd_type,cmd_priority,parent_carrier_id,"
                "src_row,src_bay,src_level,dest_row,dest_bay,dest_level,create_time\n")
        
        for i in range(mission_count):
            box = candidates[i]
            num_stages = random.randint(1, max_possible_stages)
            ws_pool = list(range(0, workstation_count))
            random.shuffle(ws_pool)
            ws_sequence = "|".join(str(ws) for ws in ws_pool[:num_stages])
            create_time = base_time + (i + 1) * 60

            f.write(f"{i+1},20260117,target,{i+1},{box['id']},"
                    f"{box['row']},{box['bay']},{box['level']},"
                    f"-1,{ws_sequence},1,{create_time}\n")
            
            # [NEW] 將資料存入記憶體陣列，格式對齊 Cython 需要的格式
            commands.append({
                'id': box['id'],
                'type': 'target',
                'dest': {'row': -1, 'bay': ws_sequence, 'level': 1}
            })
            
            parent_quantity_map[box['id']] = random.randint(10, 50)

    with open("mock_yard.csv", "w", newline="") as f:
        f.write("container_id,row,bay,level\n")
        for box in all_boxes:
            f.write(f"{box['id']},{box['row']},{box['bay']},{box['level']}\n")

    # [NEW] 建立記憶體內的 config 字典
    config_dict = {
        'max_row': max_row, 'max_bay': max_bay, 'max_level': max_level, 'total_boxes': total_boxes,
        't_travel': 5.0, 't_handle': 30.0, 't_port_handle': 15.0, 't_unit_process': 1.0,
        'workstation_count': workstation_count
    }

    with open("yard_config.csv", "w", newline="") as f:
        f.write("max_row,max_bay,max_level,total_boxes,time_travel_unit,time_handle,time_port_handle,time_unit_process,workstation_count\n")
        f.write(f"{max_row},{max_bay},{max_level},{total_boxes},5.0,30.0,15.0,1.0,{workstation_count}\n")

    print(f"Success! Generated random files and loaded into RAM.")

    selection_run_id_info = {
        "Random": {"order_scenario": "random", "selection_algo_ver": "N/A", "batch_algo_ver": "N/A"}
    }
    
    # [MODIFIED] 一次把所有必備資料從 RAM 傳出去！
    return config_dict, all_boxes, commands, parent_quantity_map, selection_run_id_info


# ==========================================
# 2. 資料庫匯入模式 (DB Import Mode)
# ==========================================
def parse_location_id(loc_id):
    if not loc_id or len(loc_id) < 10: return -1, -1, -1
    return int(loc_id[0:5]), int(loc_id[5:8]), int(loc_id[8:10])

def parse_carrier_id(car_id):
    if not car_id: return 0
    clean_id = ''.join(filter(str.isdigit, car_id))
    return int(clean_id) + 1 if clean_id else 0

def load_simulation_data(run_id, target_run_id, base_config):
    print(f"\n[X-Ray] 開始為 ID '{run_id}' 尋找資料...")
    inv_scenario = ""
    job_sequence = []
    target_dest_map = {}
    target_cmd_ids = set() 

    selection_run_id_info = {}
    try:
        with open('DB/cur_cmd_master.csv', 'r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                s_id = str(row.get('selection_run_id', '')).strip()
                if s_id and s_id not in selection_run_id_info and target_run_id == s_id:
                    # 使用 or 'N/A' 確保如果欄位是空白，會直接顯示 N/A
                    selection_run_id_info[s_id] = {
                        'order_scenario': str(row.get('order_scenario', '')).strip() or 'N/A',
                        'inv_scenario': str(row.get('inv_scenario', '')).strip() or 'N/A',
                        'selection_algo_ver': str(row.get('selection_algo_ver', '')).strip() or 'N/A',
                        'batch_algo_ver': str(row.get('batch_algo_ver', '')).strip() or 'N/A',
                        'batch_time_window': str(row.get('batch_time_window', '')).strip() or 'N/A'
                    }
        print(f"[X-Ray] Step 0: 成功備份原始 Metadata")
    except Exception as e:
        print(f"[X-Ray] Step 0 Metadata 讀取失敗: {e}")


    # --- 來源切換偵測 ---
    csv_source = 'DB/cur_cmd_master.csv'
    if os.path.exists('resequence.csv'):
        with open('resequence.csv', 'r', encoding='utf-8-sig') as f:
            if run_id in f.read():
                csv_source = 'resequence.csv'
                print(f"[X-Ray] 成功切換資料來源至: {csv_source}")

    # --- Step 1: 讀取任務主檔 (並收集 cmd_id) ---
    print(f"[X-Ray] 開始讀取: {csv_source}")
    orig_run_id = ''
    try:
        with open(csv_source, 'r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                csv_run_id = str(row.get('selection_run_id', '')).strip()
                orig_run_id = str(row.get('original_run_id', '')).strip()
                reseq_run_id = str(row.get('reseq_id', '')).strip()
                
                if run_id.strip() in (csv_run_id, orig_run_id, reseq_run_id):
                    inv_scenario = str(row.get('inv_scenario', '')).strip()
                    p_id = parse_carrier_id(row.get('parent_carrier_id', ''))
                    cmd_id = str(row.get('cmd_id', '')).strip()
                    
                    if p_id == 0: continue
                    
                    dest_str = str(row.get('dest_position', '0')).strip()
                    ws_id = int(dest_str) if dest_str else 0
                    
                    job_sequence.append(p_id)
                    if p_id not in target_dest_map: target_dest_map[p_id] = []
                    target_dest_map[p_id].append(ws_id)
                    if cmd_id: target_cmd_ids.add(cmd_id)
                    
        print(f"[X-Ray] Step 1 任務解析: 找到 {len(job_sequence)} 筆任務, 鎖定 {len(target_cmd_ids)} 個 cmd_id")
    except Exception as e:
        print(f"[X-Ray] Step 1 失敗: {e}")

    # --- Step 2: 讀取 Carrier 映射 ---
    carrier_to_parent = {}
    max_id = 0
    try:
        with open('DB/cur_carrier.csv', 'r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                c_id = row.get('carrier_id', '').strip()
                p_id = parse_carrier_id(row.get('parent_carrier_id', ''))
                carrier_to_parent[c_id] = p_id
                if p_id > max_id: max_id = p_id
    except Exception as e:
        print(f"[X-Ray] Step 2 失敗: {e}")

    # --- Step 3: 讀取物理庫存 ---
    boxes, seen_parents, used_locations = [], set(), set()
    try:
        with open('DB/cur_inventory.csv', 'r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                if str(row.get('scenario', '')).strip() == inv_scenario:
                    c_id = row.get('carrier_id', '').strip()
                    p_id = carrier_to_parent.get(c_id)
                    r, b, l = parse_location_id(row.get('location_id', ''))
                    if p_id and p_id not in seen_parents and (r,b,l) not in used_locations:
                        boxes.append({'id': p_id, 'row': r, 'bay': b, 'level': l})
                        seen_parents.add(p_id)
                        used_locations.add((r,b,l))
                        if p_id > max_id: max_id = p_id
    except Exception as e:
        print(f"[X-Ray] Step 3 失敗: {e}")

    valid_job_sequence = [t for t in job_sequence if t in seen_parents]
    
    # --- Step 4: 讀取加工數量 (使用 cmd_id 嚴格把關) ---
    parent_quantity_map = {}
    try:
        with open('DB/cur_cmd_detail.csv', 'r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                current_cmd_id = str(row.get('cmd_id', '')).strip()
                if current_cmd_id in target_cmd_ids:
                    c_id = row.get('carrier_id', '').strip()
                    qty_str = row.get('quantity', '0').strip()
                    qty = int(float(qty_str)) if qty_str else 0
                    
                    p_id = carrier_to_parent.get(c_id)
                    if p_id and p_id in seen_parents:
                        parent_quantity_map[p_id] = parent_quantity_map.get(p_id, 0) + qty
    except Exception as e:
        print(f"[X-Ray] Step 4 失敗: {e}")
        
    for p_id in valid_job_sequence:
        if p_id not in parent_quantity_map: parent_quantity_map[p_id] = 10 
            
    config = {
        'max_row': base_config['yard']['max_row'],
        'max_bay': base_config['yard']['max_bay'],
        'max_level': base_config['yard']['max_level'],
        'total_boxes': max_id,
        'agv_count': base_config['solver']['agv_count'],
        'port_count': base_config['yard'].get('port_count', 3),
        'workstation_count': base_config['yard']['workstation_count'],
        't_travel': base_config['time']['t_travel'],
        't_handle': base_config['time']['t_handle'],
        't_port_handle': base_config['time']['t_port_handle'],
        't_unit_process': base_config['time']['t_unit_process']
    }
    return config, boxes, valid_job_sequence, target_dest_map, parent_quantity_map, selection_run_id_info

def generate_db(base_config, run_id, target_run_id):
    res = load_simulation_data(run_id, target_run_id, base_config)
    if not res: return None, None, None, None
    config, boxes, valid_job_sequence, target_dest_map, parent_quantity_map, selection_run_id_info = res
    
    if not boxes or not valid_job_sequence:
        print("Warning: DB returned empty inventory or job sequence.")
        return None, None, None, None

    with open("mock_yard.csv", "w", newline="") as f:
        f.write("container_id,row,bay,level\n")
        for box in boxes:
            f.write(f"{box['id']},{box['row']},{box['bay']},{box['level']}\n")

    base_time = 1705363200
    box_lookup = {b['id']: b for b in boxes}
    ws_count = config['workstation_count']

    commands = [] # [NEW] 建立記憶體內的 commands 陣列

    with open("mock_commands.csv", "w", newline="") as f:
        f.write("cmd_no,batch_id,cmd_type,cmd_priority,parent_carrier_id,"
                "src_row,src_bay,src_level,dest_row,dest_bay,dest_level,create_time\n")
        
        serial_no = 1
        for p_id in valid_job_sequence:
            if p_id not in box_lookup: continue
            box = box_lookup[p_id]
            ws_list = target_dest_map.get(p_id, [0]) 
            ws_sequence = "|".join(str(ws) for ws in ws_list)
            
            if ws_list: ws_count = max(ws_count, max(ws_list) + 1)
            
            create_time = base_time + serial_no * 60
            f.write(f"{serial_no},20260117,target,{serial_no},{p_id},"
                    f"{box['row']},{box['bay']},{box['level']},"
                    f"-1,{ws_sequence},1,{create_time}\n")
            
            # [NEW] 將資料存入記憶體陣列
            commands.append({
                'id': p_id,
                'type': 'target',
                'dest': {'row': -1, 'bay': ws_sequence, 'level': 1}
            })
            serial_no += 1

    config['workstation_count'] = ws_count

    with open("yard_config.csv", "w", newline="") as f:
        f.write("max_row,max_bay,max_level,total_boxes,time_travel_unit,time_handle,time_port_handle,time_unit_process,workstation_count\n")
        f.write(f"{config['max_row']},{config['max_bay']},{config['max_level']},{config['total_boxes']},"
                f"{config['t_travel']},{config['t_handle']},{config['t_port_handle']},{config['t_unit_process']},{ws_count}\n")

    print(f"Success! Data loaded straight into RAM. (CSV backed up)")

    
    
    return config, boxes, commands, parent_quantity_map, selection_run_id_info

class YardDataGenerator:
    def generate_random(self, **kwargs):
        # 簡易封裝以對接 main.py 的呼叫
        return generate_random(
            kwargs.get('max_row', 6), kwargs.get('max_bay', 11), kwargs.get('max_level', 8),
            kwargs.get('total_boxes', 400), kwargs.get('mission_count', 50), kwargs.get('workstation_count', 3)
        )
    def generate_db(self, base_config, run_id, target_run_id):
        return generate_db(base_config, run_id, target_run_id)