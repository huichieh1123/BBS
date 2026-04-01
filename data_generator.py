import argparse
import csv
import random
import time
import os

class YardDataGenerator:
    def __init__(self):
        pass

    
    # 1. 隨機生成模式 (Random Generator Mode)
    
    def generate_random(self, max_row, max_bay, max_level, total_boxes, mission_count, workstation_count):
        capacity = max_row * max_bay * max_level
        if total_boxes > capacity:
            raise ValueError(f"Error: Total boxes exceeds yard capacity!")
        
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
        
        mission_count = min(mission_count, len(candidates))
        
        parent_quantity_map = {}
        job_sequence = []
        target_dest_map = {}

        for i in range(mission_count):
            box = candidates[i]
            p_id = box['id']
            job_sequence.append(p_id)
            
            num_stages = random.randint(1, min(3, workstation_count))
            ws_pool = list(range(0, workstation_count))
            random.shuffle(ws_pool)
            
            target_dest_map[p_id] = [-(ws + 1) for ws in ws_pool[:num_stages]]
            parent_quantity_map[p_id] = random.randint(10, 50)

        config_dict = {
            'max_row': max_row, 'max_bay': max_bay, 'max_level': max_level, 'total_boxes': total_boxes,
            'workstation_count': workstation_count
        }

        selection_run_id_info = {
            "Random": {"order_scenario": "random", "selection_algo_ver": "N/A", "batch_algo_ver": "N/A"}
        }
        
        return config_dict, all_boxes, job_sequence, parent_quantity_map, target_dest_map, selection_run_id_info

    
    # 2. 資料庫匯入模式 (DB Import Mode - Original Disk-Based)
    
    def parse_location_id(self, loc_id):
        if not loc_id or len(loc_id) < 10: return -1, -1, -1
        return int(loc_id[0:5]), int(loc_id[5:8]), int(loc_id[8:10])

    def parse_carrier_id(self, car_id):
        if not car_id: return 0
        clean_id = ''.join(filter(str.isdigit, car_id))
        return int(clean_id) + 1 if clean_id else 0

    def load_simulation_data(self, run_id, target_run_id, base_config):
        print(f"\n[DataGen] Disk-Based loading for ID: {run_id}")
        job_sequence = []
        target_dest_map = {}
        target_cmd_ids = set() 
        selection_run_id_info = {}

        try:
            with open('DB/cur_cmd_master.csv', 'r', encoding='utf-8-sig') as f:
                for row in csv.DictReader(f):
                    s_id = str(row.get('selection_run_id', '')).strip()
                    if s_id and s_id not in selection_run_id_info and target_run_id == s_id:
                        selection_run_id_info[s_id] = {
                            'order_scenario': str(row.get('order_scenario', '')).strip() or 'N/A',
                            'inv_scenario': str(row.get('inv_scenario', '')).strip() or 'N/A',
                            'selection_algo_ver': str(row.get('selection_algo_ver', '')).strip() or 'N/A',
                            'batch_algo_ver': str(row.get('batch_algo_ver', '')).strip() or 'N/A'
                        }
        except Exception as e: print(f"Metadata read error: {e}")

        csv_source = 'DB/cur_cmd_master.csv'
        if os.path.exists('resequence.csv'):
            with open('resequence.csv', 'r', encoding='utf-8-sig') as f:
                if run_id in f.read(): csv_source = 'resequence.csv'

        inv_scenario = ""
        try:
            with open(csv_source, 'r', encoding='utf-8-sig') as f:
                for row in csv.DictReader(f):
                    csv_run_id = str(row.get('selection_run_id', '')).strip()
                    reseq_run_id = str(row.get('reseq_id', '')).strip()
                    
                    if run_id.strip() in (csv_run_id, reseq_run_id):
                        if not inv_scenario: inv_scenario = str(row.get('inv_scenario', '')).strip()
                        p_id = self.parse_carrier_id(row.get('parent_carrier_id', ''))
                        if p_id == 0: continue
                        
                        dest_str = str(row.get('dest_position', '0')).strip()
                        ws_id = int(dest_str) if dest_str else 0
                        dest_bay = -(ws_id + 1)
                        
                        if p_id not in job_sequence: job_sequence.append(p_id)
                        if p_id not in target_dest_map: target_dest_map[p_id] = []
                        target_dest_map[p_id].append(dest_bay)
                        
                        cmd_id = str(row.get('cmd_id', '')).strip()
                        if cmd_id: target_cmd_ids.add(cmd_id)
        except Exception as e: print(f"Command load error: {e}")

        carrier_to_parent = {}
        try:
            with open('DB/cur_carrier.csv', 'r', encoding='utf-8-sig') as f:
                for row in csv.DictReader(f):
                    carrier_to_parent[row['carrier_id']] = self.parse_carrier_id(row['parent_carrier_id'])
        except Exception as e: print(f"Carrier mapping error: {e}")

        boxes = []
        seen_parents = set()
        max_id = 0
        try:
            # Check for Inventory Source
            inv_file = 'DB/cur_inventory.csv' if os.path.exists('DB/cur_inventory.csv') else 'DB/cur_carrier.csv'
            with open(inv_file, 'r', encoding='utf-8-sig') as f:
                for row in csv.DictReader(f):
                    # For cur_inventory.csv
                    if 'scenario' in row and row['scenario'] != inv_scenario: continue
                    
                    c_id = row.get('carrier_id')
                    p_id = carrier_to_parent.get(c_id)
                    loc_id = row.get('location_id', '')
                    r, b, l = self.parse_location_id(loc_id)
                    
                    if p_id and p_id not in seen_parents and r != -1:
                        boxes.append({'id': p_id, 'row': r, 'bay': b, 'level': l})
                        seen_parents.add(p_id)
                        if p_id > max_id: max_id = p_id
        except Exception as e: print(f"Inventory load error: {e}")

        parent_quantity_map = {}
        try:
            with open('DB/cur_cmd_detail.csv', 'r', encoding='utf-8-sig') as f:
                for row in csv.DictReader(f):
                    if str(row.get('cmd_id', '')).strip() in target_cmd_ids:
                        c_id = row.get('carrier_id', '').strip()
                        qty = int(float(row.get('quantity', '0') or 0))
                        p_id = carrier_to_parent.get(c_id)
                        if p_id: parent_quantity_map[p_id] = parent_quantity_map.get(p_id, 0) + qty
        except Exception as e: print(f"SKU quantity load error: {e}")
        
        for p_id in job_sequence:
            if p_id not in parent_quantity_map: parent_quantity_map[p_id] = 10 
            
        config = {
            'max_row': base_config['yard']['max_row'],
            'max_bay': base_config['yard']['max_bay'],
            'max_level': base_config['yard']['max_level'],
            'total_boxes': max_id,
            'agv_count': base_config['solver']['agv_count'],
            'port_count': base_config['yard']['port_count'],
            'workstation_count': base_config['yard']['workstation_count']
        }
        return config, boxes, job_sequence, parent_quantity_map, target_dest_map, selection_run_id_info

    def generate_db(self, base_config, run_id, target_run_id):
        return self.load_simulation_data(run_id, target_run_id, base_config)



# 3. 批量實驗管理器 (RAM-Caching Batch Data Manager)

class BatchDataManager(YardDataGenerator):
    def __init__(self):
        super().__init__()
        self.cached_master = []
        self.cached_detail = {} # cmd_id -> [rows]
        self.cached_carrier = {} # carrier_id -> parent_id
        self.cached_boxes_by_scenario = {} # scenario -> boxes_list
        self.max_id_by_scenario = {}

    def load_all_to_ram(self):
        print("\n[BatchLoader] Starting global data caching to RAM...")
        start_time = time.time()
        
        # 1. Load Carrier Mapping
        try:
            with open('DB/cur_carrier.csv', 'r', encoding='utf-8-sig') as f:
                for row in csv.DictReader(f):
                    c_id = row['carrier_id'].strip()
                    p_id = self.parse_carrier_id(row['parent_carrier_id'])
                    self.cached_carrier[c_id] = p_id
        except Exception as e: print(f"Carrier cache error: {e}")

        # 2. Load CMD Master
        try:
            with open('DB/cur_cmd_master.csv', 'r', encoding='utf-8-sig') as f:
                self.cached_master = list(csv.DictReader(f))
        except Exception as e: print(f"Master cache error: {e}")

        # 3. Load CMD Detail (The Big One)
        try:
            with open('DB/cur_cmd_detail.csv', 'r', encoding='utf-8-sig') as f:
                for row in csv.DictReader(f):
                    cmd_id = row['cmd_id'].strip()
                    if cmd_id not in self.cached_detail: self.cached_detail[cmd_id] = []
                    self.cached_detail[cmd_id].append(row)
        except Exception as e: print(f"Detail cache error: {e}")

        # 4. Load Initial Yard Inventory
        try:
            inv_file = 'DB/cur_inventory.csv' if os.path.exists('DB/cur_inventory.csv') else 'DB/cur_carrier.csv'
            with open(inv_file, 'r', encoding='utf-8-sig') as f:
                for row in csv.DictReader(f):
                    scenario = row.get('scenario', 'default').strip()
                    if scenario not in self.cached_boxes_by_scenario:
                        self.cached_boxes_by_scenario[scenario] = []
                        self.max_id_by_scenario[scenario] = 0
                    
                    c_id = row.get('carrier_id').strip()
                    p_id = self.cached_carrier.get(c_id)
                    loc_id = row.get('location_id', '')
                    r, b, l = self.parse_location_id(loc_id)
                    
                    if p_id and r != -1:
                        # Simple de-duplication
                        self.cached_boxes_by_scenario[scenario].append({'id': p_id, 'row': r, 'bay': b, 'level': l})
                        if p_id > self.max_id_by_scenario[scenario]: self.max_id_by_scenario[scenario] = p_id
        except Exception as e: print(f"Inventory cache error: {e}")

        print(f"[BatchLoader] Cache completed in {time.time() - start_time:.2f}s")

    def get_data_for_run(self, run_id, base_config):
        job_sequence = []
        target_dest_map = {}
        target_cmd_ids = set()
        selection_run_id_info = {}
        inv_scenario = ""

        # Filter Master from RAM
        for row in self.cached_master:
            rid = str(row.get('selection_run_id', '')).strip()
            if rid == run_id:
                if not inv_scenario: 
                    inv_scenario = str(row.get('inv_scenario', '')).strip()
                    selection_run_id_info[rid] = {
                        'order_scenario': str(row.get('order_scenario', '')).strip(),
                        'inv_scenario': inv_scenario,
                        'selection_algo_ver': str(row.get('selection_algo_ver', '')).strip(),
                        'batch_algo_ver': str(row.get('batch_algo_ver', '')).strip()
                    }
                
                p_id = self.parse_carrier_id(row.get('parent_carrier_id', ''))
                if p_id == 0: continue
                
                dest_str = str(row.get('dest_position', '0')).strip()
                ws_id = int(dest_str) if dest_str else 0
                dest_bay = -(ws_id + 1)
                
                if p_id not in job_sequence: job_sequence.append(p_id)
                if p_id not in target_dest_map: target_dest_map[p_id] = []
                target_dest_map[p_id].append(dest_bay)
                
                cmd_id = str(row.get('cmd_id', '')).strip()
                if cmd_id: target_cmd_ids.add(cmd_id)

        # Get Boxes from RAM
        boxes = self.cached_boxes_by_scenario.get(inv_scenario, [])
        if not boxes and 'default' in self.cached_boxes_by_scenario:
            boxes = self.cached_boxes_by_scenario['default']
        
        max_id = self.max_id_by_scenario.get(inv_scenario, 0)

        # Calculate SKU Qty from RAM
        parent_quantity_map = {}
        for cmd_id in target_cmd_ids:
            details = self.cached_detail.get(cmd_id, [])
            for row in details:
                c_id = row['carrier_id'].strip()
                qty = int(float(row.get('quantity', '0') or 0))
                p_id = self.cached_carrier.get(c_id)
                if p_id: parent_quantity_map[p_id] = parent_quantity_map.get(p_id, 0) + qty
        
        for p_id in job_sequence:
            if p_id not in parent_quantity_map: parent_quantity_map[p_id] = 10 

        config = {
            'max_row': base_config['yard']['max_row'],
            'max_bay': base_config['yard']['max_bay'],
            'max_level': base_config['yard']['max_level'],
            'total_boxes': max_id,
            'agv_count': base_config['solver']['agv_count'],
            'port_count': base_config['yard']['port_count'],
            'workstation_count': base_config['yard']['workstation_count']
        }
        return config, boxes, job_sequence, parent_quantity_map, target_dest_map, selection_run_id_info