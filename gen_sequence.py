import csv
import sys
import os
from datetime import datetime, timedelta

class SequenceOptimizer:
    def __init__(self, db_dir="DB", output_file="resequence.csv"):
        self.db_dir = db_dir
        self.output_file = output_file
        
        # 將原本的區域變數提升為物件屬性 (狀態)
        self.selected_ids = []
        self.inv_scenario = ""
        self.all_target_dest_map = {} 
        self.cmd_info_map = {}        
        self.box_pos = {}
        self.stacks = {}
        self.target_set = set()
        self.target_stacks = {}
        self.final_seq = []

    # --- Static Parsing Tools ---
    @staticmethod
    def parse_location_id(loc_id):
        if not loc_id or len(loc_id) < 10: return -1, -1, -1
        return int(loc_id[0:5]), int(loc_id[5:8]), int(loc_id[8:10])

    @staticmethod
    def parse_carrier_id(car_id):
        if not car_id: return 0
        clean_id = ''.join(filter(str.isdigit, car_id))
        return int(clean_id) + 1 if clean_id else 0

    # --- Core Pipeline Methods ---
    def _load_commands(self, num_batches, start_id):
        """讀取目標指令與基礎資訊"""
        csv_source = os.path.join(self.db_dir, 'cur_cmd_master.csv')
        try:
            with open(csv_source, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                start_collecting = (start_id is None)
                
                for row in reader:
                    current_run_id = row['selection_run_id']
                    if not start_collecting:
                        if current_run_id == start_id: start_collecting = True
                        else: continue
                    
                    if current_run_id not in self.selected_ids:
                        if len(self.selected_ids) >= num_batches: break
                        self.selected_ids.append(current_run_id)
                    
                    if not self.inv_scenario: 
                        self.inv_scenario = row['inv_scenario']
                        
                    p_id = self.parse_carrier_id(row['parent_carrier_id'])
                    if p_id == 0: continue
                    
                    ws_num = int(row['dest_position'])
                    dest_bay = -(ws_num + 1)
                    
                    if p_id not in self.all_target_dest_map:
                        self.all_target_dest_map[p_id] = []
                        self.cmd_info_map[p_id] = [] 
                        
                    self.all_target_dest_map[p_id].append(dest_bay)
                    self.cmd_info_map[p_id].append(row) 
                    
            return True
        except FileNotFoundError:
            print(f"Error: {csv_source} not found.")
            return False

    def _load_inventory(self):
        """讀取 Carrier 與 Inventory 建立貨櫃堆疊狀態"""
        carrier_to_parent = {}
        carrier_csv = os.path.join(self.db_dir, 'cur_carrier.csv')
        inv_csv = os.path.join(self.db_dir, 'cur_inventory.csv')

        with open(carrier_csv, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                carrier_to_parent[row['carrier_id']] = self.parse_carrier_id(row['parent_carrier_id'])

        with open(inv_csv, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['scenario'] == self.inv_scenario:
                    p_id = carrier_to_parent.get(row['carrier_id'])
                    if p_id and p_id in self.all_target_dest_map:
                        r, b, l = self.parse_location_id(row['location_id'])
                        self.box_pos[p_id] = (r, b, l)
                        self.stacks.setdefault((r, b), []).append(p_id)
        
        for col in self.stacks: 
            self.stacks[col].sort(key=lambda x: self.box_pos[x][2])

        self.target_set = set(self.all_target_dest_map.keys())
        for tid in self.target_set:
            if tid in self.box_pos:
                col = (self.box_pos[tid][0], self.box_pos[tid][1])
                self.target_stacks.setdefault(col, []).append(tid)
                
        for col in self.target_stacks: 
            self.target_stacks[col].sort(key=lambda x: self.box_pos[x][2])

    def _get_score(self, tid):
        """系統評分邏輯 (Greedy 排序)"""
        r, b, l = self.box_pos[tid]
        bi = sum(1 for o in self.stacks[(r, b)] if self.box_pos[o][2] < l)
        ui = sum(1 for o in self.stacks[(r, b)] if o in self.target_set and self.box_pos[o][2] > l)
        di = min(abs(r - (-1)) + abs(b - (-(w+1))) for w in range(3))
        return (2.0 * bi) - (5.0 * ui) + (0.5 * di)

    def _optimize_sequence(self):
        """執行排序計算"""
        candidates = {col: s.pop(0) for col, s in self.target_stacks.items() if s}

        while candidates:
            best_tid = min(candidates.values(), key=self._get_score)
            self.final_seq.append(best_tid)
            best_col = (self.box_pos[best_tid][0], self.box_pos[best_tid][1])
            if self.target_stacks.get(best_col): 
                candidates[best_col] = self.target_stacks[best_col].pop(0)
            else: 
                del candidates[best_col]

    def _export_results(self):
        """寫入結果至 CSV"""
        with open(self.output_file, 'w', newline='', encoding='utf-8-sig') as f:
            fieldnames = ["reseq_id", "selection_run_id", "inv_scenario", "parent_carrier_id", "dest_position", "cmd_id"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            local_now = datetime.now() + timedelta(hours=8)
            new_run_id = "RESEQ_" + local_now.strftime("%Y%m%d_%H%M%S")
            
            for tid in self.final_seq:
                for info in self.cmd_info_map[tid]:
                    writer.writerow({
                        "reseq_id": new_run_id,
                        "selection_run_id": info['selection_run_id'], 
                        "inv_scenario": self.inv_scenario,
                        "parent_carrier_id": info['parent_carrier_id'],
                        "dest_position": info['dest_position'],
                        "cmd_id": info['cmd_id']
                    })

        return self.output_file, new_run_id

    # --- Public API ---
    def generate(self, num_batches=10, start_id=None):
        """
        對外公開的生成介面，依照順序執行完整 Pipeline
        """
        # 1. 讀取指令
        success = self._load_commands(num_batches, start_id)
        if not success or not self.selected_ids:
            print("Error: No IDs collected or files missing.")
            return None, None
            
        print(f"Collected Batches: {self.selected_ids}")

        # 2. 讀取庫存與狀態
        self._load_inventory()

        # 3. 執行排序計算
        self._optimize_sequence()

        # 4. 輸出檔案
        return self._export_results()

# 保留命令列直接執行的能力
if __name__ == '__main__':
    count = 10
    start = None
    if len(sys.argv) > 1: count = int(sys.argv[1])
    if len(sys.argv) > 2: start = sys.argv[2]
    
    optimizer = SequenceOptimizer()
    out_file, run_id = optimizer.generate(count, start)
    if run_id:
        print(f"Optimized sequence generated successfully: {run_id}")