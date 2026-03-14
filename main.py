import time
import csv
import os
import sys
import datetime
import yaml

import bs_solver
import gen_sequence
import data_generator

# ==========================================
# [NEW] 雙向輸出器：將 print() 同時導向終端機與檔案
# ==========================================
class DualLogger:
    def __init__(self, filepath):
        self.terminal = sys.stdout
        self.log_file = open(filepath, "w", encoding='utf-8')

    def write(self, message):
        # self.terminal.write(message)
        self.log_file.write(message)
        self.log_file.flush() # 確保即時寫入，避免當機時遺失

    def flush(self):
        # self.terminal.flush()
        self.log_file.flush()

class YardSimulationController:
    def __init__(self, config_path="config.yaml"):
        # 1. 讀取 YAML
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
        except Exception as e:
            raise RuntimeError(f"Failed to load config.yaml: {e}")

        # 2. 基本屬性
        self.mode = self.config['simulation']['mode']
        self.target_run_id = self.config['simulation']['target_run_id']
        self.active_run_id = self.target_run_id
        
        self.data_gen = data_generator.YardDataGenerator()
        self.seq_optimizer = gen_sequence.SequenceOptimizer()
        
        self.yard_config = {}
        self.boxes = []
        self.commands = []
        self.job_sequence = []
        self.parent_quantity_map = {}
        self.selection_run_id_info = {} # [NEW] 用來存 Metadata

        # ==========================================
        # 3. 日誌系統初始化 (純時間戳資料夾)
        # ==========================================
        self.timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        log_base_dir = self.config.get('logging', {}).get('output_dir', 'logs')
        
        # 只用時間戳命名資料夾
        self.log_dir = os.path.join(log_base_dir, self.timestamp)
        os.makedirs(self.log_dir, exist_ok=True)
        
        # 將系統 stdout 導向至 execution_log.txt
        execution_log_path = os.path.join(self.log_dir, 'execution_log.txt')
        sys.stdout = DualLogger(execution_log_path)
        
        print("==================================================")
        print(f"YARD SIMULATION PIPELINE STARTED")
        print(f"Time  : {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Log Dir: {self.log_dir}")
        print("==================================================")

    def optimize_sequence(self):
        print("\n==================================================")
        print(" Phase 1: Rule-base Sequence Optimization")
        print("==================================================")
        if self.mode == "db":
            out_file, reseq_id = self.seq_optimizer.generate(num_batches=1, start_id=self.target_run_id)
            if not reseq_id:
                raise RuntimeError("Sequence generation failed.")
            self.active_run_id = reseq_id
            print(f"Optimized Sequence generated. New Run ID: {self.active_run_id}")
        else:
            print("Random mode selected. Skipping DB sequence optimization.")

    def prepare_data(self):
        print("\n==================================================")
        print(f" Phase 2: RAM Data Generation (Mode: {self.mode})")
        print("==================================================")
        
        if self.mode == "random":
            self.yard_config, self.boxes, self.commands, self.parent_quantity_map, self.selection_run_id_info = self.data_gen.generate_random(
                max_row=self.config['yard']['max_row'],
                max_bay=self.config['yard']['max_bay'],
                max_level=self.config['yard']['max_level'],
                total_boxes=self.config['random']['total_boxes'],
                mission_count=self.config['random']['mission_count'],
                workstation_count=self.config['yard']['workstation_count']
            )
        elif self.mode == "db":
            # [MODIFIED] 接住多出來的 selection_run_id_info
            self.yard_config, self.boxes, self.commands, self.parent_quantity_map, self.selection_run_id_info = self.data_gen.generate_db(self.config, self.active_run_id, self.target_run_id)
        
        if not self.boxes or not self.commands:
            raise RuntimeError("Data generation returned empty data!")

        self.job_sequence = []
        for cmd in self.commands:
            if cmd['id'] not in self.job_sequence:
                self.job_sequence.append(cmd['id'])
                
        print(f"Successfully loaded {len(self.boxes)} boxes and {len(self.job_sequence)} jobs into RAM.")

    def run_solver(self):
        print("\n==================================================")
        print(f" Phase 3: Running Beam Search ({len(self.job_sequence)} targets)")
        print("==================================================")
        
        bs_solver.set_config(
            self.config['time']['t_travel'], 
            self.config['time']['t_handle'], 
            self.config['time']['t_port_handle'],
            self.config['time']['t_unit_process'],
            self.config['solver']['agv_count'], 
            self.config['solver']['beam_width'], 
            self.yard_config['workstation_count']
        )

        flat_config = self.yard_config
        flat_config['agv_count'] = self.config['solver']['agv_count']
        flat_config['port_count'] = self.config['yard'].get('port_count', 3)
        flat_config['t_travel'] = self.config['time']['t_travel']
        flat_config['t_handle'] = self.config['time']['t_handle']
        flat_config['t_port_handle'] = self.config['time']['t_port_handle']
        flat_config['t_unit_process'] = self.config['time']['t_unit_process']

        logs = bs_solver.run_fixed_solver(
            flat_config, 
            self.boxes, 
            self.commands, 
            self.job_sequence, 
            self.parent_quantity_map
        )
        return logs

    def _export_results(self, logs, cpu_time):
        """內部方法：寫出 Detailed CSV 與 Summary CSV"""
        
        # 1. 匯出 詳細任務清單
        missions_file = os.path.join(self.log_dir, 'output_missions_python.csv')
        try:
            with open(missions_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["mission_no", "agv_id", "mission_type", "container_id", "related_target_id", "src_pos", "dst_pos", "start_time", "end_time", "makespan"])
                
                for log in logs:
                    s_str = f"work station {0 if log.src[1]<=4 else 1}(Port {(log.src[1]+1) if log.src[1]<=4 else (log.src[1]-5)})" if log.src[0] == -1 else f"({log.src[0]};{log.src[1]};{log.src[2]})"
                    d_str = f"work station {0 if log.dst[1]<=4 else 1}(Port {(log.dst[1]+1) if log.dst[1]<=4 else (log.dst[1]-5)})" if log.dst[0] == -1 else f"({log.dst[0]};{log.dst[1]};{log.dst[2]})"

                    writer.writerow([
                        log.mission_no, log.agv_id, log.mission_type, log.container_id, log.related_target_id,
                        s_str, d_str, log.start_time, log.end_time, log.makespan
                    ])
            print(f"\nDetails saved to: {missions_file}")
        except IOError as e:
            print(f"\nError writing missions file: {e}")

        # 2. 統計數據與萃取 Metadata
        total_tasks = len(logs)
        makespan = logs[-1].makespan if total_tasks > 0 else 0.0
        
        type_counts = {'target': 0, 'transfer': 0, 'return': 0, 'reshuffle': 0, 'temp_return': 0}
        for log in logs:
            type_counts[log.mission_type] = type_counts.get(log.mission_type, 0) + 1

        # 取得這次任務的 Metadata (如果找不到，給予 N/A)
        target_id_str = str(self.target_run_id).strip()
        
        # 嘗試取得 Metadata，如果精準命中就拿出來
        meta = self.selection_run_id_info.get(target_id_str, {})
        
        # 3. 匯出 mission_summary.csv
        summary_file = os.path.join(self.log_dir, 'mission_summary.csv')
        try:
            with open(summary_file, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(["Metric", "Value"])
                
                # --- A. 任務情境資訊 ---
                writer.writerow(["[Scenario Info]", ""])
                writer.writerow(["selection_run_id", self.target_run_id])
                writer.writerow(["order_scenario", meta.get('order_scenario', 'N/A')])
                writer.writerow(["inv_scenario", meta.get('inv_scenario', 'N/A')])
                writer.writerow(["selection_algo_ver", meta.get('selection_algo_ver', 'N/A')])
                writer.writerow(["batch_algo_ver", meta.get('batch_algo_ver', 'N/A')])
                writer.writerow(["batch_time_window", meta.get('batch_time_window', 'N/A')])
                writer.writerow(["", ""])
                
                # --- B. 運算結果統計 ---
                writer.writerow(["[Results]", ""])
                writer.writerow(["Number of Tasks", total_tasks])
                writer.writerow(["Makespan (s)", f"{makespan:.2f}"])
                writer.writerow(["CPU Time (s)", f"{cpu_time:.4f}"])
                writer.writerow(["", ""])
                
                # --- C. 任務類型分布 ---
                writer.writerow(["[Task Breakdown]", ""])
                writer.writerow(["target", type_counts.get('target', 0)])
                writer.writerow(["transfer", type_counts.get('transfer', 0)])
                writer.writerow(["return", type_counts.get('return', 0)])
                writer.writerow(["reshuffle", type_counts.get('reshuffle', 0)])
                writer.writerow(["temp_return", type_counts.get('temp_return', 0)])
                writer.writerow(["", ""])

                # --- D. 參數設定 ---
                writer.writerow(["[Arguments settings]", ""])
                writer.writerow(["t_travel", self.config['time']['t_travel']])
                writer.writerow(["t_handle", self.config['time']['t_handle']])
                writer.writerow(["t_port_handle", self.config['time']['t_port_handle']])
                writer.writerow(["t_unit_process", self.config['time']['t_unit_process']])
                writer.writerow(["agv_count", self.config['solver']['agv_count']])
                writer.writerow(["beam_width", self.config['solver']['beam_width']])
                writer.writerow(["max_row", self.config['yard']['max_row']])
                writer.writerow(["max_bay", self.config['yard']['max_bay']])
                writer.writerow(["max_level", self.config['yard']['max_level']])
                writer.writerow(["workstation_count", self.config['yard']['workstation_count']])
                writer.writerow(["port_count", self.config['yard']['port_count']])


            print(f"Summary saved to: {summary_file}")
            if total_tasks > 0:
                print(f"Final Makespan: {makespan:.2f}s")
        except IOError as e:
            print(f"Error writing summary file: {e}")

    def execute_pipeline(self):
        start_t = time.time()
        try:
            self.optimize_sequence()
            self.prepare_data()
            logs = self.run_solver()
            
            # 精準紀錄包含 Rule-base 與 C++ 演算法的總運算時間
            cpu_time = time.time() - start_t
            self._export_results(logs, cpu_time)
            
        except Exception as e:
            print(f"\n[Pipeline Failed] {e}")
        finally:
            total_time = time.time() - start_t
            print(f"\nTotal Execution Time: {total_time:.2f}s")
            
            # 安全還原 stdout (這一步很重要，讓程式優雅結束)
            if isinstance(sys.stdout, DualLogger):
                sys.stdout = sys.stdout.terminal

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Yard Simulation Pipeline")
    parser.add_argument('--config', type=str, default='config.yaml', help='Path to YAML config file')
    
    parser.add_argument('--mode', choices=['random', 'db'], help='Override simulation mode')
    parser.add_argument('--run_id', type=str, help='Override target run ID for DB mode')
    
    parser.add_argument('--rows', type=int, help='Override max_row')
    parser.add_argument('--bays', type=int, help='Override max_bay')
    parser.add_argument('--levels', type=int, help='Override max_level')
    parser.add_argument('--boxes', type=int, help='Override total_boxes')
    parser.add_argument('--missions', type=int, help='Override mission_count')
    parser.add_argument('--ws', type=int, dest='workstations', help='Override workstation_count')

    args = parser.parse_args()

    sim_controller = YardSimulationController(config_path=args.config)

    if args.mode: sim_controller.config['simulation']['mode'] = args.mode
    if args.run_id: sim_controller.config['simulation']['target_run_id'] = args.run_id
    if args.rows: sim_controller.config['yard']['max_row'] = args.rows
    if args.bays: sim_controller.config['yard']['max_bay'] = args.bays
    if args.levels: sim_controller.config['yard']['max_level'] = args.levels
    if args.workstations: sim_controller.config['yard']['workstation_count'] = args.workstations
    if args.boxes: sim_controller.config['random']['total_boxes'] = args.boxes
    if args.missions: sim_controller.config['random']['mission_count'] = args.missions

    sim_controller.mode = sim_controller.config['simulation']['mode']
    sim_controller.target_run_id = sim_controller.config['simulation']['target_run_id']
    sim_controller.active_run_id = sim_controller.target_run_id

    print(f"Starting simulation in [{sim_controller.mode.upper()}] mode...")
    sim_controller.execute_pipeline()