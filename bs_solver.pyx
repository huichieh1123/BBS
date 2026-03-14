# distutils: language = c++
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True

from libcpp.vector cimport vector
from libcpp.string cimport string
from libcpp.unordered_map cimport unordered_map
from libcpp.algorithm cimport sort
from libcpp.cmath cimport abs
from cython.parallel import prange
from libc.math cimport fmax, fmin
from libc.stdlib cimport rand, RAND_MAX, srand
import time

# ==========================================
# 1. C++ Struct Definitions
# ==========================================
cdef extern from *:
    """
    #include <vector>
    #include <string>
    #include <cmath>
    #include <algorithm>
    #include <iostream>
    #include <limits>
    #include <random>
    #include <unordered_map>

    struct TimeWindow {
        double start;
        double end;
        TimeWindow() : start(0.0), end(0.0) {}
        TimeWindow(double s, double e) : start(s), end(e) {}
    };

    double get_earliest_start_time(const std::vector<TimeWindow>& free_windows, double ready_time, double duration) {
        if (!free_windows.empty()) {
            const TimeWindow& last_win = free_windows.back();
            double start_t = std::max(ready_time, last_win.start);
            if (start_t >= last_win.start && last_win.end >= start_t + duration) {
                return start_t;
            }
        }
        
        auto it = std::lower_bound(free_windows.begin(), free_windows.end(), ready_time,
            [](const TimeWindow& w, double val) { return w.end <= val; });
            
        for (; it != free_windows.end(); ++it) {
            double start_t = std::max(ready_time, it->start);
            if (it->end - start_t >= duration) {
                return start_t;
            }
        }
        return ready_time; 
    }

    double book_time_window(std::vector<TimeWindow>& free_windows, double ready_time, double duration) {
        if (!free_windows.empty()) {
            TimeWindow& last_win = free_windows.back();
            double start_t = std::max(ready_time, last_win.start);
            if (start_t >= last_win.start && last_win.end >= start_t + duration) {
                if (start_t > last_win.start) {
                    double old_end = last_win.end;
                    last_win.end = start_t;
                    free_windows.push_back(TimeWindow(start_t + duration, old_end));
                } else {
                    last_win.start = start_t + duration;
                }
                return start_t;
            }
        }

        auto it = std::lower_bound(free_windows.begin(), free_windows.end(), ready_time,
            [](const TimeWindow& w, double val) { return w.end <= val; });

        for (; it != free_windows.end(); ++it) {
            double start_t = std::max(ready_time, it->start);
            if (it->end - start_t >= duration) {
                double end_t = start_t + duration;
                if (start_t > it->start) {
                    double old_end = it->end;
                    it->end = start_t;
                    free_windows.insert(it + 1, TimeWindow(end_t, old_end));
                } else {
                    it->start = end_t;
                    if (it->start >= it->end) {
                        free_windows.erase(it);
                    }
                }
                return start_t;
            }
        }
        return ready_time;
    }

    struct Coordinate {
        int row;
        int bay;  
        int tier; 
        
        Coordinate() : row(-1), bay(-1), tier(-1) {}
        Coordinate(int r, int b, int t) : row(r), bay(b), tier(t) {}

        bool operator==(const Coordinate& other) const {
            return row == other.row && bay == other.bay && tier == other.tier;
        }
        
        bool operator<(const Coordinate& other) const {
            if (row != other.row) return row < other.row;
            if (bay != other.bay) return bay < other.bay;
            return tier < other.tier;
        }
    };

    Coordinate make_coord(int r, int b, int t) {
        return Coordinate(r, b, t);
    }

    struct Agent {
        int id;
        Coordinate currentPos;
        double availableTime;
    };

    struct MissionLog {
        int mission_no;
        int agv_id;
        int batch_id;
        int container_id;
        int related_target_id;
        Coordinate src;
        Coordinate dst;
        int mission_priority;
        long long start_time_epoch;
        long long end_time_epoch;
        double makespan_snapshot;
        int type_code; 
        int mission_status; 
    };

    struct YardSystem {
        int MAX_ROWS;
        int MAX_BAYS;
        int MAX_TIERS;
        std::vector<std::vector<std::vector<int>>> grid;
        std::vector<Coordinate> boxLocations;

        void init(int r, int b, int t, int total) {
            MAX_ROWS = r; MAX_BAYS = b; MAX_TIERS = t;
            grid.resize(r, std::vector<std::vector<int>>(b, std::vector<int>(t, 0)));
            boxLocations.resize(total + 1, Coordinate(-1, -1, -1));
        }

        void initBox(int id, int r, int b, int t) {
            if(r >= MAX_ROWS || b >= MAX_BAYS || t >= MAX_TIERS) return;
            grid[r][b][t] = id;
            if (id >= boxLocations.size()) boxLocations.resize(id + 1, Coordinate(-1, -1, -1));
            boxLocations[id] = Coordinate(r, b, t);
        }

        // [MODIFIED] 修改 moveToPort，使其寫入真實的物理 bay 座位
        void moveToPort(int id, int ws_id, int port_id) {
            if (id >= boxLocations.size()) return;
            Coordinate pos = boxLocations[id];
            if (pos.row != -1) {
                grid[pos.row][pos.bay][pos.tier] = 0;
            }
            int dest_bay = 0;
            if (ws_id == 0) dest_bay = port_id - 1;       // WS 1: 0,1,2,3,4
            else if (ws_id == 1) dest_bay = port_id + 5;  // WS 2: 6,7,8,9,10
            
            boxLocations[id] = Coordinate(-1, dest_bay, 0);
        }
        
        void returnFromPort(int id, int r, int b) {
            if (id >= boxLocations.size()) return;
            if (r < 0 || r >= MAX_ROWS || b < 0 || b >= MAX_BAYS) return;
            
            for (int t = 0; t < MAX_TIERS; ++t) {
                if (grid[r][b][t] == 0) {
                    grid[r][b][t] = id;
                    boxLocations[id] = Coordinate(r, b, t);
                    break;
                }
            }
        }

        void moveBox(int r1, int b1, int r2, int b2) {
            int t1 = -1;
            for (int t = 0; t < MAX_TIERS; ++t) {
                if (grid[r1][b1][t] != 0) {
                    t1 = t;
                    break;
                }
            }
            if (t1 == -1) return; 
            
            int id = grid[r1][b1][t1];
            grid[r1][b1][t1] = 0;

            for (int t = 0; t < MAX_TIERS; ++t) {
                if (grid[r2][b2][t] == 0) {
                    grid[r2][b2][t] = id;
                    boxLocations[id] = Coordinate(r2, b2, t);
                    break;
                }
            }
        }
        
        Coordinate getBoxPosition(int id) const {
            if (id >= boxLocations.size()) return Coordinate(-1, -1, -1);
            return boxLocations[id];
        }

        bool isBottom(int id) const {
            if (id >= boxLocations.size()) return false;
            Coordinate pos = boxLocations[id];
            if (pos.row == -1) return true; 
            
            for (int t = 0; t < pos.tier; ++t) {
                if (grid[pos.row][pos.bay][t] != 0) return false;
            }
            return true;
        }

        std::vector<int> getBlockingBoxes(int id) const {
            std::vector<int> blockers;
            if (id >= boxLocations.size()) return blockers;
            Coordinate pos = boxLocations[id];
            if (pos.row == -1) return blockers;
            
            for (int t = pos.tier - 1; t >= 0; --t) {
                if (grid[pos.row][pos.bay][t] != 0) {
                    blockers.push_back(grid[pos.row][pos.bay][t]);
                }
            }
            return blockers;
        }

        bool canReceiveBox(int r, int b) const {
             if (r < 0 || r >= MAX_ROWS || b < 0 || b >= MAX_BAYS) return false;
             for (int t = 0; t < MAX_TIERS; ++t) {
                 if (grid[r][b][t] == 0) return true;
             }
             return false;
        }
        
        int getLowestEmptyTier(int r, int b) const {
             if (r < 0 || r >= MAX_ROWS || b < 0 || b >= MAX_BAYS) return -1;
             for (int t = 0; t < MAX_TIERS; ++t) {
                 if (grid[r][b][t] == 0) return t;
             }
             return -1;
        }
    };

    struct SearchNode {
        YardSystem yard;
        std::vector<Agent> agvs;
        double g;
        double h;
        double f;
        std::vector<std::vector<double>> gridBusyTime;
        
        std::vector<std::vector<std::vector<TimeWindow>>> portsFreeWindows; 
        std::vector<double> containerReadyTime;
        std::vector<int> ws_progress; 
        std::vector<MissionLog> history;
        
        bool operator<(const SearchNode& other) const {
            return f < other.f;
        }
    };
    """
    
    cdef cppclass TimeWindow:
        double start
        double end
        TimeWindow() nogil
        TimeWindow(double, double) nogil

    double get_earliest_start_time(const vector[TimeWindow]&, double, double) nogil
    double book_time_window(vector[TimeWindow]&, double, double) nogil

    cdef cppclass Coordinate:
        int row
        int bay
        int tier
        bint operator==(const Coordinate&)

    Coordinate make_coord(int r, int b, int t) nogil

    cdef struct Agent:
        int id
        Coordinate currentPos
        double availableTime

    cdef struct MissionLog:
        int mission_no
        int agv_id
        int batch_id
        int container_id
        int related_target_id
        Coordinate src
        Coordinate dst
        int mission_priority
        long long start_time_epoch
        long long end_time_epoch
        double makespan_snapshot
        int type_code
        int mission_status

    cdef cppclass YardSystem:
        int MAX_ROWS
        int MAX_BAYS
        int MAX_TIERS
        vector[vector[vector[int]]] grid
        vector[Coordinate] boxLocations
        void init(int r, int b, int t, int total) nogil
        void initBox(int id, int r, int b, int t) nogil
        void moveToPort(int id, int ws_id, int port_id) nogil 
        void returnFromPort(int id, int r, int b) nogil 
        void moveBox(int r1, int b1, int r2, int b2) nogil
        Coordinate getBoxPosition(int id) nogil
        bint isBottom(int id) nogil 
        vector[int] getBlockingBoxes(int id) nogil
        bint canReceiveBox(int r, int b) nogil
        int getLowestEmptyTier(int r, int b) nogil

    cdef cppclass SearchNode:
        YardSystem yard
        vector[Agent] agvs
        double g
        double h
        double f
        vector[vector[double]] gridBusyTime
        vector[vector[vector[TimeWindow]]] portsFreeWindows
        vector[double] containerReadyTime
        vector[int] ws_progress
        vector[MissionLog] history
        bint operator<(const SearchNode&) const

    void printf(const char *format, ...) nogil

# ==========================================
# 2. Global Variables & Cython Helpers
# ==========================================
cdef double W_PENALTY_BLOCKING = 2000.0 
cdef double W_PENALTY_LOOKAHEAD = 500.0

cdef double TIME_TRAVEL_UNIT = 5.0
cdef double TIME_HANDLE = 30.0
cdef double TIME_PORT_HANDLE = 15.0
cdef double TIME_UNIT_PROCESS = 1.0

cdef int AGV_COUNT = 5
cdef int BEAM_WIDTH = 400
cdef int WORK_STATION_COUNT = 1
cdef int PORTS_PER_WS = 5

def set_config(double t_travel, double t_handle, double t_port_handle, double t_unit_process, int agv_cnt, int beam_w, int work_station_cnt):
    global TIME_TRAVEL_UNIT, TIME_HANDLE, TIME_PORT_HANDLE, TIME_UNIT_PROCESS, AGV_COUNT, BEAM_WIDTH, WORK_STATION_COUNT
    TIME_TRAVEL_UNIT = t_travel
    TIME_HANDLE = t_handle
    TIME_PORT_HANDLE = t_port_handle
    TIME_UNIT_PROCESS = t_unit_process
    AGV_COUNT = agv_cnt
    BEAM_WIDTH = beam_w
    WORK_STATION_COUNT = work_station_cnt

# [NEW] 真實物理座標轉換引擎
cdef inline int get_port_bay(int ws_id, int port_idx) noexcept nogil:
    if ws_id == 0: return port_idx - 1
    return port_idx + 5

cdef inline int get_ws_from_bay(int bay) noexcept nogil:
    if bay <= 4: return 0
    return 1

cdef inline int get_port_from_bay(int bay) noexcept nogil:
    if bay <= 4: return bay + 1
    return bay - 5

# ==========================================
# 3. Helper Functions
# ==========================================

cdef int getSeqIndex(int boxId, vector[int]& seq) noexcept nogil:
    for k in range(seq.size()):
        if seq[k] == boxId:
            return k
    return 999999 

cdef double calculateRILPenalty(YardSystem& yard, int r, int b, vector[int]& seq, int currentSeqIdx, int movingBoxId) noexcept nogil:
    cdef int insert_t = yard.getLowestEmptyTier(r, b)
    if insert_t == -1: return 1e9 

    cdef int movingBoxRank = getSeqIndex(movingBoxId, seq)
    cdef int t, boxId, rank
    cdef int blockingCount = 0
    cdef double penalty = 0.0
    cdef int belowBoxId, belowBoxRank

    for t in range(insert_t):
        boxId = yard.grid[r][b][t]
        if boxId == 0: continue
        rank = getSeqIndex(boxId, seq)
        if rank > movingBoxRank:
            blockingCount += 1

    if blockingCount > 0:
        penalty += W_PENALTY_BLOCKING * blockingCount 
    else:
        if insert_t > 0:
            belowBoxId = yard.grid[r][b][insert_t - 1]
            belowBoxRank = getSeqIndex(belowBoxId, seq)
            if belowBoxRank < movingBoxRank:
                penalty += 0.0 
            else:
                if belowBoxRank > currentSeqIdx: 
                    penalty += W_PENALTY_LOOKAHEAD / <double>(belowBoxRank - currentSeqIdx)

    return penalty

cdef double getTravelTime(Coordinate src, Coordinate dst) nogil:
    cdef int r1 = src.row
    cdef int b1 = src.bay 
    cdef int r2 = dst.row
    cdef int b2 = dst.bay 
    if r1 == -1: b1 = src.bay 
    if r2 == -1: b2 = dst.bay

    if r1 == -1 and r2 == -1 and b1 == b2 and src.tier == dst.tier: return 0.0
    if r1 == -1 and r2 == -1: return abs(b1 - b2) * TIME_TRAVEL_UNIT

    cdef double dist = abs(r1 - r2) + abs(b1 - b2)
    return dist * TIME_TRAVEL_UNIT

cdef double calculate_3D_UBALB(YardSystem& yard, vector[int]& ws_progress, vector[int]& seq, int currentSeqIdx, unordered_map[int, vector[int]]& targetMap, unordered_map[int, int]& boxQtyMap) noexcept nogil:
    cdef double total_time = 0.0
    cdef size_t i
    cdef int targetId, l, prog, total, next_ws
    cdef double dyn_proc
    cdef Coordinate curr
    
    for i in range(currentSeqIdx, seq.size()):
        targetId = seq[i]
        prog = ws_progress[targetId]
        
        if not targetMap.count(targetId): continue
        total = targetMap[targetId].size()
        
        if i == currentSeqIdx and prog == total: continue

        curr = yard.getBoxPosition(targetId)
        
        dyn_proc = 0.0
        if boxQtyMap.count(targetId):
            dyn_proc = <double>boxQtyMap[targetId] * TIME_UNIT_PROCESS
        
        if curr.row != -1:
            for l in range(0, curr.tier):
                if yard.grid[curr.row][curr.bay][l] != 0:
                    total_time += TIME_HANDLE + TIME_TRAVEL_UNIT + TIME_HANDLE

        for s in range(prog, total):
            next_ws = targetMap[targetId][s]
            # [MODIFIED] 使用真實物理座標計算預估抵達時間
            total_time += getTravelTime(curr, make_coord(-1, get_port_bay(next_ws, 1), 0)) + TIME_PORT_HANDLE + dyn_proc
            curr = make_coord(-1, get_port_bay(next_ws, 1), 0)

        total_time += getTravelTime(curr, make_coord(yard.MAX_ROWS/2, yard.MAX_BAYS/2, 0)) + TIME_HANDLE
        
    return total_time / <double>AGV_COUNT

cdef int calculateReturnPenalty(YardSystem& yard, int r, int b, vector[int]& seq, int currentSeqIdx) noexcept nogil:
    cdef int penalty = 0
    cdef int t, boxId, urgency
    cdef size_t k

    for t in range(yard.MAX_TIERS):
        boxId = yard.grid[r][b][t]
        if boxId == 0: continue
        for k in range(currentSeqIdx + 1, seq.size()):
            if seq[k] == boxId:
                urgency = (k - currentSeqIdx)
                penalty += 1000 // (urgency + 1)
    return penalty

# ==========================================
# 4. BBS Solver
# ==========================================
cdef vector[MissionLog] solveAndRecord(YardSystem& initialYard, vector[int]& seq, unordered_map[int, vector[int]]& targetMap, unordered_map[int, int]& boxQtyMap) noexcept nogil:
    srand(12345)
    
    cdef SearchNode root
    root.yard = initialYard
    root.g = 0
    root.h = 0
    root.f = 0
    
    root.gridBusyTime.resize(initialYard.MAX_ROWS, vector[double](initialYard.MAX_BAYS, 0.0))
    
    root.portsFreeWindows.resize(WORK_STATION_COUNT + 1)
    cdef int i, j
    for i in range(WORK_STATION_COUNT + 1):
        root.portsFreeWindows[i].resize(PORTS_PER_WS + 1)
        for j in range(PORTS_PER_WS + 1):
            root.portsFreeWindows[i][j].push_back(TimeWindow(0.0, 1e12))
            
    root.containerReadyTime.resize(initialYard.boxLocations.size(), 0.0)
    root.ws_progress.resize(initialYard.boxLocations.size(), 0)
    
    cdef Agent agv
    agv.currentPos = make_coord(0, 0, 0)
    agv.availableTime = 0.0
    
    for i in range(AGV_COUNT):
        agv.id = i
        root.agvs.push_back(agv)

    cdef vector[SearchNode] currentBeam
    currentBeam.push_back(root)
    
    cdef size_t seqIdx
    cdef int targetId, expansion_limit
    cdef bint targetCycleDone
    cdef vector[SearchNode] nextBeam
    
    cdef SearchNode node, newNode, newNodeTemp
    cdef Coordinate targetPos, src, dst, selectedPortCoord, dst_yard
    cdef int r, b, bestAGV, blockerId, selectedPort, targetWS, currentWS, bestDestPort
    cdef int curr_prog, total_stops, movingBoxId, p_idx
    cdef double bestFinishTime, bestStartTime, travel, start, travelToDest, finish, pickupDoneTime, maxAGV, pickupTime, penalty, noise
    cdef double agvArrivalAtPort, processStart, agvArrival, actualProcessStart
    cdef double minPortFinishTime, dropOffTime, agvFreeTime, portFinishTime, bestAGVFreeTime 
    cdef double dynamic_process_time
    cdef bint isBottom
    cdef vector[int] blockers
    cdef vector[int] ws_route
    cdef MissionLog log

    for seqIdx in range(seq.size()):
        targetId = seq[seqIdx]
        targetCycleDone = False
        expansion_limit = 0
        
        if targetMap.count(targetId):
            ws_route = targetMap[targetId]
        else:
            ws_route.push_back(1) 
            
        total_stops = ws_route.size()
        
        dynamic_process_time = 0.0
        if boxQtyMap.count(targetId):
            dynamic_process_time = <double>boxQtyMap[targetId] * TIME_UNIT_PROCESS
        
        while not targetCycleDone and expansion_limit < 400: 
            expansion_limit += 1
            nextBeam.clear()

            for node in currentBeam:
                targetPos = node.yard.getBoxPosition(targetId)
                curr_prog = node.ws_progress[targetId]

                # Case A: DONE
                if targetPos.row != -1 and curr_prog == total_stops:
                    nextBeam.push_back(node)
                    targetCycleDone = True
                    continue
                
                # Container is at a Workstation
                if targetPos.row == -1:
                    # [MODIFIED] 解碼真實座標，抓出所屬的 WS 和 Port ID
                    currentWS = get_ws_from_bay(targetPos.bay)
                    selectedPort = get_port_from_bay(targetPos.bay)
                    src = make_coord(-1, targetPos.bay, 0)
                    
                    if curr_prog < total_stops:
                        # ==========================================
                        # Case B1: Transfer Directly to Next Workstation
                        # ==========================================
                        targetWS = ws_route[curr_prog]
                        bestAGV = -1
                        bestFinishTime = 1e9
                        bestAGVFreeTime = 1e9
                        bestStartTime = 0
                        bestDestPort = -1
                        
                        for i in range(AGV_COUNT):
                            travel = getTravelTime(node.agvs[i].currentPos, src)
                            start = fmax(node.agvs[i].availableTime, node.containerReadyTime[targetId])
                            
                            for p_idx in range(1, PORTS_PER_WS + 1):
                                # [MODIFIED] 使用真實座標計算距離
                                travelToDest = getTravelTime(src, make_coord(-1, get_port_bay(targetWS, p_idx), 0))
                                agvArrivalAtPort = start + travel + TIME_PORT_HANDLE + travelToDest
                                
                                processStart = get_earliest_start_time(node.portsFreeWindows[targetWS][p_idx], agvArrivalAtPort, TIME_PORT_HANDLE + dynamic_process_time)
                                portFinishTime = processStart + TIME_PORT_HANDLE + dynamic_process_time
                                
                                if portFinishTime < bestFinishTime:
                                    bestFinishTime = portFinishTime
                                    bestAGVFreeTime = processStart + TIME_PORT_HANDLE 
                                    bestAGV = i
                                    bestStartTime = start
                                    bestDestPort = p_idx

                        newNode = node
                        newNode.yard.moveToPort(targetId, targetWS, bestDestPort)
                        newNode.ws_progress[targetId] = curr_prog + 1 
                        
                        selectedPortCoord = make_coord(-1, get_port_bay(targetWS, bestDestPort), 0)
                        agvArrival = bestStartTime + getTravelTime(newNode.agvs[bestAGV].currentPos, src) + TIME_PORT_HANDLE + getTravelTime(src, selectedPortCoord)
                        actualProcessStart = book_time_window(newNode.portsFreeWindows[targetWS][bestDestPort], agvArrival, TIME_PORT_HANDLE + dynamic_process_time)
                        
                        newNode.containerReadyTime[targetId] = actualProcessStart + TIME_PORT_HANDLE + dynamic_process_time
                        
                        newNode.agvs[bestAGV].currentPos = selectedPortCoord
                        newNode.agvs[bestAGV].availableTime = bestAGVFreeTime
                        
                        maxAGV = 0
                        for i in range(AGV_COUNT):
                            maxAGV = fmax(maxAGV, newNode.agvs[i].availableTime)
                        newNode.g = maxAGV
                        newNode.h = calculate_3D_UBALB(newNode.yard, newNode.ws_progress, seq, seqIdx, targetMap, boxQtyMap) 
                        noise = (<double>rand() / <double>RAND_MAX) * 0.01
                        newNode.f = newNode.g + newNode.h + noise

                        log.mission_no = newNode.history.size() + 1
                        log.agv_id = bestAGV
                        log.type_code = 3 
                        log.batch_id = 20260117
                        log.container_id = targetId
                        log.related_target_id = targetId
                        log.src = src
                        log.dst = selectedPortCoord 
                        log.start_time_epoch = <long long>bestStartTime + 0
                        log.end_time_epoch = <long long>bestAGVFreeTime + 0
                        log.makespan_snapshot = newNode.g
                        log.mission_priority = 0
                        log.mission_status = 0

                        newNode.history.push_back(log)
                        nextBeam.push_back(newNode)

                        # ==========================================
                        # Case B1-Temp: Temporary Buffer Return to Yard
                        # ==========================================
                        for r in range(node.yard.MAX_ROWS):
                            for b in range(node.yard.MAX_BAYS):
                                if not node.yard.canReceiveBox(r, b): continue
                                
                                dst_yard = make_coord(r, b, node.yard.getLowestEmptyTier(r, b))
                                penalty = calculateReturnPenalty(node.yard, r, b, seq, seqIdx)
                                
                                bestAGV = -1
                                bestFinishTime = 1e9
                                bestStartTime = 0
                                
                                for i in range(AGV_COUNT):
                                    travel = getTravelTime(node.agvs[i].currentPos, src)
                                    start = fmax(node.agvs[i].availableTime, node.containerReadyTime[targetId])
                                    travelToDest = getTravelTime(src, dst_yard)
                                    finish = start + travel + TIME_PORT_HANDLE + travelToDest + TIME_HANDLE
                                    
                                    if finish < bestFinishTime:
                                        bestFinishTime = finish
                                        bestAGV = i
                                        bestStartTime = start
                                
                                newNodeTemp = node
                                newNodeTemp.yard.returnFromPort(targetId, dst_yard.row, dst_yard.bay)
                                
                                newNodeTemp.agvs[bestAGV].currentPos = dst_yard
                                newNodeTemp.agvs[bestAGV].availableTime = bestFinishTime
                                newNodeTemp.gridBusyTime[dst_yard.row][dst_yard.bay] = bestFinishTime
                                
                                maxAGV = 0
                                for i in range(AGV_COUNT):
                                    maxAGV = fmax(maxAGV, newNodeTemp.agvs[i].availableTime)
                                newNodeTemp.g = maxAGV
                                newNodeTemp.h = calculate_3D_UBALB(newNodeTemp.yard, newNodeTemp.ws_progress, seq, seqIdx, targetMap, boxQtyMap) 
                                noise = (<double>rand() / <double>RAND_MAX) * 0.01
                                newNodeTemp.f = newNodeTemp.g + newNodeTemp.h + penalty + noise
                                
                                log.mission_no = newNodeTemp.history.size() + 1
                                log.agv_id = bestAGV
                                log.type_code = 4 
                                log.batch_id = 20260117
                                log.container_id = targetId
                                log.related_target_id = targetId
                                log.src = src
                                log.dst = dst_yard
                                log.start_time_epoch = <long long>bestStartTime + 0
                                log.end_time_epoch = <long long>bestFinishTime + 0
                                log.makespan_snapshot = newNodeTemp.g
                                log.mission_priority = 0
                                log.mission_status = 0
                                
                                newNodeTemp.history.push_back(log)
                                nextBeam.push_back(newNodeTemp)

                    else:
                        # ==========================================
                        # Case B2: Final Return to Yard
                        # ==========================================
                        for r in range(node.yard.MAX_ROWS):
                            for b in range(node.yard.MAX_BAYS):
                                if not node.yard.canReceiveBox(r, b): continue
                                
                                dst = make_coord(r, b, node.yard.getLowestEmptyTier(r, b))
                                penalty = calculateReturnPenalty(node.yard, r, b, seq, seqIdx)
                                
                                bestAGV = -1
                                bestFinishTime = 1e9
                                bestStartTime = 0
                                
                                for i in range(AGV_COUNT):
                                    travel = getTravelTime(node.agvs[i].currentPos, src)
                                    start = fmax(node.agvs[i].availableTime, node.containerReadyTime[targetId])
                                    travelToDest = getTravelTime(src, dst)
                                    finish = start + travel + TIME_PORT_HANDLE + travelToDest + TIME_HANDLE
                                    
                                    if finish < bestFinishTime:
                                        bestFinishTime = finish
                                        bestAGV = i
                                        bestStartTime = start
                                
                                newNode = node
                                newNode.yard.returnFromPort(targetId, dst.row, dst.bay)
                                
                                newNode.agvs[bestAGV].currentPos = dst
                                newNode.agvs[bestAGV].availableTime = bestFinishTime
                                newNode.gridBusyTime[dst.row][dst.bay] = bestFinishTime
                                
                                maxAGV = 0
                                for i in range(AGV_COUNT):
                                    maxAGV = fmax(maxAGV, newNode.agvs[i].availableTime)
                                newNode.g = maxAGV
                                newNode.h = calculate_3D_UBALB(newNode.yard, newNode.ws_progress, seq, seqIdx + 1, targetMap, boxQtyMap) 
                                noise = (<double>rand() / <double>RAND_MAX) * 0.01
                                newNode.f = newNode.g + newNode.h + penalty + noise
                                
                                log.mission_no = newNode.history.size() + 1
                                log.agv_id = bestAGV
                                log.type_code = 2 
                                log.batch_id = 20260117
                                log.container_id = targetId
                                log.related_target_id = targetId
                                log.src = src
                                log.dst = dst
                                log.start_time_epoch = <long long>bestStartTime + 0
                                log.end_time_epoch = <long long>bestFinishTime + 0
                                log.makespan_snapshot = newNode.g
                                log.mission_priority = 0
                                log.mission_status = 0
                                
                                newNode.history.push_back(log)
                                nextBeam.push_back(newNode)

                else:
                    isBottom = node.yard.isBottom(targetId)
                    if isBottom:
                        # ==========================================
                        # Case C: RETRIEVE (Yard -> Workstation)
                        # ==========================================
                        src = node.yard.getBoxPosition(targetId)
                        targetWS = ws_route[curr_prog]

                        bestAGV = -1
                        bestFinishTime = 1e9
                        bestAGVFreeTime = 1e9
                        bestStartTime = 0
                        bestDestPort = -1
                        
                        for i in range(AGV_COUNT):
                            travel = getTravelTime(node.agvs[i].currentPos, src)
                            start = fmax(node.agvs[i].availableTime, node.gridBusyTime[src.row][src.bay])
                            
                            for p_idx in range(1, PORTS_PER_WS + 1):
                                # [MODIFIED] 使用真實座標
                                travelToDest = getTravelTime(src, make_coord(-1, get_port_bay(targetWS, p_idx), 0))
                                agvArrivalAtPort = start + travel + TIME_HANDLE + travelToDest
                                
                                processStart = get_earliest_start_time(node.portsFreeWindows[targetWS][p_idx], agvArrivalAtPort, TIME_PORT_HANDLE + dynamic_process_time)
                                portFinishTime = processStart + TIME_PORT_HANDLE + dynamic_process_time
                                
                                if portFinishTime < bestFinishTime:
                                    bestFinishTime = portFinishTime
                                    bestAGVFreeTime = processStart + TIME_PORT_HANDLE 
                                    bestAGV = i
                                    bestStartTime = start
                                    bestDestPort = p_idx

                        newNode = node
                        newNode.yard.moveToPort(targetId, targetWS, bestDestPort)
                        newNode.ws_progress[targetId] = curr_prog + 1
                        
                        selectedPortCoord = make_coord(-1, get_port_bay(targetWS, bestDestPort), 0)
                        agvArrival = bestStartTime + getTravelTime(newNode.agvs[bestAGV].currentPos, src) + TIME_HANDLE + getTravelTime(src, selectedPortCoord)
                        actualProcessStart = book_time_window(newNode.portsFreeWindows[targetWS][bestDestPort], agvArrival, TIME_PORT_HANDLE + dynamic_process_time)
                        
                        newNode.containerReadyTime[targetId] = actualProcessStart + TIME_PORT_HANDLE + dynamic_process_time
                        
                        newNode.agvs[bestAGV].currentPos = selectedPortCoord
                        newNode.agvs[bestAGV].availableTime = bestAGVFreeTime
                        
                        pickupDoneTime = bestStartTime + getTravelTime(node.agvs[bestAGV].currentPos, src) + TIME_HANDLE
                        newNode.gridBusyTime[src.row][src.bay] = pickupDoneTime

                        maxAGV = 0
                        for i in range(AGV_COUNT):
                            maxAGV = fmax(maxAGV, newNode.agvs[i].availableTime)
                        newNode.g = maxAGV
                        newNode.h = calculate_3D_UBALB(newNode.yard, newNode.ws_progress, seq, seqIdx, targetMap, boxQtyMap) 
                        noise = (<double>rand() / <double>RAND_MAX) * 0.01
                        newNode.f = newNode.g + newNode.h + noise

                        log.mission_no = newNode.history.size() + 1
                        log.agv_id = bestAGV
                        log.type_code = 0 
                        log.batch_id = 20260117
                        log.container_id = targetId
                        log.related_target_id = targetId
                        log.src = src
                        log.dst = selectedPortCoord 
                        log.start_time_epoch = <long long>bestStartTime + 0
                        log.end_time_epoch = <long long>bestAGVFreeTime + 0
                        log.makespan_snapshot = newNode.g
                        log.mission_priority = 0
                        log.mission_status = 0

                        newNode.history.push_back(log)
                        nextBeam.push_back(newNode)
                    else:
                        # ==========================================
                        # Case D: RESHUFFLE
                        # ==========================================
                        blockers = node.yard.getBlockingBoxes(targetId)
                        if blockers.empty(): continue
                        
                        blockerId = blockers.back()
                        movingBoxId = blockerId 
                        src = node.yard.getBoxPosition(blockerId)

                        for r in range(node.yard.MAX_ROWS):
                            for b in range(node.yard.MAX_BAYS):
                                if r == src.row and b == src.bay: continue
                                if not node.yard.canReceiveBox(r, b): continue
                                
                                dst = make_coord(r, b, node.yard.getLowestEmptyTier(r, b))
                                penalty = calculateRILPenalty(node.yard, r, b, seq, seqIdx, movingBoxId)

                                bestAGV = -1
                                bestFinishTime = 1e9
                                bestStartTime = 0

                                for i in range(AGV_COUNT):
                                    travel = getTravelTime(node.agvs[i].currentPos, src)
                                    colReady = fmax(node.gridBusyTime[src.row][src.bay], node.gridBusyTime[r][b])
                                    start = fmax(node.agvs[i].availableTime, colReady)
                                    travelToDest = getTravelTime(src, dst)
                                    finish = start + travel + TIME_HANDLE + travelToDest + TIME_HANDLE
                                    if finish < bestFinishTime:
                                        bestFinishTime = finish
                                        bestAGV = i
                                        bestStartTime = start
                                
                                newNode = node
                                newNode.yard.moveBox(src.row, src.bay, dst.row, dst.bay)
                                newNode.agvs[bestAGV].currentPos = dst
                                newNode.agvs[bestAGV].availableTime = bestFinishTime
                                pickupTime = bestStartTime + getTravelTime(node.agvs[bestAGV].currentPos, src) + TIME_HANDLE
                                newNode.gridBusyTime[src.row][src.bay] = pickupTime
                                newNode.gridBusyTime[dst.row][dst.bay] = bestFinishTime
                                
                                maxAGV = 0
                                for i in range(AGV_COUNT):
                                    maxAGV = fmax(maxAGV, newNode.agvs[i].availableTime)
                                newNode.g = maxAGV
                                newNode.h = calculate_3D_UBALB(newNode.yard, newNode.ws_progress, seq, seqIdx, targetMap, boxQtyMap)
                                noise = (<double>rand() / <double>RAND_MAX) * 0.01
                                newNode.f = newNode.g + newNode.h + penalty + noise

                                log.mission_no = newNode.history.size() + 1
                                log.agv_id = bestAGV
                                log.type_code = 1 
                                log.batch_id = 20260117
                                log.container_id = blockerId
                                log.related_target_id = targetId
                                log.src = src
                                log.dst = dst
                                log.start_time_epoch = <long long>bestStartTime + 0
                                log.end_time_epoch = <long long>bestFinishTime + 0
                                log.makespan_snapshot = newNode.g
                                log.mission_priority = 0
                                log.mission_status = 0

                                newNode.history.push_back(log)
                                nextBeam.push_back(newNode)

            if nextBeam.empty(): break
            sort(nextBeam.begin(), nextBeam.end())
            if nextBeam.size() > BEAM_WIDTH:
                nextBeam.resize(BEAM_WIDTH)
            
            currentBeam = nextBeam
            
            checkPos = currentBeam[0].yard.getBoxPosition(targetId)
            if checkPos.row != -1 and currentBeam[0].ws_progress[targetId] == total_stops:
                targetCycleDone = True

        if currentBeam.empty(): return vector[MissionLog]()

    return currentBeam[0].history

# ==========================================
# 5. Entry Point
# ==========================================

cdef class PyMissionLog:
    cdef public int mission_no
    cdef public int agv_id
    cdef public str mission_type
    cdef public int container_id
    cdef public int related_target_id
    cdef public tuple src
    cdef public tuple dst
    cdef public long long start_time
    cdef public long long end_time
    cdef public double makespan

def run_fixed_solver(dict config, list boxes, list commands, list fixed_seq_ids, dict parent_quantity_map):
    cdef YardSystem initialYard
    initialYard.init(config['max_row'], config['max_bay'], config['max_level'], config['total_boxes'])
    
    for box in boxes:
        initialYard.initBox(box['id'], box['row'], box['bay'], box['level'])

    cdef vector[int] sequence
    cdef unordered_map[int, vector[int]] targetMap
    
    cdef unordered_map[int, int] cppQtyMap
    for k, v in parent_quantity_map.items():
        cppQtyMap[int(k)] = int(v)

    cdef vector[int] ws_list
    cdef int cid, ws_id
    cdef vector[MissionLog] finalLogs

    for cmd in commands:
        cid = int(cmd['id'])
        raw_ws = str(cmd['dest']['bay']) 
        
        ws_list.clear() 
        for ws_str in raw_ws.split('|'):
            try:
                ws_id = int(ws_str)
                if ws_id >= 0:
                    ws_list.push_back(ws_id)
            except ValueError:
                pass
        
        if ws_list.empty():
            ws_list.push_back(0) 
            
        targetMap[cid] = ws_list

    for pid in fixed_seq_ids:
        sequence.push_back(pid)
    
    print(f"Running Fixed Sequence Solver with {sequence.size()} targets...")
    
    finalLogs = solveAndRecord(initialYard, sequence, targetMap, cppQtyMap)
    
    py_logs = []
    for log in finalLogs:
        pl = PyMissionLog()
        pl.mission_no = log.mission_no
        pl.agv_id = log.agv_id
        pl.container_id = log.container_id
        pl.related_target_id = log.related_target_id
        
        if log.type_code == 0: pl.mission_type = "target"
        elif log.type_code == 1: pl.mission_type = "reshuffle"
        elif log.type_code == 2: pl.mission_type = "return"
        elif log.type_code == 3: pl.mission_type = "transfer"      
        elif log.type_code == 4: pl.mission_type = "temp_return"   
        else: pl.mission_type = "unknown"
        
        # 這裡現在會傳出真實的實體座標，例如 (-1, 0, 0)
        pl.src = (log.src.row, log.src.bay, log.src.tier)
        pl.dst = (log.dst.row, log.dst.bay, log.dst.tier)
        pl.start_time = log.start_time_epoch
        pl.end_time = log.end_time_epoch
        pl.makespan = log.makespan_snapshot
        py_logs.append(pl)
        
    return py_logs