import random
import time
import pandas as pd
from collections import defaultdict, deque


# 生成随机作业数据
def generate_jobs(num_jobs):
    # 唯一子组件加工时间
    unique_processing_times = [random.randint(1, 10) for _ in range(num_jobs)]
    # 公共组件加工时间（每个作业的公共子组件加工时间）
    common_processing_times = [random.randint(1, 5) for _ in range(num_jobs)]
    # 批次设置时间
    delta = random.randint(1, 5)
    # 优先级关系
    priority_relations = []
    all_pairs = [(i, j) for i in range(num_jobs) for j in range(num_jobs) if i != j]
    for _ in range(random.randint(0, num_jobs)):
        pair = random.choice(all_pairs)
        if pair not in priority_relations:
            priority_relations.append(pair)
    return unique_processing_times, common_processing_times, priority_relations, delta


# 计算制造跨度和最大成本（fmax = Lmax = 最晚完成时间）
def calculate_metrics(schedule, unique_processing_times, common_processing_times, delta):
    current_time = 0
    completion_times = {}
    for batch in schedule:
        if not batch:
            continue
        # 公共子组件处理时间：该批次所有作业的公共子组件时间之和 + 设置时间delta
        common_time = sum(common_processing_times[job] for job in batch) + delta
        current_time += common_time
        # 处理唯一子组件，按顺序处理每个作业
        for job in batch:
            unique_time = unique_processing_times[job]
            current_time += unique_time
            completion_times[job] = current_time
    c_max = max(completion_times.values()) if completion_times else 0
    f_max = c_max  # fmax 直接等于最晚完成时间（Lmax）
    return c_max, f_max


# 检查调度是否满足优先级关系
def is_valid_schedule(schedule, priority_relations):
    # 将批次列表展开为作业顺序
    flat_schedule = [job for batch in schedule if batch for job in batch]
    for a, b in priority_relations:
        if flat_schedule.index(a) > flat_schedule.index(b):
            return False
    return True


# 根据优先约束构建自然可行调度
def construct_natural_feasible_schedule(num_jobs, priority_relations):
    out_degree = defaultdict(int)
    for a, b in priority_relations:
        out_degree[a] += 1

    schedule = [[] for _ in range(num_jobs)]
    for i in range(num_jobs - 1, -1, -1):
        zero_out_degree_jobs = [job for job in range(num_jobs) if out_degree[job] == 0]
        for job in zero_out_degree_jobs:
            schedule[i].append(job)
            for a, b in priority_relations:
                if b == job:
                    out_degree[a] -= 1
    return schedule


# 根据Lawler顺序调整唯一子组件顺序
def lawler_order(unique_processing_times, jobs, priority_relations):
    def find_min_job(unscheduled_jobs, total_time):
        min_job = None
        min_cost = float('inf')
        for job in unscheduled_jobs:
            cost = total_time + unique_processing_times[job]
            if cost < min_cost:
                min_cost = cost
                min_job = job
        return min_job

    lawler_schedule = []
    unscheduled_jobs = jobs.copy()
    total_time = 0
    while unscheduled_jobs:
        min_job = find_min_job(unscheduled_jobs, total_time)
        lawler_schedule.append(min_job)
        unscheduled_jobs.remove(min_job)
        total_time += unique_processing_times[min_job]
    return lawler_schedule


# Algorithm Cmax-Fmax 算法
def algorithm_cmax_fmax(unique_processing_times, common_processing_times, priority_relations, delta):
    num_jobs = len(unique_processing_times)
    PO_T = []
    e = 0
    y_e = float('inf')
    S_e = construct_natural_feasible_schedule(num_jobs, priority_relations)

    while True:
        # 计算当前调度的最大成本作为新的上界
        current_schedule = [batch for batch in S_e if batch]
        if not current_schedule:
            break
        y_e_plus_1 = calculate_metrics(current_schedule, unique_processing_times, common_processing_times, delta)[1]
        S_e_plus_1 = [[] for _ in range(num_jobs)]

        # Step 2.1 调整调度
        for i in range(num_jobs - 1, -1, -1):
            for j in range(len(S_e[i]) - 1, -1, -1):
                T_k = S_e[i][j]
                # Case (1) 检查优先级约束冲突
                has_successor_moved = any(succ in S_e[i] for _, succ in priority_relations if _ == T_k)
                if has_successor_moved and i == 1:
                    S_e_plus_1 = []
                    break
                elif has_successor_moved:
                    S_e_plus_1[i - 1].append(T_k)
                    S_e[i].pop(j)
                    continue

                # Case (2) 检查成本约束
                c_max, f_max = calculate_metrics([[T_k]], unique_processing_times, common_processing_times, delta)
                if f_max >= y_e_plus_1:
                    if i == 1 or j == len(S_e[i]) - 1:
                        S_e_plus_1 = []
                        break
                    else:
                        products_to_move = S_e[i][j:]
                        S_e[i] = S_e[i][:j]
                        S_e_plus_1[i - 1].extend(products_to_move)
                        break

        if not S_e_plus_1:
            # 保存当前调度的帕累托点
            current_schedule = [batch for batch in S_e if batch]
            c_max, f_max = calculate_metrics(current_schedule, unique_processing_times, common_processing_times, delta)
            PO_T.append((c_max, f_max, current_schedule))
            break

        # Step 2.2 更新调度并按Lawler顺序处理唯一子组件
        previous_completion = 0
        for i in range(num_jobs):
            batch = S_e_plus_1[i]
            if not batch:
                continue
            # 处理公共子组件
            common_time = sum(common_processing_times[job] for job in batch) + delta
            start_time = previous_completion + delta
            start_time += sum(common_processing_times[job] for job in batch)
            # 按Lawler顺序处理唯一子组件
            lawler_scheduled = lawler_order(unique_processing_times, batch, priority_relations)
            S_e_plus_1[i] = lawler_scheduled
            # 更新时间
            current_time = start_time
            for job in lawler_scheduled:
                current_time += unique_processing_times[job]
            previous_completion = current_time

        S_e = S_e_plus_1
        e += 1

    # 处理帕累托点去重
    pareto_points = []
    for c_max, f_max, _ in PO_T:
        is_pareto = True
        for existing in pareto_points:
            if c_max >= existing[0] and f_max >= existing[1] and (c_max > existing[0] or f_max > existing[1]):
                is_pareto = False
                break
        if is_pareto:
            new_points = []
            for existing in pareto_points:
                if not (existing[0] >= c_max and existing[1] >= f_max and (existing[0] > c_max or existing[1] > f_max)):
                    new_points.append(existing)
            new_points.append((c_max, f_max))
            pareto_points = new_points

    return pareto_points


# 主函数
def main():
    job_counts = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    results = []
    for num_jobs in job_counts:
        run_times = []
        for _ in range(10):
            unique_processing_times, common_processing_times, priority_relations, delta = generate_jobs(num_jobs)
            print(f"作业数量: {num_jobs}")
            print(f"唯一子组件加工时间: {unique_processing_times}")
            print(f"公共组件加工时间: {common_processing_times}")
            print(f"优先级关系: {priority_relations}")
            print(f"批次设置时间: {delta}")

            start_time = time.time()
            pareto_points = algorithm_cmax_fmax(unique_processing_times, common_processing_times, priority_relations, delta)
            end_time = time.time()
            run_time = end_time - start_time
            run_times.append(run_time)

            print(f"找到的帕累托点 (Cmax, fmax): {pareto_points}")
            print(f"运行时间: {run_time:.6e} 秒")
            print("-" * 50)

        max_run_time = max(run_times)
        avg_run_time = sum(run_times) / len(run_times)
        results.append({
            '作业数量': num_jobs,
            '最大运行时间': f"{max_run_time:.6e}",
            '平均运行时间': f"{avg_run_time:.6e}"
        })

    # 汇总成表格
    df = pd.DataFrame(results)
    print(df)


if __name__ == "__main__":
    main()