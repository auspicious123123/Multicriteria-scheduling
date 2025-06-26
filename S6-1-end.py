import random
import time
import pandas as pd
from collections import defaultdict, deque



def generate_jobs(num_jobs):
    unique_processing_times = [random.randint(1, 10) for _ in range(num_jobs)]
    common_processing_times = [random.randint(1, 5) for _ in range(num_jobs)]
    delta = random.randint(1, 5)
    priority_relations = []
    all_pairs = [(i, j) for i in range(num_jobs) for j in range(num_jobs) if i != j]
    for _ in range(random.randint(0, num_jobs)):
        pair = random.choice(all_pairs)
        if pair not in priority_relations:
            priority_relations.append(pair)
    return unique_processing_times, common_processing_times, priority_relations, delta


def calculate_metrics(schedule, unique_processing_times, common_processing_times, delta):
    current_time = 0
    completion_times = {}
    for batch in schedule:
        if not batch:
            continue
        common_time = sum(common_processing_times[job] for job in batch) + delta
        current_time += common_time
        for job in batch:
            unique_time = unique_processing_times[job]
            current_time += unique_time
            completion_times[job] = current_time
    c_max = max(completion_times.values()) if completion_times else 0
    f_max = c_max  
    return c_max, f_max



def is_valid_schedule(schedule, priority_relations):
    flat_schedule = [job for batch in schedule if batch for job in batch]
    for a, b in priority_relations:
        if flat_schedule.index(a) > flat_schedule.index(b):
            return False
    return True


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


# Algorithm Cmax-Fmax
def algorithm_cmax_fmax(unique_processing_times, common_processing_times, priority_relations, delta):
    num_jobs = len(unique_processing_times)
    PO_T = []
    e = 0
    y_e = float('inf')
    S_e = construct_natural_feasible_schedule(num_jobs, priority_relations)

    while True:
        current_schedule = [batch for batch in S_e if batch]
        if not current_schedule:
            break
        y_e_plus_1 = calculate_metrics(current_schedule, unique_processing_times, common_processing_times, delta)[1]
        S_e_plus_1 = [[] for _ in range(num_jobs)]
        for i in range(num_jobs - 1, -1, -1):
            for j in range(len(S_e[i]) - 1, -1, -1):
                T_k = S_e[i][j]
                has_successor_moved = any(succ in S_e[i] for _, succ in priority_relations if _ == T_k)
                if has_successor_moved and i == 1:
                    S_e_plus_1 = []
                    break
                elif has_successor_moved:
                    S_e_plus_1[i - 1].append(T_k)
                    S_e[i].pop(j)
                    continue

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
            current_schedule = [batch for batch in S_e if batch]
            c_max, f_max = calculate_metrics(current_schedule, unique_processing_times, common_processing_times, delta)
            PO_T.append((c_max, f_max, current_schedule))
            break

        
        previous_completion = 0
        for i in range(num_jobs):
            batch = S_e_plus_1[i]
            if not batch:
                continue
            common_time = sum(common_processing_times[job] for job in batch) + delta
            start_time = previous_completion + delta
            start_time += sum(common_processing_times[job] for job in batch)
            lawler_scheduled = lawler_order(unique_processing_times, batch, priority_relations)
            S_e_plus_1[i] = lawler_scheduled
            current_time = start_time
            for job in lawler_scheduled:
                current_time += unique_processing_times[job]
            previous_completion = current_time

        S_e = S_e_plus_1
        e += 1

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


# Main function
def main():
    job_counts = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    results = []
    for num_jobs in job_counts:
        run_times = []
        for _ in range(10):
            unique_processing_times, common_processing_times, priority_relations, delta = generate_jobs(num_jobs)
            print(f"Number of Jobs: {num_jobs}")
            print(f"Processing Time of the Unique Subcomponent: {unique_processing_times}")
            print(f"Processing Time of the Common Component: {common_processing_times}")
            print(f"Priority Relationship: {priority_relations}")
            print(f"Batch Setup Time: {delta}")

            start_time = time.time()
            pareto_points = algorithm_cmax_fmax(unique_processing_times, common_processing_times, priority_relations, delta)
            end_time = time.time()
            run_time = end_time - start_time
            run_times.append(run_time)

            print(f"Identified Pareto Points (Cmax, fmax): {pareto_points}")
            print(f"Execution Time: {run_time:.6e} 秒")
            print("-" * 50)

        max_run_time = max(run_times)
        avg_run_time = sum(run_times) / len(run_times)
        results.append({
            'Number of Jobs': num_jobs,
            'Maximum Execution Time': f"{max_run_time:.6e}",
            'Average Execution Time': f"{avg_run_time:.6e}"
        })

    # Summarize into a table
    df = pd.DataFrame(results)
    print(df)


if __name__ == "__main__":
    main()
