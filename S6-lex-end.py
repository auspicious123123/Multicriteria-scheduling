import random
import time
from collections import deque


class Product:
    def __init__(self, id, t1, t2, f, g):
        self.id = id
        self.t1 = t1 
        self.t2 = t2  
        self.f = f    
        self.g = g    
        self.predecessors = set()  



class Schedule:
    def __init__(self, batches):
        self.batches = batches  



def generate_products(n):
    products = []
    for i in range(n):
        t1 = random.randint(1, 10) 
        t2 = random.randint(1, 10) 
        f = lambda C: C + random.randint(1, 10)  
        g = lambda C: C + random.randint(1, 10) 
        products.append(Product(i, t1, t2, f, g))

    
    priority_relations = []
    all_pairs = [(i, j) for i in range(n) for j in range(n) if i != j]
    for _ in range(random.randint(0, n)):
        pair = random.choice(all_pairs)
        if pair not in priority_relations:
            priority_relations.append(pair)

   
    for pair in priority_relations:
        products[pair[1]].predecessors.add(products[pair[0]])

    return products



def compute_costs(schedule):
    f_max = 0
    g_max = 0
    C_max = 0
    current_time = 0

    for batch in schedule.batches:
        batch_t1 = sum(p.t1 for p in batch)
        current_time += batch_t1

       
        for p in batch:
            p_completion_time = current_time + p.t2
            f_max = max(f_max, p.f(p_completion_time))
            g_max = max(g_max, p.g(p_completion_time))
            C_max = max(C_max, p_completion_time)
            current_time += p.t2

    return f_max, g_max, C_max


# Algorithm Cmax - Fmax
def algorithm_cmax_fmax(products):
    n = len(products)
    graph = {product: set() for product in products}
    for product in products:
        for predecessor in product.predecessors:
            graph[predecessor].add(product)

    PO_T = []
    e = 0
    y_e = float('inf')
    S_e = [deque() for _ in range(n)]
    unscheduled_products = set(products)
    for i in range(n - 1, -1, -1):
        out_degree_zero_products = [product for product in unscheduled_products if not graph[product]]
        for product in out_degree_zero_products:
            S_e[i].append(product)
            unscheduled_products.remove(product)

    while True:
        y_e_plus_1 = compute_costs(Schedule(S_e))[0]
        modified_S_e = [deque(sub_batch) for sub_batch in S_e]
        changed = False

        for i in range(n - 1, -1, -1):
            for j in range(len(modified_S_e[i]) - 1, -1, -1):
                p = modified_S_e[i][j]
                if any(succ in modified_S_e[i] for succ in p.predecessors):
                    if i == 0:
                        break
                    else:
                        moved_product = modified_S_e[i].popleft()
                        modified_S_e[i - 1].append(moved_product)
                        changed = True
                elif p.f(compute_costs(Schedule(modified_S_e))[2]) >= y_e_plus_1:
                    if i == 0 or j == len(modified_S_e[i]) - 1:
                        break
                    else:
                        moved_products = list(modified_S_e[i])[j:]
                        for product in moved_products:
                            modified_S_e[i].remove(product)
                            modified_S_e[i - 1].append(product)
                        changed = True

        if not changed:
            break
        S_e = modified_S_e
        e += 1
        y_e = y_e_plus_1

    final_S_star = Schedule(S_e)
    return final_S_star


# Algorithm Fmax - Gmax - Cmax
def algorithm_fmax_gmax_cmax(products):
    n = len(products)
    S_star = algorithm_cmax_fmax(products)
    S_e = [deque(sub_batch) for sub_batch in S_star.batches]
    S_e = Schedule(S_e)

    e = 0
    Y_e = float('inf')

    while True:
        Y_e_plus_1 = Y_e
        f_max, g_max, C_max = compute_costs(S_e)
        Y_e_plus_1 = g_max

        for i in range(n - 1, -1, -1):
            batch = S_e.batches[i]
            for j in range(len(batch) - 1, -1, -1):
                p = batch[j]
                if any(succ in batch for succ in p.predecessors) or p.f(C_max) > f_max or p.g(C_max) >= Y_e_plus_1:
                    if i == 0:
                        return S_e
                    else:
                        moved_products = list(batch)[j:]
                        for product in moved_products:
                            batch.remove(product)
                            S_e.batches[i - 1].append(product)

        if Y_e_plus_1 == Y_e:
            break
        e += 1
        Y_e = Y_e_plus_1

    return S_e


# Main function
def main():
    job_sizes = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    results = []

    for n in job_sizes:
        max_time = 0
        total_time = 0

        for _ in range(15):
            products = generate_products(n)

            start_time = time.time()
            schedule = algorithm_fmax_gmax_cmax(products)
            end_time = time.time()

            runtime = end_time - start_time
            max_time = max(max_time, runtime)
            total_time += runtime

            
            print(f"n = {n}")
            print("Generated Products:")
            for p in products:
                print(f"Product {p.id}: t1 = {p.t1}, t2 = {p.t2}, Predecessors = {[pred.id for pred in p.predecessors]}")

          
            f_max, g_max, C_max = compute_costs(schedule)
            print(f"Pareto Point: f_max = {f_max}, g_max = {g_max}, C_max = {C_max}")
            print(f"Runtime: {runtime:.6E} seconds")
            print()

        avg_time = total_time / 15
        results.append((n, avg_time, max_time))

   
    print("Summary Table:")
    print("n\tAvg Runtime (s)\tMax Runtime (s)")
    for n, avg_time, max_time in results:
        print(f"{n}\t{avg_time:.6E}\t{max_time:.6E}")


if __name__ == "__main__":
    main()
