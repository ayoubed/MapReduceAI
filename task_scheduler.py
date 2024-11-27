from collections import defaultdict, deque
import threading
from typing import List, Dict, Optional
import logging
from task import Task, RetryPolicy, TaskRegistry, TaskResult

class TaskScheduler:
    def __init__(
        self,
        default_timeout: Optional[float] = None,
        default_retry_policy: Optional[RetryPolicy] = None
    ):
        self.graph = defaultdict(list)
        self.in_degree = defaultdict(int)
        self.tasks = {}
        self.registry = TaskRegistry()
        self.default_timeout = default_timeout
        self.default_retry_policy = default_retry_policy or RetryPolicy()
        
    def add_task(self, task: Task):
        if task.timeout is None and self.default_timeout is not None:
            task.timeout = self.default_timeout
        if task.retry_policy is None:
            task.retry_policy = self.default_retry_policy
            
        self.tasks[task.task_id] = task
        for dep_id in task.all_dependency_ids:
            self.graph[dep_id].append(task.task_id)
            self.in_degree[task.task_id] += 1
                
        if task.task_id not in self.graph:
            self.graph[task.task_id] = []
            
    def topological_sort(self) -> List[List[str]]:
        in_degree = self.in_degree.copy()
        queue = deque()
        levels = []
        
        current_level = []
        for task in self.graph:
            if in_degree[task] == 0:
                current_level.append(task)
                
        if current_level:
            levels.append(current_level)
            queue.extend(current_level)
            
        while queue:
            next_level = []
            for _ in range(len(queue)):
                task = queue.popleft()
                
                for dependent in self.graph[task]:
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        next_level.append(dependent)
                        
            if next_level:
                levels.append(next_level)
                queue.extend(next_level)
                
        return levels
        
    def execute(self) -> Dict[str, TaskResult]:
        levels = self.topological_sort()
        
        for level_idx, level in enumerate(levels):
            logging.info(f"Executing level {level_idx + 1}")
            threads = []
            
            for task_id in level:
                thread = threading.Thread(
                    target=self.tasks[task_id],
                    name=f"Task-{task_id}",
                    args=(self.registry,)
                )
                threads.append(thread)
                thread.start()
                
            for thread in threads:
                thread.join()
                
            logging.info(f"Completed level {level_idx + 1}")
            
        return self.registry.results