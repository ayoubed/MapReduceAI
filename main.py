from collections import defaultdict, deque
import logging
from task import RetryPolicy, UnreliableTask, TaskResult
from task_scheduler import TaskScheduler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(message)s'
)

if __name__ == "__main__": 
    task_a = UnreliableTask(
        work_time=1.0,
        failure_probability=0.5,
        result_value="A result",
        task_id="A",
        timeout=2.0,
        retry_policy=RetryPolicy(max_retries=3,initial_delay=1.0,max_delay=5.0,backoff_factor=2.0,jitter=True)
    )
    
    task_b = UnreliableTask(
        work_time=1.5,
        failure_probability=0.3,
        result_value="B result",
        task_id="B",
        timeout=2.0,
        retry_policy=RetryPolicy(max_retries=4,initial_delay=1.0,max_delay=5.0,backoff_factor=2.0,jitter=True),
        required_dependencies=["A"]
    )
    
    task_c = UnreliableTask(
        work_time=1.0,
        failure_probability=0.4,
        result_value="C result",
        task_id="C",
        timeout=2.0,
        retry_policy=RetryPolicy(max_retries=5,initial_delay=1.0,max_delay=5.0,backoff_factor=2.0,jitter=True),
        optional_dependencies=["A"]
    )
    
    scheduler = TaskScheduler(
        default_timeout=2.0,
        default_retry_policy=RetryPolicy(max_retries=1,initial_delay=1.0,max_delay=5.0,backoff_factor=2.0,jitter=True)
    )
    
    for task in [task_a, task_b, task_c]:
        scheduler.add_task(task)
    
    results = scheduler.execute()
    
    for task_id, result in results.items():
        if result.timed_out:
            print(f"Task {task_id} timed out after {result.total_attempts} attempts")
        elif result.error:
            print(f"Task {task_id} failed after {result.total_attempts} attempts: {result.error}")
        else:
            print(f"Task {task_id} succeeded on attempt {result.attempt}/{result.total_attempts}: {result.output}")