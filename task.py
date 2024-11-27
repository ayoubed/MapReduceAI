import time
from typing import List, Dict, Optional, Any, NamedTuple
import logging
from abc import ABC, abstractmethod
import uuid
from threading import Lock, Event
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import random

@dataclass
class RetryPolicy:
    """Configuration for task retry behavior"""
    max_retries: int = 3
    initial_delay: float = 1.0
    max_delay: float = 30.0
    backoff_factor: float = 2.0
    jitter: bool = True

    def get_delay(self, attempt: int) -> float:
        """Calculate delay for current retry attempt"""
        delay = min(
            self.initial_delay * (self.backoff_factor ** (attempt - 1)),
            self.max_delay
        )
        if self.jitter:
            delay *= random.uniform(0.5, 1.5)
        return delay

@dataclass
class TaskResult:
    """Container for task execution results"""
    task_id: str
    output: Any
    error: Optional[Exception] = None
    completed: bool = False
    timed_out: bool = False
    attempt: int = 1
    total_attempts: int = 1

class Dependency(NamedTuple):
    """Represents a task dependency with optional flag"""
    task_id: str
    required: bool = True

class TaskRegistry:
    """Thread-safe registry for storing task results"""
    def __init__(self):
        self.results: Dict[str, TaskResult] = {}
        self._lock = Lock()
        
    def set_result(self, task_id: str, output: Any, attempt: int = 1, total_attempts: int = 1):
        with self._lock:
            self.results[task_id] = TaskResult(
                task_id=task_id, 
                output=output, 
                completed=True,
                attempt=attempt,
                total_attempts=total_attempts
            )
            
    def set_error(self, task_id: str, error: Exception, attempt: int = 1, total_attempts: int = 1):
        with self._lock:
            self.results[task_id] = TaskResult(
                task_id=task_id,
                output=None,
                error=error,
                completed=True,
                attempt=attempt,
                total_attempts=total_attempts
            )
            
    def set_timeout(self, task_id: str, attempt: int = 1, total_attempts: int = 1):
        with self._lock:
            self.results[task_id] = TaskResult(
                task_id=task_id,
                output=None,
                error=TimeoutError(f"Task timed out on attempt {attempt}/{total_attempts}"),
                completed=True,
                timed_out=True,
                attempt=attempt,
                total_attempts=total_attempts
            )
            
    def get_result(self, task_id: str) -> Optional[TaskResult]:
        with self._lock:
            return self.results.get(task_id)

class Task(ABC):
    def __init__(
        self, 
        task_id: Optional[str] = None, 
        required_dependencies: List[str] = None,
        optional_dependencies: List[str] = None,
        timeout: Optional[float] = None,
        retry_policy: Optional[RetryPolicy] = None
    ):
        self.task_id = task_id or str(uuid.uuid4())
        self.dependencies = [
            Dependency(task_id=dep, required=True) 
            for dep in (required_dependencies or [])
        ] + [
            Dependency(task_id=dep, required=False) 
            for dep in (optional_dependencies or [])
        ]
        self.timeout = timeout
        self.retry_policy = retry_policy or RetryPolicy()
        self._cancel_event = Event()
        
    @property
    def all_dependency_ids(self) -> List[str]:
        return [dep.task_id for dep in self.dependencies]
        
    @property
    def required_dependency_ids(self) -> List[str]:
        return [dep.task_id for dep in self.dependencies if dep.required]
        
    def cancel(self):
        self._cancel_event.set()
        
    def should_cancel(self) -> bool:
        return self._cancel_event.is_set()
        
    @abstractmethod
    def execute(self, inputs: Dict[str, Any]) -> Any:
        """Implement task logic here"""
        pass
        
    def _execute_with_timeout(self, inputs: Dict[str, Any], executor: ThreadPoolExecutor) -> Any:
        future = executor.submit(self.execute, inputs)
        try:
            return future.result(timeout=self.timeout)
        except TimeoutError:
            self.cancel()
            future.cancel()
            raise
            
    def _execute_with_retry(self, inputs: Dict[str, Any], registry: TaskRegistry):
        attempt = 1
        last_error = None
        
        while attempt <= self.retry_policy.max_retries:
            try:
                if self.timeout is not None:
                    with ThreadPoolExecutor(max_workers=1) as executor:
                        try:
                            output = self._execute_with_timeout(inputs, executor)
                            registry.set_result(
                                self.task_id, 
                                output,
                                attempt=attempt,
                                total_attempts=attempt
                            )
                            return
                        except TimeoutError as e:
                            last_error = e
                            logging.warning(
                                f"Task {self.task_id} timed out on attempt {attempt}/{self.retry_policy.max_retries}"
                            )
                else:
                    output = self.execute(inputs)
                    registry.set_result(
                        self.task_id,
                        output,
                        attempt=attempt,
                        total_attempts=attempt
                    )
                    return
                    
            except Exception as e:
                last_error = e
                logging.warning(
                    f"Task {self.task_id} failed on attempt {attempt}/{self.retry_policy.max_retries}: {str(e)}"
                )
                
            if attempt < self.retry_policy.max_retries:
                delay = self.retry_policy.get_delay(attempt)
                logging.info(f"Retrying task {self.task_id} in {delay:.2f} seconds")
                time.sleep(delay)
                
            attempt += 1
            
        if isinstance(last_error, TimeoutError):
            registry.set_timeout(
                self.task_id,
                attempt=attempt-1,
                total_attempts=attempt-1
            )
        else:
            registry.set_error(
                self.task_id,
                last_error,
                attempt=attempt-1,
                total_attempts=attempt-1
            )
        
    def __call__(self, registry: TaskRegistry):
        logging.info(f"Starting task {self.task_id}")
        try:
            inputs = {}
            for dep in self.dependencies:
                result = registry.get_result(dep.task_id)
                
                if dep.required:
                    if result is None:
                        raise Exception(f"Required dependency {dep.task_id} has no result")
                    if result.error:
                        raise Exception(f"Required dependency {dep.task_id} failed with error: {result.error}")
                    inputs[dep.task_id] = result.output
                else:
                    if result is not None and not result.error:
                        inputs[dep.task_id] = result.output
                    else:
                        logging.warning(f"Optional dependency {dep.task_id} not available")
            
            self._execute_with_retry(inputs, registry)
            
        except Exception as e:
            logging.error(f"Task {self.task_id} failed with error: {str(e)}")
            registry.set_error(self.task_id, e)
            raise

class UnreliableTask(Task):
    def __init__(
        self,
        work_time: float,
        failure_probability: float,
        result_value: Any,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.work_time = work_time
        self.failure_probability = failure_probability
        self.result_value = result_value
        
    def execute(self, inputs: Dict[str, Any]) -> Any:
        start_time = time.time()
        
        # Simulate work
        while time.time() - start_time < self.work_time:
            if self.should_cancel():
                raise Exception("Task cancelled")
            time.sleep(0.1)
            
        # Random failure
        if random.random() < self.failure_probability:
            raise Exception("Random task failure")
            
        return self.result_value