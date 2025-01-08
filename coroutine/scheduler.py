from priority_queue import PriorityQueue
from typing import Any, Callable, Optional
from time import time, sleep

class Scheduler:
    def __init__(self):
        self.tasks = PriorityQueue()
    
    def add(self, time_point:float, task: Callable[[], bool], frequency:Optional[float] = None) -> None:
        if not callable(task):
            raise TypeError("task must be callable")
        
        self.tasks.push(time_point, (task, frequency))

    def run(self):
        while not self.tasks.is_empty():
            time_point, (task, frequency) = self.tasks.pop()
            now = time()

            if time_point > now:
                sleep(time_point - now)
            try:
                print(f"Executing task scheduled at {time_point}")
                should_continue = task()
                
                if should_continue and frequency is not None:
                    next_time_point = time() + frequency
                    self.add(next_time_point, task, frequency)

            except Exception as e:
                print(f"An error occurred: {e}")
            


def main():
    scheduler = Scheduler()
    current = time()
    
    # One-time tasks
    def task1() -> bool:
        print("Task 1 executed")
        return False  # Don't repeat
        
    def task2() -> bool:
        print("Task 2 executed")
        return False
        
    # Repeating task with counter
    counter = 0
    def repeating_task() -> bool:
        nonlocal counter
        counter += 1
        print(f"Repeating task execution #{counter}")
        return counter < 3  # Stop after 3 executions
    
    # Schedule test tasks
    scheduler.add(current + 1, task1)  # One-time task
    scheduler.add(current + 2, task2)  # One-time task
    scheduler.add(current + 3, repeating_task, 2)  # Repeat every 2 seconds
    
    scheduler.run()

if __name__ == "__main__":
    main()