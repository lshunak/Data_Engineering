from priority_queue import PriorityQueue
from typing import Any, Callable
from time import time, sleep

class Scheduler:
    def __init__(self):
        self.tasks = PriorityQueue()
    
    def add(self, time_point:float, task: Callable[[], Any]) -> None:
        if not callable(task):
            raise TypeError("task must be callable")
        
        self.tasks.push(time_point, task)

    def run(self):
        while not self.tasks.is_empty():
            time_point, task = self.tasks.pop()
            now = time()
            if time_point > now:
                sleep(time_point - now)

            task()


def main():
    scheduler = Scheduler()
    current = time()
    
    # Schedule test tasks
    scheduler.add(current + 2, lambda: print("Task 1"))
    scheduler.add(current + 1, lambda: print("Task 2"))
    scheduler.add(current + 3, lambda: print("Task 3"))
    
    scheduler.run()

if __name__ == "__main__":
    main()