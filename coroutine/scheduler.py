from priority_queue import PriorityQueue
from typing import Optional, Iterator
from time import time, sleep

class Scheduler:
    def __init__(self):
        self.tasks = PriorityQueue()
    
    def add(self, time_point: float, coroutine: Iterator[None], frequency: Optional[float] = None) -> None:
        if not hasattr(coroutine, '__next__'):
            raise TypeError("task must be a coroutine")
        
        original_coroutine = coroutine
        
        def task_wrapper() -> bool:
            try:
                next(original_coroutine)
                return True
            except StopIteration:
                print("Coroutine task completed")
                return False
            except Exception as e:
                print(f"An error occurred: {e}")
                return False

        # Push wrapper with original coroutine
        self.tasks.push(time_point, (task_wrapper, frequency))

    def run(self) -> None:
        while not self.tasks.is_empty():
            time_point, (task, frequency) = self.tasks.pop()
            now = time()

            if time_point > now:
                sleep(time_point - now)

            try:
                print(f"Executing task scheduled at {time_point}")
                should_continue = task()
                
                if should_continue and frequency is not None:
                    next_time = time() + frequency
                    self.tasks.push(next_time, (task, frequency))
                    
            except Exception as e:
                print(f"An error occurred: {e}")
            

def test_p_2():

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

def test_p_3():
    
    scheduler = Scheduler()
    current = time()
    
    def coroutine_task(repeats: int) -> bool:
        for i in range(repeats):
            print(f"Coroutine task execution #{i + 1}")
            yield

    task = coroutine_task(3)
    scheduler.add(current + 1, task, 2)
    scheduler.run()

def main():
#    test_p_2()
    test_p_3()


if __name__ == "__main__":
    main()