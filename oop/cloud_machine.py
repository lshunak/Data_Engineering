from datetime import datetime

class Machine:
    def __init__(self, id: str, type: int):
        if type not in [1,2]:
            raise ValueError("Invalid machine type")
        
        self.id = id
        self.type = type
        self.rate_per_minute = 2 if type == 1 else 5
        self.start_time = None
        self.total_cost = 0
        self.is_running = False

    def __str__(self):
        status = "Running" if self.active else "Stopped"
        return f"Machine {self.machine_id}: Type {self.machine_type}, Status: {status}, Accumulated Cost: ${self.accumulated_cost:.2f}"


    def start_machine(self):
        if self.is_running:
            print("Machine already running")
            return
        
        self.start_time = datetime.now()
        self.is_running = True

    def stop_machine(self):
        if not self.is_running:
            print("Machine is not running")
            return

        self.calculate_cost()
        self.is_running = False
        self.start_time = None

    def calculate_cost(self):
        if self.active:
            active_duration = (datetime.now() - self.start_time).total_seconds() // 60
            self.accumulated_cost += active_duration * self.rate_per_minute
            self.start_time = datetime.now()
        return self.accumulated_cost

class CloudService:
    def __init__(self):
        self.machines = []

    def add_machine(self, machine):
        self.machines.append(machine)

    def remove_machine(self, machine_id):
        for machine in self.machines:
            if machine.id == machine_id:
                self.machines.remove(machine)
                return
        print("Machine not found")

    def get_machine(self, machine_id):
        for machine in self.machines:
            if machine.id == machine_id:
                return machine
        return None

    def calculate_total_cost(self):
        total_cost = 0
        for machine in self.machines:
            total_cost += machine.calculate_cost()
        return total_cost

    def __str__(self):
        return "\n".join([str(machine) for machine in self.machines])
    
    