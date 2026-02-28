import time

class AlertStateManager:

    def __init__(self, cooldown_seconds):
        self.cooldown_seconds = cooldown_seconds
        self.machine_state = {}

    def should_send(self, machine_id):

        now = time.time()

        if machine_id not in self.machine_state:
            self.machine_state[machine_id] = now
            return True

        last_sent = self.machine_state[machine_id]

        if now - last_sent >= self.cooldown_seconds:
            self.machine_state[machine_id] = now
            return True

        return False