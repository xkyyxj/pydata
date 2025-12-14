from abc import ABC, abstractmethod

class AbstractSchedulerTask(ABC):
    @abstractmethod
    def execute(self):
        pass

    @abstractmethod
    def get_execute_time(self):
        pass