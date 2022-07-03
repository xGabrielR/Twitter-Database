from abc import ABC, abstractmethod

class BasePipeline(ABC):
    
    @abstractmethod
    def create_buckets(self, *args):
        ...

    @abstractmethod
    def api_request(self):
        ...

    @abstractmethod
    def load_data(self):
        ... 

    @abstractmethod
    def get_users(self):
        ...

    @abstractmethod
    def get_posts(self):
        ...

    @abstractmethod
    def get_retweets(self):
        ...