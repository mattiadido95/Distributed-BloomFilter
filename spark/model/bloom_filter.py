import mmh3


class BloomFilter:
    def __init__(self, m, k):
        self.m = m
        self.k = k
        self.array_bf = [0 for _ in range(m)]

    def add(self, title):
        for i in range(self.k):
            self.array_bf[abs(mmh3.hash(title, i) % self.m)] = 1

    def find(self, title):
        for i in range(self.k):
            if self.array_bf[abs(mmh3.hash(title, i) % self.m)] == 0:
                return False
        return True

    def merge(self, bf):
        self.array_bf = [x|y for x,y in zip(self.array_bf, bf.get())]
        return self

    def get(self):
        return self.array_bf