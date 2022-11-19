class Test(object):
    def __init__(self) -> None:
        self.start = "0"
        self.next_start = "1"
        self.node = 3
    def __repr__(self) -> str:
        return ''.format(self.start, self.next_start, self.node)

test = Test()
print("TEST:",test.__repr__())