class ControllerState():
    def __init__(self, name, params):
        self.name = name
        self.params = params

    def __eq__(self, other):
        return other is ControllerState and self.name == other.name

    def __str__(self):
        return f'["name" : {self.name}, "params": {self.params}]'
