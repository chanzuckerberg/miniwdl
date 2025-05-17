# mypy: ignore-errors

class svmem:
    total: int
    ...

def virtual_memory() -> svmem:
    ...
