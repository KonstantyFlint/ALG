import random


def iter_edges(problem_size: int):
    index = 2
    while index < problem_size:
        yield index - 2, index
        index += 2
    index -= 1

    yield index - 1, index

    while index >= 3:
        yield index - 2, index
        index -= 2


def buzz(problem_size: int):
    for i in range(problem_size * 3):
        a = random.randint(0, problem_size)
        b = random.randint(0, problem_size)
        if b < a:
            a, b = b, a
        if a == b:
            b += 1
        yield a, b


def generate_problem(filename: str, problem_size: int) -> None:
    with open(filename, "w") as file:
        for a, b in iter_edges(problem_size):
            file.write(f"{a}, {b}\n")
