from list_utils.read_file import read_file

from generate_problem import generate_problem
from solution import solve

FILENAME = "test.txt"
PROBLEM_SIZE = 1000


def test():
    generate_problem(FILENAME, PROBLEM_SIZE)
    edges = read_file(FILENAME, (int, int))
    s = solve(edges)
    print(s)


test()
