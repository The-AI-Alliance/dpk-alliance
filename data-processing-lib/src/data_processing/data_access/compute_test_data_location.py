import os


def compute_data_location(loc: str) -> str:
    """
    Compute absolute data location
    :param loc: partial location
    :return: absolute location
    """
    location = os.path.dirname(__file__)
    sp = location.split("src")[0]
    return sp + loc


if __name__ == "__main__":
    res = compute_data_location("test-data/noop")
    print(res)
