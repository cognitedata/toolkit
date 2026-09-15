def analyze(datapoints: list[float]) -> float:
    if not datapoints:
        return 0.0
    return sum(datapoints) / len(datapoints)
