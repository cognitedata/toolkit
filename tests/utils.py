import random
from contextlib import contextmanager


@contextmanager
def rng_context(seed: int | str):
    """Temporarily override internal random state for deterministic behaviour without side-effects

    Idea stolen from pandas source `class RNGContext`.
    """
    state = random.getstate()
    random.seed(seed)
    np_state = None
    try:
        import numpy as np
    except ImportError:
        pass
    else:
        np_state = np.random.get_state()
        if isinstance(seed, str):
            seed = sum(ord(c) for c in seed)
        np.random.seed(int(seed))
    try:
        yield
    finally:
        random.setstate(state)
        if np_state:
            try:
                np.random.set_state(np_state)
            except NameError:
                pass
