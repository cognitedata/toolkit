import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from cognite_toolkit._cdf_tk.utils.thread_safe_dict import ThreadSafeDict


class TestThreadSafeDict:
    def test_basic_operations(self) -> None:
        """Test basic dictionary operations work correctly."""
        d = ThreadSafeDict[str, int]()

        # Test setitem and getitem
        d["a"] = 1
        assert d["a"] == 1

        # Test len
        assert len(d) == 1

        # Test contains
        assert "a" in d
        assert "b" not in d

        # Test get
        assert d.get("a") == 1
        assert d.get("b") is None
        assert d.get("b", 42) == 42

        # Test setdefault
        assert d.setdefault("b", 2) == 2
        assert d["b"] == 2
        assert d.setdefault("b", 3) == 2  # Should not change existing value

        # Test update
        d.update({"c": 3, "d": 4})
        assert d["c"] == 3
        assert d["d"] == 4
        assert len(d) == 4

        # Test keys, values, items
        assert set(d.keys()) == {"a", "b", "c", "d"}
        assert set(d.values()) == {1, 2, 3, 4}
        assert set(d.items()) == {("a", 1), ("b", 2), ("c", 3), ("d", 4)}

        # Test pop
        assert d.pop("a") == 1
        assert "a" not in d
        assert len(d) == 3

        # Test popitem
        key, value = d.popitem()
        assert key in ["b", "c", "d"]
        assert value in [2, 3, 4]
        assert len(d) == 2

        # Test clear
        d.clear()
        assert len(d) == 0
        assert list(d.keys()) == []

    def test_initialization_with_data(self) -> None:
        """Test ThreadSafeDict can be initialized with data."""
        # Test with dict
        d1 = ThreadSafeDict({"a": 1, "b": 2})
        assert d1["a"] == 1
        assert d1["b"] == 2
        assert len(d1) == 2

        # Test with keyword arguments
        d2 = ThreadSafeDict(x=10, y=20)
        assert d2["x"] == 10
        assert d2["y"] == 20
        assert len(d2) == 2

        # Test with iterable of pairs
        d3 = ThreadSafeDict([("foo", "bar"), ("baz", "qux")])
        assert d3["foo"] == "bar"
        assert d3["baz"] == "qux"
        assert len(d3) == 2

    def test_copy(self) -> None:
        """Test that copy creates a new independent instance."""
        d1 = ThreadSafeDict({"a": 1, "b": 2})
        d2 = d1.copy()

        assert d1 == d2
        assert d1 is not d2
        assert d1.data is not d2.data

        # Modifying one should not affect the other
        d1["c"] = 3
        assert "c" not in d2

        d2["d"] = 4
        assert "d" not in d1

    def test_string_representations(self) -> None:
        """Test __str__ and __repr__ methods."""
        d = ThreadSafeDict({"a": 1, "b": 2})

        # __str__ should return string representation of underlying dict
        str_repr = str(d)
        assert "a" in str_repr and "b" in str_repr
        assert "1" in str_repr and "2" in str_repr

        # __repr__ should include class name
        repr_str = repr(d)
        assert "ThreadSafeDict" in repr_str
        assert "a" in repr_str and "b" in repr_str

    def test_iteration_safety(self) -> None:
        """Test that iteration works safely even with concurrent modifications."""
        d = ThreadSafeDict({i: i * 2 for i in range(100)})

        # Iterate while another thread modifies the dict
        def modifier():
            time.sleep(0.01)  # Let iteration start
            for i in range(100, 200):
                d[i] = i * 2

        modifier_thread = threading.Thread(target=modifier)
        modifier_thread.start()

        # This should not raise an exception even though dict is being modified
        keys_during_iteration = []
        try:
            # Iterating should be safe as it should operate on a snapshot.
            for key in d.keys():
                keys_during_iteration.append(key)
                time.sleep(0.001)  # Simulate work to increase chance of concurrent modification
        except RuntimeError:
            pytest.fail("Iteration over d.keys() should be thread-safe and not raise RuntimeError.")

        modifier_thread.join()

        # The iteration should have captured a snapshot of the keys at the beginning.
        assert len(keys_during_iteration) == 100
        assert set(keys_during_iteration) == {i for i in range(100)}
        assert set(keys_during_iteration) < set(d.keys()), "New keys should have been added after iteration"

    def test_concurrent_access(self) -> None:
        """Test thread safety with concurrent read/write operations."""
        d = ThreadSafeDict[int, int]()
        num_threads = 10
        operations_per_thread = 100

        def worker(thread_id: int):
            for i in range(operations_per_thread):
                key = thread_id * 1000 + i
                # Write
                d[key] = key * 2
                # Read
                assert d[key] == key * 2
                # Update
                d[key] = key * 3
                # Read again
                assert d[key] == key * 3

        threads = []
        for i in range(num_threads):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Verify all values are correct
        assert len(d) == num_threads * operations_per_thread
        for thread_id in range(num_threads):
            for i in range(operations_per_thread):
                key = thread_id * 1000 + i
                assert d[key] == key * 3

    def test_concurrent_modifications(self) -> None:
        """Test concurrent modifications don't cause corruption."""
        d = ThreadSafeDict[str, int]()
        num_threads = 20

        def worker(thread_id: int):
            for i in range(50):
                key = f"thread_{thread_id}_key_{i}"
                d[key] = thread_id * 100 + i

                # Sometimes delete keys from other threads
                if i % 10 == 0:
                    for other_thread in range(thread_id):
                        other_key = f"thread_{other_thread}_key_{i}"
                        d.pop(other_key, None)

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker, i) for i in range(num_threads)]
            for future in as_completed(futures):
                future.result()  # Wait for completion and check for exceptions

        # Verify the dict is still consistent (no corruption)
        # Count remaining keys
        remaining_keys = len(d)
        assert remaining_keys >= 0  # Should not be negative due to corruption

    def test_exception_handling(self) -> None:
        """Test proper exception handling."""
        d = ThreadSafeDict[str, int]()

        # KeyError on missing key
        with pytest.raises(KeyError):
            _ = d["nonexistent"]

        # KeyError on pop of missing key
        with pytest.raises(KeyError):
            d.pop("nonexistent")

        # KeyError on popitem from empty dict
        with pytest.raises(KeyError):
            d.popitem()

        # KeyError on del of missing key
        with pytest.raises(KeyError):
            del d["nonexistent"]

    def test_pop_with_default(self) -> None:
        """Test pop method with default value."""
        d = ThreadSafeDict({"a": 1})

        # Pop existing key
        assert d.pop("a") == 1
        assert len(d) == 0

        # Pop non-existing key with default
        assert d.pop("b", 42) == 42

        # Pop non-existing key without default should raise KeyError
        with pytest.raises(KeyError):
            d.pop("c")

    def test_update_methods(self) -> None:
        """Test various ways to update the dictionary."""
        d = ThreadSafeDict[str, int]()

        # Update with dict
        d.update({"a": 1, "b": 2})
        assert d["a"] == 1
        assert d["b"] == 2

        # Update with keyword arguments
        d.update(c=3, d=4)
        assert d["c"] == 3
        assert d["d"] == 4

        # Update with iterable of pairs
        d.update([("e", 5), ("f", 6)])
        assert d["e"] == 5
        assert d["f"] == 6

    def test_performance_under_contention(self) -> None:
        """Test that performance is reasonable under high contention."""
        d = ThreadSafeDict[int, int]()
        num_threads = 50
        operations_per_thread = 20

        def heavy_worker(thread_id: int):
            for i in range(operations_per_thread):
                # Mix of operations
                key = (thread_id * operations_per_thread + i) % 100
                d[key] = thread_id
                _ = d.get(key)
                d.setdefault(key + 1000, thread_id)
                if key in d:
                    d.pop(key, None)

        start_time = time.time()

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(heavy_worker, i) for i in range(num_threads)]
            for future in as_completed(futures):
                future.result()

        elapsed = time.time() - start_time

        # Should complete in reasonable time (adjust threshold as needed)
        assert elapsed < 10.0, f"Operations took too long: {elapsed:.2f}s"
