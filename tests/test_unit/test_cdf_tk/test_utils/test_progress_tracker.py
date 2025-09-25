import pytest

from cognite_toolkit._cdf_tk.utils.progress_tracker import ProgressTracker


class TestProgressTracker:
    def setup_method(self):
        self.steps = ["extract", "transform", "load"]
        self.tracker = ProgressTracker(self.steps)

    def test_initialization_sets_all_pending(self):
        progress = self.tracker.get_progress("item1")
        assert all(status == "pending" for status in progress.values())
        assert list(progress.keys()) == self.steps

    def test_set_success_only_affects_one_step(self):
        self.tracker.set_progress("item1", "extract", "success")
        progress = self.tracker.get_progress("item1")
        assert progress["extract"] == "success"
        assert progress["transform"] == "pending"
        assert progress["load"] == "pending"

    def test_set_failed_aborts_subsequent_steps(self):
        self.tracker.set_progress("item1", "extract", "failed")
        progress = self.tracker.get_progress("item1")
        assert progress["extract"] == "failed"
        assert progress["transform"] == "aborted"
        assert progress["load"] == "aborted"

    def test_set_aborted_and_pending(self):
        self.tracker.set_progress("item1", "extract", "aborted")
        progress = self.tracker.get_progress("item1")
        assert progress["extract"] == "aborted"
        assert progress["transform"] == "pending"
        assert progress["load"] == "pending"
        self.tracker.set_progress("item1", "transform", "pending")
        progress = self.tracker.get_progress("item1")
        assert progress["transform"] == "pending"

    def test_invalid_status_raises(self):
        with pytest.raises(ValueError):
            self.tracker.set_progress("item1", "extract", "not_a_status")

    def test_invalid_step_raises(self):
        with pytest.raises(ValueError):
            self.tracker.set_progress("item1", "not_a_step", "pending")

    def test_get_progress_for_step(self):
        self.tracker.set_progress("item1", "extract", "success")
        status = self.tracker.get_progress("item1", "extract")
        assert status == "success"

    def test_auto_init_on_get(self):
        progress = self.tracker.get_progress("new_item")
        assert all(status == "pending" for status in progress.values())

    def test_multiple_item_ids(self):
        self.tracker.set_progress(1, "extract", "success")
        self.tracker.set_progress(("a", 2), "extract", "failed")
        assert self.tracker.get_progress(1, "extract") == "success"
        assert self.tracker.get_progress(("a", 2), "extract") == "failed"

    def test_result_method(self):
        self.tracker.set_progress("item1", "extract", "success")
        self.tracker.set_progress("item2", "extract", "failed")
        result = self.tracker.result()
        assert result == {
            "item1": {"extract": "success", "transform": "pending", "load": "pending"},
            "item2": {"extract": "failed", "transform": "aborted", "load": "aborted"},
        }

    def test_aggregate_method(self):
        self.tracker.set_progress("item1", "extract", "success")
        self.tracker.set_progress("item1", "transform", "success")
        self.tracker.set_progress("item1", "load", "success")
        self.tracker.set_progress("item2", "extract", "failed")
        self.tracker.set_progress("item3", "extract", "success")
        self.tracker.set_progress("item3", "transform", "failed")
        aggregate = dict(self.tracker.aggregate())
        expected = {
            ("extract", "success"): 2,
            ("extract", "failed"): 1,
            ("transform", "success"): 1,
            ("transform", "aborted"): 1,
            ("transform", "failed"): 1,
            ("load", "success"): 1,
            ("load", "aborted"): 2,
        }
        assert aggregate == expected
