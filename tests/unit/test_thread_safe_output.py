"""
Tests for thread-safe output capture.

These tests verify that output from worker threads is properly captured
and doesn't leak to stdout/stderr, ensuring clean progress bar output.
"""

import sys
import threading

from domain_status_graph.utils.thread_safe_output import (
    ThreadSafeOutputCapture,
    install_thread_output_capture,
    uninstall_thread_output_capture,
)


def test_thread_safe_capture_captures_stdout():
    """Test that ThreadSafeOutputCapture captures stdout."""
    original_stdout = sys.stdout

    with ThreadSafeOutputCapture() as capture:
        print("This should be captured")
        sys.stdout.write("Direct write should be captured")

    # Should be restored
    assert sys.stdout is original_stdout

    # Should have captured output
    stdout_content, _ = capture.get_captured_output()
    assert "This should be captured" in stdout_content
    assert "Direct write should be captured" in stdout_content


def test_thread_safe_capture_works_in_threads():
    """Test that output capture works in worker threads."""
    captured_output = []

    def worker_thread():
        # Install capture in worker thread
        install_thread_output_capture()
        try:
            print("Worker thread output")
            sys.stdout.write("Worker direct write")
        finally:
            # Uninstall returns captured output
            stdout, stderr = uninstall_thread_output_capture()
            captured_output.append((stdout, stderr))

    thread = threading.Thread(target=worker_thread)
    thread.start()
    thread.join()

    # Should have captured output from worker thread
    assert len(captured_output) == 1
    stdout, stderr = captured_output[0]
    assert "Worker thread output" in stdout
    assert "Worker direct write" in stdout


def test_thread_safe_capture_isolates_threads():
    """Test that each thread has its own output capture."""
    results = []

    def worker_thread(thread_id):
        install_thread_output_capture()
        try:
            print(f"Thread {thread_id} output")
        finally:
            # Uninstall returns captured output
            stdout, _ = uninstall_thread_output_capture()
            results.append((thread_id, stdout))

    threads = [threading.Thread(target=worker_thread, args=(i,)) for i in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Each thread should have captured its own output
    assert len(results) == 3
    for thread_id, stdout in results:
        assert f"Thread {thread_id} output" in stdout


def test_thread_safe_capture_filters_datamule_noise():
    """Test that datamule noise is captured (filtering happens in suppress_datamule_output)."""
    with ThreadSafeOutputCapture() as capture:
        print("Loading submissions")
        print("Successfully loaded 2 submissions")
        print("Meaningful message that should be logged")

    stdout, _ = capture.get_captured_output()
    # All output should be captured (filtering happens later)
    assert "Loading submissions" in stdout
    assert "Successfully loaded 2 submissions" in stdout
    assert "Meaningful message that should be logged" in stdout
