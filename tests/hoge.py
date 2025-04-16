import unittest
import pytest
import sys

def another_function():
    sys.stdout.write("Output via sys.stdout\n")

class TestFileOutput(unittest.TestCase):
    def test_more_output(self):
        sys.stdout.write("More output.\n")
        out, _ = self.capfd.readouterr() # ignore
        self.assertEqual(out, "More output.\n")

    @pytest.fixture(autouse=True)
    def set_capfd(self, capfd: pytest.CaptureFixture):
        self.capfd = capfd

if __name__ == '__main__':
    pytest.main([__file__])
