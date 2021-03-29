import unittest
from src.core import Batch


class TestBatch(unittest.TestCase):

    def test_append(self):
        batch = Batch()
        batch.append(1)
        batch.append(2)
        self.assertTrue(len(batch) == 2)


if __name__ == '__main__':
    unittest.main()