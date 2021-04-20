import unittest
from src.io import CsvOutput, TextOutput, OrcOuput, GzipOutput
from pathlib import Path



class TestOutput(unittest.TestCase):

    def test_orc(self):
        basepath = Path('tmp')
        # OrcOuput(r, url, basepath=basepath)
        # output.write()
        # assert output.filepath().stat().st_size == ?
        # output.clean()

    def test_csv(self):
        basepath = Path('tmp')
        output = CsvOutput([{"a": 1, "b": 2, "c": 3}], '', basepath=basepath)
        output.write()
        assert output.filepath().stat().st_size == 14
        output.clean()

    def test_txt(self):
        basepath = Path('tmp')
        output = TextOutput([{"a": 1, "b": 2, "c": 3}], '', basepath=basepath)
        output.write()
        assert output.filepath().stat().st_size == 26
        output.clean()


class TestCompress(unittest.TestCase):
    def test_gzip(self):
        basepath = Path('tmp')
        output = TextOutput([{'a': 1}], '', basepath=basepath)
        output.write_buff()
        gz_out = GzipOutput(output)
        gz_out.write_buff()
        gz_out.write_disk()
        gz_out.clean()


if __name__ == '__main__':
    unittest.main()
