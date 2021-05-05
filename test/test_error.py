import unittest
from src.core import Request, BuffStream, BaseStream, DummyMetadata
from src.io import CsvOutput
import json
import time
from pathlib import Path


async def pprint(session, url, headers, timeout):
    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            r = await response.json()
            r = json.loads(r)
            return CsvOutput(r, url, basepath=Path('tmp'))
        else:
            return CsvOutput(None, url, basepath=Path('tmp'))


class TestBuffStream(unittest.TestCase):

    def test_error(self):
        buff = BuffStream(stream=BaseStream(["http://localhost:8080/rand"]))
        request = Request(buff=buff, metadata=DummyMetadata(Path('tmp')))
        #try:
        output = request.output(request.get(callback=pprint))
        tasks = request.run(output)
        #except Exception:
        #    print("Exception")
        #print(tasks)


if __name__ == '__main__':
    unittest.main()
