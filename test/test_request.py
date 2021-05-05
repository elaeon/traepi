import unittest
from src.core import Request, BuffStream, BaseStream, DummyMetadata
from src.io import CsvOutput
import json
import time
from pathlib import Path


async def pprint(session, url, headers, timeout):
    async with session.get(url, headers=headers, timeout=timeout) as response:
        if response.status == 200:
            r = await response.json()
            r = json.loads(r)
            return CsvOutput(r, url, basepath=Path('tmp'))
        else:
            return CsvOutput([], url, basepath=Path('tmp'))


class TestBuffStream(unittest.TestCase):

    def test_request(self):
        buff = BuffStream(stream=BaseStream(["http://localhost:8080/1", "http://localhost:8080/2",
                                             "http://localhost:8080/3"]))
        #start = time.time()
        #request = Request(buff=buff, metadata=DummyMetadata(Path('tmp')))
        #output = request.output(request.get(callback=pprint), sep='|', header=False)
        #tasks = request.run(output)
        #end = time.time()
        #print(tasks, end-start)

    def test_badresponse(self):
        buff = BuffStream(stream=BaseStream(["http://localhost:8080/4"]))
        request = Request(buff=buff, metadata=DummyMetadata(Path('tmp')))
        output = request.output(request.get(callback=pprint))
        tasks = request.run(output)

    def test_longresponse(self):
        buff = BuffStream(stream=BaseStream(["http://localhost:8080/long"]))
        request = Request(buff=buff, metadata=DummyMetadata(Path('tmp')))
        output = request.output(request.get(callback=pprint, timeout=1))
        tasks = request.run(output)


if __name__ == '__main__':
    unittest.main()
