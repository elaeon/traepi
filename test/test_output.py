import unittest
from src.core import Request, BuffStream, gzipf, BaseStream
from src.io import CsvOutput, Stdout, OrcOuput
import json
import time


class TestBuffStream(unittest.TestCase):

    def test_request(self):
        async def pprint(session, url, headers):
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    r = await response.json()
                    r = json.loads(r)
                    return OrcOuput(r, url)
                else:
                    return OrcOuput(None, url)

        buff = BuffStream(stream=BaseStream(["http://localhost:8080/1", "http://localhost:8080/2",
                                             "http://localhost:8080/3"]))
        print(buff)
        start = time.time()
        request = Request(buff=buff)
        output = request.output(request.get(callback=pprint),
                                srt_type={"id": "int", "text": "string", "date": "string"}, metadata=None)
        tasks = request.run(output)
        end = time.time()
        print(tasks, end-start)


if __name__ == '__main__':
    unittest.main()