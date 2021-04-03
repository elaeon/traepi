import unittest
from src.core import Request, BuffStream, gzipf, BaseStream
from src.io import CsvOutput, Stdout
import json
import time


class TestBuffStream(unittest.TestCase):

    def test_request(self):
        async def pprint(session, url, headers):
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    r = await response.json()
                    r = json.loads(r)
                    return CsvOutput(r, url)
                else:
                    return CsvOutput(None, url)

        buff = BuffStream(stream=BaseStream(["http://localhost:8080/1", "http://localhost:8080/2",
                                             "http://localhost:8080/3"]))
        print(buff)
        start = time.time()
        request = Request(buff=buff)
        output = request.output(request.get(callback=pprint), sep='|', compress=gzipf, header=False, metadata=None)
        tasks = request.run(output)
        end = time.time()
        print(tasks, end-start)


    def test_badresponse(self):
        async def pprint(session, url, headers):
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    r = await response.json()
                    r = json.loads(r)
                    return Stdout(r, url)
                else:
                    return Stdout(None, url)

        buff = BuffStream(stream=BaseStream(["http://localhost:8080/4", "http://localhost:8080/long"]))
        request = Request(buff=buff)
        #output = request.output(request.get(callback=pprint))
        #tasks = request.run(output)
        #print(tasks)



if __name__ == '__main__':
    unittest.main()