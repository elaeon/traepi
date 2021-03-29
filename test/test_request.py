import unittest
from src.core import Request, Batch, gzipf, Output, Pagination, Stdout
import json
import time


# class TestBatch(unittest.TestCase):
#
#     def test_request(self):
#         async def pprint(session, url, headers):
#             async with session.get(url, headers=headers) as response:
#                 if response.status == 200:
#                     r = await response.json()
#                     r = json.loads(r)
#                     return Output(r, url)
#                 else:
#                     return Output(None, url)
#
#         batch = Batch(["http://localhost:8080/1", "http://localhost:8080/2", "http://localhost:8080/3"])
#                        #"http://localhost:8080/4"])
#         print(batch)
#         start = time.time()
#         request = Request(batch_url=batch)
#         output = request.to_csv(request.get(callback=pprint), sep='|', compress=gzipf, header=False, metadata=None)
#         tasks = request.run(output)
#         end = time.time()
#         print(tasks, end-start)


class TestPagination(unittest.TestCase):

    def test_pag(self):
        async def pprint(session, url, headers):
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    r = await response.json()
                    r = json.loads(r)
                    return Stdout(r, url)
                else:
                    return Stdout(None, url)

        batch = Batch(pagination=Pagination).batcher(["http://localhost:8080/pag"])
        request = Request(batch_url=batch)
        output = request.to_memory(request.get(callback=pprint))
        tasks = request.run(output)


if __name__ == '__main__':
    unittest.main()