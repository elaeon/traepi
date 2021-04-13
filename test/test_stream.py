import unittest
from src.core import Request, BuffStream, StreamPage, StreamPageWait, CSVMetadata
from src.io import TextOutput
import json
from src.core import DummyMetadata


class TestPageStream(unittest.TestCase):

    def test_pag(self):
        async def pprint(session, url, headers):
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    r = await response.json()
                    r = json.loads(r)
                    return TextOutput(r, url, basepath='tmp')
                else:
                    return TextOutput(None, url, basepath='tmp')

        buff = BuffStream(stream=StreamPage(resource="http://localhost:8080/pag", key='pageIndex'))
        request = Request(buff=buff, metadata=DummyMetadata('tmp'))
        output = request.output(request.get(callback=pprint), clean=True)
        request.run(output)

    def test_seq_pag(self):
        async def pprint(session, url, headers):
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    r = await response.json()
                    r = json.loads(r)
                    return TextOutput(r, url, headers=response.headers, basepath='tmp')
                else:
                    return TextOutput(None, url, basepath='tmp')

        buff = BuffStream(stream=StreamPageWait(resource="http://localhost:8080/pag_h", key='pageIndex',
                                                response_wait_key='next', pageSize=100))
        metadata = CSVMetadata('tmp')
        request = Request(buff=buff, metadata=metadata)
        output = request.output(request.get(callback=pprint))
        request.run(output)
        for line in metadata.read():
            assert int(line[0]) >= 1


if __name__ == '__main__':
    unittest.main()