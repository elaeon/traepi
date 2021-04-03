import aiohttp
import asyncio
import csv
import io
import gzip
import urllib
from abc import ABC, abstractmethod


class Stream(ABC):
    @abstractmethod
    def __next__(self):
        raise NotImplemented

    def __iter__(self):
        return self

    @staticmethod
    def build_url(url, url_params):
        return urllib.parse.urlunparse(
            urllib.parse.urlparse(url)._replace(query=urllib.parse.urlencode(url_params)))


class BaseStream(Stream):
    def __init__(self, seq):
        if isinstance(seq, list) or isinstance(seq, tuple):
            self.stream = iter(seq)
        elif isinstance(seq, iter):
            self.stream = seq

    def __next__(self):
        return next(self.stream)


class StreamPage(Stream):
    def __init__(self, resource, key, **params):
        # {'pageIndex': 1, 'pageSize': 100}
        self.url_params = params if len(params) > 0 else \
            dict(urllib.parse.parse_qsl(urllib.parse.urlparse(resource).query))
        self.url = self.build_url(resource, params)
        self.key = key

    def __next__(self):
        self.url_params[self.key] = int(self.url_params.get(self.key, 0)) + 1
        return self.build_url(self.url, self.url_params)


class BuffStream:
    def __init__(self, stream: Stream = None, buff_size: int = 5):
        self.buff_size = buff_size
        self.stream = stream

    def __repr__(self):
        return f'{self.__class__.__name__}'

    def __iter__(self):
        return self

    def __next__(self) -> list:
        buff = []
        for e in self.stream:
            if len(buff) < self.buff_size:
                buff.append(e)
            else:
                break
        if len(buff) > 0:
            return buff
        else:
            raise StopIteration


class Metadata(ABC):
    @staticmethod
    @abstractmethod
    def write(domain_name, content, url):
        pass


class CSVMetadata(Metadata):
    @staticmethod
    def write(domain_name, content, url):
        with open(f'{domain_name}.metadata.csv', 'a', encoding='utf-8', newline='') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow([url, str(len(content))])


async def except_fn(callback, session, url, headers):
    try:
        return await callback(session, url, headers)
    except aiohttp.ServerDisconnectedError:
        pass


class Request:
    def __init__(self, buff: BuffStream):
        self.buff = buff
        self.is_buff_depleted = False

    async def get(self, headers=None, callback=None):
        for batch in self.buff:
            async with aiohttp.ClientSession() as session:
                tasks = [asyncio.create_task(except_fn(callback, session, url, headers)) for url in batch]
                yield tasks
                if self.is_buff_depleted is True:
                    break

    async def output(self, coro, **kwargs):
        tasks = coro
        async for task_b in tasks:
            for task in asyncio.as_completed(task_b, timeout=5):
                try:
                    output = await task
                    if output.content is None or len(output.content) == 0:
                        self.is_buff_depleted = True
                    output.write(**kwargs)
                except asyncio.exceptions.TimeoutError:
                    pass

    def run(self, output):
        r = asyncio.run(output)
        return r


def gzipf(filename: str, buffer: io.StringIO):
    with gzip.GzipFile(f'{filename}.csv.gz', mode='wb') as f:
        f.write(buffer.getvalue().encode())


