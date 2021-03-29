import aiohttp
import asyncio
import csv
import io
import gzip
import uuid
import urllib
from abc import ABC, abstractmethod


class Pagination:
    def __init__(self, url, **params):
        # {'pageIndex': 1, 'pageSize': 100}
        self.url_params = params if len(params) > 0 else dict(urllib.parse.parse_qsl(urllib.parse.urlparse(url).query))
        self.url = self.build_url(url, params)

    def next(self, key):
        self.url_params[key] = int(self.url_params[key]) + 1
        self.build_url()

    def build_url(self, url, url_params):
        return urllib.parse.urlunparse(
            urllib.parse.urlparse(url)._replace(query=urllib.parse.urlencode(url_params)))


class Batch:
    def __init__(self, pagination=Pagination):
        self.buff = list()
        self.pagination = pagination

    def __repr__(self):
        v = [e for e in self[:3]]
        return f'{self.__class__.__name__}({v})'

    def __getitem__(self, item):
        return self.buff[item]

    def append(self, v):
        self.buff.append(v)

    def __len__(self):
        return len(self.buff)

    @staticmethod
    def batcher(stream, batch_size=5) -> list:
        batch = Batch()
        for e in stream:
            batch.append(e)
            if len(batch) == batch_size:
                yield batch
                batch = Batch()

        if len(batch) != 0:
            yield batch


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


class Request:
    def __init__(self, batch_url: Batch):
        self.batch_url = batch_url

    async def get(self, headers=None, callback=None):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for batch in self.batch_url:
                for url in batch:
                    t = asyncio.create_task(callback(session, url, headers))
                    tasks.append(t)
            for task in asyncio.as_completed(tasks):
                yield task

    async def to_csv(self, coro, sep='|', compress=None, header=True, metadata=None):
        tasks = coro
        async for task in tasks:
            output = await task
            output._write(header=header, compress=compress, sep=sep, metadata=metadata)

    async def to_memory(self, coro):
        tasks = coro
        async for task in tasks:
            output = await task
            output._write()

    def run(self, output):
        r = asyncio.run(output)
        return r


class Output:
    def __init__(self, content: list, url: str):
        self.content = content
        self.url = url
        self.id = str(uuid.uuid5(uuid.NAMESPACE_DNS, url))
        self.domain_name = urllib.parse.urlparse(self.url).netloc

    def _write(self, header=True, compress=None, sep='|', metadata: Metadata = CSVMetadata):
        if not self.is_valid():
            raise Exception("not csv compatible")
        names = self.get_header()
        output_io = io.StringIO()
        dict_writer = csv.DictWriter(output_io, fieldnames=names, delimiter=sep)
        if header is True:
            dict_writer.writeheader()
        for r in self.content:
            dict_writer.writerow(r)
        if compress is None:
            with open(f'{self.id}.csv', 'w', encoding='utf-8', newline='') as f:
                f.write(output_io.getvalue())
        else:
            compress(self.id, output_io)
        if metadata is not None:
            metadata.write(self.domain_name, self.content, self.url)

    def get_header(self) -> dict.keys:
        if isinstance(self.content, list):
            if isinstance(self.content[0], dict):
                return self.content[0].keys()
            else:
                fake_header = {f'col{1}': False for i in range(len(self.content))}
                return fake_header.keys()
        elif isinstance(self.content, dict):
            return self.content.keys()
        else:
            return None

    def is_valid(self) -> bool:
        return self.get_header() is not None


class Stdout:
    def __init__(self, content: list, url: str):
        self.content = content
        self.url = url
        self.id = str(uuid.uuid5(uuid.NAMESPACE_DNS, url))
        self.domain_name = urllib.parse.urlparse(self.url).netloc

    def _write(self):
        print(self.content)


def gzipf(filename, buffer):
    with gzip.GzipFile(f'{filename}.csv.gz', mode='wb') as f:
        f.write(buffer.getvalue().encode())


