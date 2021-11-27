import cchardet
import traceback
import time
import json
import asyncio
from urllib.parse import urlencode, urlparse, urljoin, quote
import aiohttp
import os
from tools.constant import USER_AGENT_LIST
from config import Logger, Config
from database import MongoDatabase
import random


try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass


class ManualClient:
    name = 'ManualClient'
    log = Logger(level='warning').logger

    suffix_voice = ['.mp3']
    # params = {'count': 1000}
    params = {}

    def __init__(self):
        self._workers = 0                 # 当前工作中的线程数
        self.workers_max = 10             # 最大线程数
        self.server_host = 'localhost'
        self.server_port = Config.SERVER_PORT

        self.loop = asyncio.get_event_loop()
        self.queue = asyncio.Queue(loop=self.loop)
        self.sem = asyncio.Semaphore(10)

        self.mongo = MongoDatabase()
        mongo_db = MongoDatabase().db()
        self.collection_voice = mongo_db['english_voice']

    # 向server使用POST发送url的执行结果
    async def send_result(self, result):
        # print('【client】发送下载结果到server, 内容: %s' % result)
        url = 'http://%s:%s/fileurls' % (self.server_host, self.server_port)
        try:
            async with aiohttp.ClientSession(loop=self.loop) as client:
                async with client.post(url, json=result, timeout=3) as response:
                    return response.status
        except:
            traceback.print_exc()
            pass

    async def save_file(self, url, ukey, content):
        # print('准备保存%s的内容, 文件类型是：%s' % (ukey, content_type))
        path = urlparse(url).path
        content_type = os.path.splitext(path)[-1]
        if content_type not in self.suffix_voice:
            return False

        data = {'_id': ukey, 'content': content, 'file_suffix': content_type}
        try:
            self.mongo.do_insert_one(self.collection_voice, {'_id': data['_id']}, data)
            good = True
        except Exception as e:
            traceback.print_exc()
            raise e
        return good

    async def download(self, url, timeout=25):
        status_code = 900
        content = ''
        headers = {'User-Agent': random.choice(USER_AGENT_LIST)}
        try:
            async with self.sem:
                async with aiohttp.ClientSession(loop=self.loop) as client:
                    async with client.get(url, headers=headers, timeout=timeout) as response:
                        status_code = response.status
                        if status_code == 200:
                            content = await response.read()
        except Exception as e:
            self.log.error('【client】下载失败: {}，exception: {}, {}'.format(url, str(type(e)), str(e)))
        return status_code, content

    # client的主要方法, 在循环抓取中，被不断的创建，达到并发目的。
    async def consume(self):
        while True:
            item = await self.queue.get()                       # 从队列中删除并返回一个元素。如果队列为空，则等待，直到队列中有元素。
            url = item['url']
            ukey = item['ukey']
            status_code, content = await self.download(url)
            print('【client】完成%s请求, ukey: %s, code：%s' % (url, ukey, status_code))

            self.queue.task_done()                              # 每当消费协程调用task_done()，即表示这个任务已完成，_workers即减少1。
            self._workers -= 1
            # print('self._workers减1后，还剩：%s' % self._workers)

            if content:
                res = await self.save_file(url, ukey, content)   # 文件保存不成功，使用800代码
                if not res:
                    status_code = 800
                   
            # 将本次请求的结果通过send_result()方法，发送给url_server
            result = {'ukey': ukey, 'status_code': status_code}
            await self.send_result(result)

    # 根据最大线程数和队列中的线程数的差额，向server请求urls
    async def produce(self):
        count = self.workers_max - self.queue.qsize()
        if count <= 0:
            print('【client】workers_max小于queue队列中的任务数量, produce不用获取新的urls对象。')
            return None
        try:
            async with aiohttp.ClientSession(loop=self.loop) as client:
                url = 'http://%s:%s/fileurls' % (self.server_host, self.server_port)
                async with client.get(url, params=self.params, timeout=5) as response:
                    if response.status not in [200, 201]:
                        return
                    text = await response.text()
                    jsondata = json.loads(text)
                    print('【client】获取%s条数据' % len(jsondata))

                    for one in jsondata:
                        await self.queue.put(one)
                    print('【client】produce请求成功，queue队列中任务数量为：%s, workers数量为：%s' % (self.queue.qsize(), self._workers))
        except:
            traceback.print_exc()
            return None

    # 爬虫的主流程是在方法loop_crawl()里面实现的。
    async def main(self):
        last_get = 0
        while True:
            if time.time() - last_get > 60:
                await self.produce()                                # 设定每60秒运行一次生产者produce
                last_get = time.time()

            # 任务每加1（同时计数加1），就运行一次消费者consume
            self._workers += 1
            consumer = asyncio.ensure_future(self.consume())        # 运行消费者consume，
            await self.queue.join()                                 # 阻塞至队列中所有的元素都被接收和处理完毕。当未完成计数降到零的时候， join() 阻塞被解除。
            consumer.cancel()

            if self._workers > self.workers_max:
                print('================ 【client】当前的_workers数量%s超过了workers_max的设定%s，休息3秒 ==================' % (self._workers, self.workers_max))
                await asyncio.sleep(3)

    def start(self):
        # executor = Executor()
        # self.loop.set_default_executor(executor)
        self.loop.create_task(self.main())
        try:
            # future = self.loop.run_in_executor(None, blocking阻塞方法)
            self.loop.run_forever()
        except KeyboardInterrupt:
            print('【client】手动键盘关闭')
            # tasks = asyncio.Task.all_tasks(loop=self.loop)  # 此方法 已弃用 并将在 Python 3.9 中移除。改用 asyncio.all_tasks() 函数。
            tasks = asyncio.all_tasks()
            group = asyncio.gather(*tasks, return_exceptions=True)
            group.cancel()
            self.loop.run_until_complete(group)
        finally:
            print('【client】最终loop关闭')
            # executor.shutdown(wait=True)
            self.loop.close()


if __name__ == '__main__':
    ManualClient().start()



