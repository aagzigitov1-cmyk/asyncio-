import unittest

from queue_manager import CrawlerQueue


class CrawlerQueueTests(unittest.IsolatedAsyncioTestCase):
    async def test_priority_and_empty_queue(self):
        queue = CrawlerQueue()
        await queue.add_url("https://example.com/low", priority=10)
        await queue.add_url("https://example.com/high", priority=1)
        await queue.add_url("https://example.com/medium", priority=5)

        self.assertEqual((await queue.get_next()).priority, 1)
        self.assertEqual((await queue.get_next()).priority, 5)
        self.assertEqual((await queue.get_next()).priority, 10)
        self.assertIsNone(await queue.get_next())


if __name__ == "__main__":
    unittest.main()
