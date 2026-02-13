import unittest

from db import db_operation


class DbRetryDecoratorTests(unittest.IsolatedAsyncioTestCase):
    async def test_retries_on_transient_error(self):
        state = {"calls": 0}

        @db_operation(retries=3, base_delay_sec=0.0)
        async def flaky() -> str:
            state["calls"] += 1
            if state["calls"] < 2:
                raise ConnectionError("temporary")
            return "ok"

        result = await flaky()
        self.assertEqual(result, "ok")
        self.assertEqual(state["calls"], 2)

    async def test_does_not_retry_on_non_transient_error(self):
        state = {"calls": 0}

        @db_operation(retries=3, base_delay_sec=0.0)
        async def broken() -> None:
            state["calls"] += 1
            raise ValueError("bad input")

        with self.assertRaises(ValueError):
            await broken()
        self.assertEqual(state["calls"], 1)


if __name__ == "__main__":
    unittest.main()
