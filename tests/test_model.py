from unittest import TestCase

from druidpoc.model import MarketEvent


class TestModel(TestCase):
    def test_new_transaction(self):
        self.assertRaises(TypeError, MarketEvent, 'abc', 'gold', 'c1', 500, 'Russia', 10, 16.7)
        self.assertRaises(TypeError, MarketEvent, 'trade', 'gold', 'c1', 500, 'Russia', 10, -16.7)
        me = MarketEvent('trade', 'gold', 'c1', 500, 'Russia', -10, 16.7)
        self.assertEquals('gold', me.product_name)
        self.assertEquals(-10, me.qty)
        self.assertEquals(16.7, me.price)
        self.assertEquals('c1', me.customer_name)
        self.assertEquals('Russia', me.customer_location)
        self.assertEquals(500, me.customer_num_employees)
