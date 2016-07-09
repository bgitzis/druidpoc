from datetime import datetime

VALID_RECORD_TYPES = {'trade', 'sell_order', 'buy_order'}


class MarketEvent(object):
    def __init__(self, record_type, product_name, customer_name, customer_num_employees,
                 customer_location, qty, price, time=None):
        """
        :type record_type: basestring
        :type product_name: basestring
        :type customer_name: basestring
        :type customer_num_employees: int
        :type customer_location: basestring
        :type qty: int
        :type price: float
        :type time: datetime
        """
        if record_type in VALID_RECORD_TYPES:
            self.record_type = record_type
        else:
            raise TypeError('record_type must be one of %s' % list(VALID_RECORD_TYPES))
        self.product_name = product_name
        self.customer_name = customer_name
        self.customer_num_employees = customer_num_employees
        self.customer_location = customer_location
        self.qty = qty
        if price <= 0:
            raise TypeError('price must positive non zero, got %d' % price)
        self.price = price
        if time is None:
            self.time = datetime.utcnow()
        else:
            self.time = time

    def __str__(self):
        return '%s(%s)' % (type(self).__name__, ', '.join(sorted('%s=%s' % item for item in vars(self).items())))


if __name__ == '__main__':
    print MarketEvent('trade', 'gold', 'c1', 500, 'Russia', -10, 16.7)
