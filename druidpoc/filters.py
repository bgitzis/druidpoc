from pydruid.utils.filters import Dimension as PydDimension

from pydruid.utils.filters import Filter as PydFilter


class Dimension(PydDimension):
    def __init__(self, dim):
        PydDimension.__init__(self, dim=dim)

    def __ge__(self, other):
        return Filter(dimension=self.dimension, type='ge', value=other)

    def __gt__(self, other):
        return Filter(dimension=self.dimension, type='gt', value=other)

    def __le__(self, other):
        return Filter(dimension=self.dimension, type='le', value=other)

    def __lt__(self, other):
        return Filter(dimension=self.dimension, type='lt', value=other)


class Filter(PydFilter):
    def __init__(self, **args):
        if 'type' not in args.keys():
            PydFilter.__init__(self, **args)
        elif args['type'] == 'ge':
            self.filter = {'filter': {'type': 'bound',
                                      "dimension": args["dimension"],
                                      'lower': args['value'],
                                      'alphaNumeric': True,
                                      'lowerStrict': True}}
        elif args['type'] == 'gt':
            self.filter = {'filter': {'type': 'bound',
                                      "dimension": args["dimension"],
                                      'lower': args['value'],
                                      'alphaNumeric': True}}
        elif args['type'] == 'le':
            self.filter = {'filter': {'type': 'bound',
                                      "dimension": args["dimension"],
                                      'upper': args['value'],
                                      'alphaNumeric': True,
                                      'upperStrict': True}}
        elif args['type'] == 'lt':
            self.filter = {'filter': {'type': 'bound',
                                      "dimension": args["dimension"],
                                      'upper': args['value'],
                                      'alphaNumeric': True}}
        else:
            PydFilter.__init__(self, **args)
