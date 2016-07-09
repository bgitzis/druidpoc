from pprint import pprint


def check_immediate_availability():
    return True


def main():
    results = {'immediate_availability': check_immediate_availability()}
    pprint(results)


if __name__ == '__main__':
    main()
