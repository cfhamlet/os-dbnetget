import inspect

import pytest
from os_dbnetget import utils


def test_split_endpoint():
    endpoint = 'test01:8080'
    s, p = utils.split_endpoint(endpoint)
    assert s == 'test01'
    assert p == 8080
    endpoint = b'test01:8080'
    s, p = utils.split_endpoint(endpoint)
    assert s == b'test01'
    assert p == 8080


def test_walk_modules():
    mods = utils.walk_modules('tests.test_main.for_test_module_utils')
    expected = [
        'tests.test_main.for_test_module_utils',
        'tests.test_main.for_test_module_utils.test_mod1',
        'tests.test_main.for_test_module_utils.test_mod1.class_b',
        'tests.test_main.for_test_module_utils.test_mod1.class_a',
        'tests.test_main.for_test_module_utils.test_mod2',
        'tests.test_main.for_test_module_utils.test_mod2.test_mod1',
    ]
    assert set([m.__name__ for m in mods]) == set(expected)

    with pytest.raises(ImportError):
        utils.walk_modules('test._for_test_module_utils', skip_fail=False)


def test_iter_classes():
    from tests.test_main.for_test_module_utils.test_mod1.class_b import BaseClass
    classes = [c for c in utils.iter_classes(
        'tests.test_main.for_test_module_utils', BaseClass)]
    expected = [
        'ClassB',
    ]
    assert set([c.__name__ for c in classes]) == set(expected)

    from tests.test_main.for_test_module_utils.test_mod1.class_a import ClassA
    classes = [c for c in utils.iter_classes(
        'tests.test_main.for_test_module_utils', (ClassA, BaseClass))]
    expected = [
        'ClassB',
        'ClassAA',
    ]
    assert set([c.__name__ for c in classes]) == set(expected)


def test_check_positive():

    data = [
        (1, int, 1),
        ('1', int, 1),
        (0, int, ValueError),
        ('0', int, ValueError),
        (-1, int, ValueError),
        ('-1', int, ValueError),
        ('1.1', int, ValueError),
    ]
    for input, input_type, expected in data:
        if inspect.isclass(expected) and issubclass(expected, Exception):
            with pytest.raises(expected):
                utils.check_positive(input_type, input)
        else:
            assert expected == utils.check_positive(input_type, input)


def test_check_range():
    data = [
        (1, int, (0, 10), 1),
        ('1', int, (0, 10), 1),
        (11, int, (0, 10), ValueError),
        ('11', int, (0, 10), ValueError),
        (-1, int, (0, 10), ValueError),
        ('-1', int, (0, 10), ValueError),
        (1.1, float, (0, 10), 1.1),
        ('1.1', float, (0, 10), 1.1),
        ('1.1', int, (0, 10), ValueError),
    ]
    for input, input_type, valid_range, expected in data:
        if inspect.isclass(expected) and issubclass(expected, Exception):
            with pytest.raises(expected):
                utils.check_range(
                    input_type, valid_range[0], valid_range[1], input)
        else:
            assert expected == utils.check_range(
                input_type, valid_range[0], valid_range[1], input)
