import pytest
from os_dbnetget import utils


def test_split_endpoint():
    endpoint = 'test01:8080'
    s, p = utils.split_endpoint(endpoint)
    assert s == 'test01'
    assert p == 8080


def test_walk_modules():
    mods = utils.walk_modules('tests.for_test_module_utils')
    expected = [
        'tests.for_test_module_utils',
        'tests.for_test_module_utils.test_mod1',
        'tests.for_test_module_utils.test_mod1.class_b',
        'tests.for_test_module_utils.test_mod1.class_a',
        'tests.for_test_module_utils.test_mod2',
        'tests.for_test_module_utils.test_mod2.test_mod1',
    ]
    assert set([m.__name__ for m in mods]) == set(expected)

    with pytest.raises(ImportError):
        utils.walk_modules('test._for_test_module_utils', skip_fail=False)


def test_iter_classes():
    from tests.for_test_module_utils.test_mod1.class_b import BaseClass
    classes = [c for c in utils.iter_classes('tests.for_test_module_utils', BaseClass)]
    expected = [
        'ClassB',
    ]
    assert set([c.__name__ for c in classes]) == set(expected)

    from tests.for_test_module_utils.test_mod1.class_a import ClassA
    classes = [c for c in utils.iter_classes('tests.for_test_module_utils', (ClassA, BaseClass))]
    expected = [
        'ClassB',
        'ClassAA',
    ]
    assert set([c.__name__ for c in classes]) == set(expected)
