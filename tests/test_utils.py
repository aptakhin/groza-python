import pytest

from groza.utils import CamelCaseFieldTransformer


def test_word():
    cc = CamelCaseFieldTransformer()

    assert cc.to_db('position') == 'position'
    assert cc.from_db('position') == 'position'

    assert cc.from_db('suite_id') == 'suiteId'
    assert cc.to_db('suiteId') == 'suite_id'


if __name__ == '__main__':
    pytest.main(['-s', '-x', __file__])
