import pytest
import xcalar.container.ewma as ewma


@pytest.mark.parametrize("testInput, expectedOutput, winSize", [
    ('1.3,2.4,5.7,8.2,9.7,11.4,13.6,21.4,24.2',
     '14.671,15.284,16.109,16.573,16.869,17.434,18.938,22.644,13.444', 9),
    ('1.3,2.4,5.7,8.2,9.7,11.4,13.6,21.4,24.2',
     '1.3,2.4,5.7,8.2,9.7,11.4,13.6,21.4,24.2', 0),
    ('1.4,5,2.6,4.7,6.8,11.14', '2.6,4.271,3.8,6.22,8.247,7.427', 3),
])
def test_ewma(testInput, expectedOutput, winSize):
    actualOutput = ewma.ewma(testInput, winSize)
    assert (actualOutput == expectedOutput)
