from taskchampion import Annotation


# IDK if this is a good idea, but it seems overkill to have another test to
# test the getter ... while using it for testing
def test_get_set_entry():
    a = Annotation()
    a.entry = "2024-05-07T01:35:57+03:00"

    assert a.entry == "2024-05-06T22:35:57+00:00"


def test_get_set_description():
    a = Annotation()

    a.description = "This is a basic description"

    assert a.description == "This is a basic description"
