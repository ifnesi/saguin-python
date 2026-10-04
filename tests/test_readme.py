"""The README names verbs; this checks the library still has them.

A claim about behaviour is a thing to run, not a thing to write once - a
sentence the code has stopped agreeing with is believed by everyone who
reads it next, and no test catches it, because it is not code. The README
is the first thing anybody reads, so the names in it are checked here.

It counts what it read and refuses a suspiciously small number, because a
pattern that quietly stopped matching passes by comparing nothing.
"""

import pathlib
import re

import saguin

README = pathlib.Path(__file__).resolve().parent.parent / "README.md"

# `client.append.publish(`, `reader.queue.ack(`, and so on: an object, a
# namespace, a verb. The object's name varies through the document on
# purpose - client, reader, worker - so it is not anchored to one.
CALLS = re.compile(r"\b\w+\.(append|latest|queue|admin)\.(\w+)\(")


def test_every_verb_the_readme_names_exists():
    text = README.read_text()
    named = sorted(set(CALLS.findall(text)))
    assert len(named) >= 10, (
        "only found {} verbs in the README, which cannot be all of them - the "
        "pattern has stopped matching".format(len(named))
    )
    # Resolved the way a reader would reach them.
    made = saguin.Client("readme-check")
    for namespace, verb in named:
        group = getattr(made, namespace)
        assert hasattr(group, verb), (
            "the README calls client.{}.{}(), which this library does not "
            "have".format(namespace, verb)
        )


def test_the_advice_in_a_refusal_names_verbs_that_exist():
    """A wrong-channel refusal tells the caller which verbs to use
    instead, so a verb renamed without it becomes advice to call something
    that is not there - which is worse than no advice.

    **It counts before it compares.** The first version of this matched
    `.verb(` and the advice at the time was written `job_submit(), ...`
    with no leading dot, so the loop ran zero times and the test passed
    over three verbs that had been renamed out of existence. A pattern
    that stopped matching passes by comparing nothing.
    """
    from saguin.client import VERBS_OF

    made = saguin.Client("verbs-check")
    named = re.compile(r"\.?(\w+)\(")
    checked = 0
    for kind, advice in VERBS_OF.items():
        group = getattr(made, kind)
        found = named.findall(advice)
        assert len(found) >= 2, (
            "read only {} verbs out of the {} advice {!r} - the pattern has "
            "stopped matching".format(len(found), kind, advice)
        )
        for verb in found:
            checked += 1
            assert hasattr(group, verb), (
                "a {} channel refusal tells the caller to use {}(), which "
                "client.{} does not have".format(kind, verb, kind)
            )
    assert checked >= 10, "checked only {} verbs in all".format(checked)


def test_this_library_describes_itself_on_its_own_terms():
    """This SDK stands alone: its documents, comments and tests describe
    what it does, never by comparison with another client library. A
    mention that creeps back in points its readers at a project they were
    never meant to need. The pattern is assembled from parts so that this
    file can be swept along with the rest."""
    other = re.compile(
        "|".join(("saguin-" + "js", r"\bJava" + r"Script\b", r"\bMQTT\." + "js" + r"\b")),
        re.I,
    )
    here = pathlib.Path(__file__).resolve().parent.parent
    swept = [here / "README.md", here / "CONTRIBUTING.md"]
    for folder in ("src", "tests", "examples"):
        swept.extend(
            one for one in (here / folder).rglob("*")
            if one.suffix in (".py", ".md", ".yaml")
        )
    assert len(swept) >= 15, (
        "swept only {} files, which cannot be the whole repository".format(len(swept))
    )
    for name in swept:
        found = other.findall(name.read_text())
        assert not found, (
            "{} mentions another client library ({}), and this SDK "
            "describes itself on its own terms".format(
                name.relative_to(here), sorted(set(found)))
        )
