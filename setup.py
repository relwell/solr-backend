"""
Wikia Solr Backend
"""

from setuptools import setup

setup(
    name=u'wikia_solr_backend',
    version=u'0.0.1',
    author=u'Wikia Platform Team',
    author_email=u'platform-l@wikia-inc.com',
    description=u'Wikia Deploy Tools',
    license=u'Other',
    test_suite=u'deploytools.test',
    install_requires=[u"requests", u"wikiautils"],
    packages=[u'wikia_solr_backend'],
)