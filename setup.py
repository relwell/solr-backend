"""
Wikia Solr Backend
"""

from setuptools import setup

setup(
    name=u'wikia_solr_backend',
    version=u'0.0.1',
    author=u'Robert Elwell',
    author_email=u'robert@wikia-inc.com',
    description=u'Wikia Deploy Tools',
    license=u'Other',
    install_requires=[u"requests", u"wikiautils"],
    packages=[u'wikia_solr_backend'],
)